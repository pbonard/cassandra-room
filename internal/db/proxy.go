package db

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/pbonard/cassandra-room/internal/accord"
	"github.com/pbonard/cassandra-room/internal/cluster"
	"github.com/pbonard/cassandra-room/internal/storage/lsm"
	"github.com/pbonard/cassandra-room/internal/storage/row"
	"github.com/pbonard/cassandra-room/internal/types"
)

// ClusterOptions make a DB a node of a cluster.
type ClusterOptions struct {
	// ListenAddress is the internode address, "host:port" (storage_port),
	// the node listens on.
	ListenAddress string
	// BroadcastAddress is the internode address the node is known by and
	// other nodes connect to (broadcast_address), when it differs from
	// ListenAddress (NAT, another network); it is the node's identity.
	BroadcastAddress string
	// PreferLocal makes nodes of the same data center connect to each
	// other at their listen addresses (prefer_local), keeping broadcast
	// addresses for other data centers.
	PreferLocal bool
	// ReplaceAddress makes a new node take the place of a dead one, given
	// by address or host id (replace_address_first_boot).
	ReplaceAddress string
	// Seeds are internode addresses of nodes to join through.
	Seeds []string
	// NumTokens is the number of vnodes (num_tokens), 16 by default.
	NumTokens int
	// WriteTimeout and ReadTimeout bound requests to replicas
	// (write_request_timeout, read_request_timeout): 2 s and 5 s.
	WriteTimeout, ReadTimeout time.Duration
	// AccordDurabilityInterval is how often Accord's durability rounds run
	// (30 s by default).
	AccordDurabilityInterval time.Duration
	// Hinted handoff: MaxHintWindow is how long a node may be down and
	// still get hints (max_hint_window, default 3h); HintThrottle the
	// bytes per second hints are replayed at, shared by the nodes being
	// replayed to (hinted_handoff_throttle, default 1 MiB/s); DisableHints
	// turns hints off (hinted_handoff_enabled: false).
	MaxHintWindow time.Duration
	HintThrottle  int64
	DisableHints  bool
	// Messaging configures internode encryption and compression.
	Messaging cluster.MessagingOptions
	// Membership tunes failure detection and gossip (memberlist's LAN
	// defaults if nil).
	Membership *cluster.Config
}

// proxy coordinates statements across the cluster (StorageProxy): writes
// go to every replica of each partition and wait for the consistency
// level; reads ask enough replicas, merge their rows (row fragments merge
// by timestamp, so the merge is reconciliation) and repair the replicas
// that were behind.
type proxy struct {
	// testRefuseMutations makes the node refuse MUTATION messages (tests).
	testRefuseMutations atomic.Pointer[bool]
	// streamUsed is when each outbound stream session was last used.
	streamMu   sync.Mutex
	streamUsed map[string]time.Time
	d          *DB
	m          *cluster.Messaging
	g          *cluster.Gossiper
	cms        *cluster.CMS
	// accord runs Accord transactions (transactional_mode tables).
	accord *accord.Node
	lats   sync.Map // lsm.TableID -> *readLatency, for speculative retry
	// initialCMS are the metadata service's members before the metadata
	// records any.
	initialCMS []string
	// replaced holds the host ids of nodes a joining node replaces: they
	// count as down.
	replaced atomic.Pointer[map[string]bool]
	opts     ClusterOptions
	ring     atomic.Pointer[cluster.Ring]
	self     string
	stopCh   chan struct{}
	wg       sync.WaitGroup
	hintMu   sync.Mutex
	// downSince is when each node was last seen going down.
	downSince sync.Map // addr -> time.Time
	started   time.Time
	seq       atomic.Uint64
}

// cmsStateFileName holds the metadata service's acceptor state.
const cmsStateFileName = "cms-paxos.json"

// readFile reads a file of the data directory, nil if there is none.
func (d *DB) readFile(name string) ([]byte, error) {
	f, err := d.fs.Open(d.fs.PathJoin(d.dir, name))
	if oserrorIsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// hintsStorage is the engine table holding hints (system.hints).
const hintsStorage lsm.TableID = 1 << 40

// errors with protocol fields.
type unavailableError struct {
	cl              primitive.ConsistencyLevel
	required, alive int
}

func (e *unavailableError) Error() string {
	return fmt.Sprintf("Cannot achieve consistency level %v", e.cl)
}

type timeoutError struct {
	write              bool
	cl                 primitive.ConsistencyLevel
	received, blockFor int
	batch, cas         bool
}

func (e *timeoutError) Error() string {
	op := "read"
	if e.write {
		op = "write"
	}
	return fmt.Sprintf("Operation timed out - received only %d responses (%s, %v).", e.received, op, e.cl)
}

func (d *DB) startCluster(o *ClusterOptions) error {
	if o.NumTokens <= 0 {
		o.NumTokens = 16
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 2 * time.Second
	}
	if o.ReadTimeout <= 0 {
		o.ReadTimeout = 5 * time.Second
	}
	if o.MaxHintWindow <= 0 {
		o.MaxHintWindow = 3 * time.Hour
	}
	if o.HintThrottle <= 0 {
		o.HintThrottle = 1 << 20
	}
	if _, err := d.engine.Table(hintsStorage); errors.Is(err, lsm.ErrUnknownTable) {
		if _, err := d.engine.CreateTable(hintsStorage); err != nil {
			return err
		}
	}
	d.cleanStreaming()
	if len(d.local.Tokens) == 0 {
		d.local.Tokens = cluster.RandomTokens(o.NumTokens)
		d.ddlMu.Lock()
		err := d.saveSchema(d.schema.Load())
		d.ddlMu.Unlock()
		if err != nil {
			return err
		}
	}
	mo := o.Messaging
	mo.Broadcast = o.BroadcastAddress
	m, err := cluster.NewMessagingWith(o.ListenAddress, mo)
	if err != nil {
		return err
	}
	p := &proxy{d: d, m: m, opts: *o, self: m.Addr(), stopCh: make(chan struct{}), started: time.Now()}
	if o.PreferLocal {
		m.SetDialAddress(p.dialAddress)
	}
	m.Handle(cluster.VerbMutation, p.handleMutation)
	m.Handle(cluster.VerbReadRow, p.handleReadRow)
	m.Handle(cluster.VerbReadDigest, p.handleReadDigest)
	m.Handle(cluster.VerbReadRange, p.handleReadRange)
	m.Handle(cluster.VerbReadRangeReverse, p.handleReadRangeReverse)
	m.Handle(cluster.VerbRepair, p.handleRepair)
	m.Handle(cluster.VerbStreamPush, p.handleStreamPush)
	m.Handle(cluster.VerbStreamFile, p.handleStreamFile)
	m.Handle(cluster.VerbCounterMutation, p.handleCounterMutation)
	p.g = cluster.NewGossiper(m, cluster.EndpointState{HostID: d.local.HostID, Epoch: d.schema.Load().Epoch}, o.Seeds)
	if o.Membership != nil {
		p.g.SetConfig(*o.Membership)
	}
	// The metadata names the Cluster Metadata Service's members; until the
	// first commit records them, they are the seeds (TCM's initial CMS).
	p.initialCMS = o.Seeds
	if len(p.initialCMS) == 0 {
		p.initialCMS = []string{p.self}
	}
	if err := d.replayMetadataLog(); err != nil {
		m.Close()
		return err
	}
	p.cms = cluster.NewCMS(m, metaStore{p})
	saved, err := d.readFile(cmsStateFileName)
	if err != nil {
		m.Close()
		return err
	}
	if err := p.cms.SetPersistence(saved, func(b []byte) error { return d.writeFileAtomic(cmsStateFileName, b) }); err != nil {
		m.Close()
		return err
	}
	if err := p.startAccord(); err != nil {
		m.Close()
		return err
	}
	p.g.OnChange(p.membershipChanged)
	p.g.OnLiveness(func(addr string, up bool) {
		if up {
			p.downSince.Delete(addr)
			go p.replayHints()
		} else {
			p.downSince.LoadOrStore(addr, time.Now())
		}
	})
	d.proxy = p
	d.store = p
	p.ring.Store(cluster.NewRing(nodeList(d.schema.Load())))
	m.Start()
	// A restarted node also contacts the peers it knew: seeds' addresses
	// may have changed (pods), or be down.
	known := slices.Clone(o.Seeds)
	for _, n := range d.schema.Load().Nodes {
		if n.Status != cluster.StatusLeft && n.Addr != p.self && !slices.Contains(known, n.Addr) {
			known = append(known, n.Addr)
		}
	}
	p.g.AddSeeds(known...)
	p.g.Start()
	for _, s := range known {
		if s != p.self && p.cms.CatchUp(s) == nil {
			break
		}
	}
	if err := p.join(); err != nil {
		p.stop()
		return err
	}
	p.wg.Add(1)
	go p.background()
	return nil
}

// replacedNode reports whether a joining node is replacing n.
func (p *proxy) replacedNode(n *cluster.Node) bool {
	r := p.replaced.Load()
	return r != nil && (*r)[n.HostID]
}

// up reports whether a node is up: alive, and not being replaced.
func (p *proxy) up(n *cluster.Node) bool {
	if r := p.replaced.Load(); r != nil && (*r)[n.HostID] {
		return false
	}
	return n.Addr == p.self || p.g.IsAlive(n.Addr)
}

// cmsMembers returns the metadata service's members.
func (p *proxy) cmsMembers() []string {
	s := p.d.schema.Load()
	m := s.CMS
	if len(m) == 0 {
		return p.initialCMS
	}
	// A member restarted at a new address (a pod's IP changes) is still
	// listed at its old one until its REGISTER commits: it is this node.
	if me := s.Nodes[p.d.local.HostID]; me != nil && me.Addr != p.self && slices.Contains(m, me.Addr) {
		m = slices.Clone(m)
		m[slices.Index(m, me.Addr)] = p.self
	}
	return m
}

// join registers this node in the cluster metadata and, if it joins a
// cluster with data, bootstraps it: PrepareJoin, streaming, FinishJoin.
// With ReplaceAddress it takes over a dead node's tokens instead
// (PrepareReplace, streaming, FinishReplace).
func (p *proxy) join() error {
	d := p.d
	me := &cluster.Node{Addr: p.self, InternalAddr: p.internalAddr(), HostID: d.local.HostID, NativeAddr: d.opts.Address.String(), NativePort: d.opts.Port,
		DataCenter: d.opts.DataCenter, Rack: d.opts.Rack, Tokens: d.local.Tokens, ReleaseVersion: ReleaseVersion}
	if r := p.opts.ReplaceAddress; r != "" {
		if cur := d.schema.Load().Nodes[me.HostID]; cur == nil || cur.Status == cluster.StatusJoining {
			return p.replace(me, r)
		}
	}
	kind := "REGISTER"
	_, err := d.change(kind, func(next *Schema) (message.Message, bool, error) {
		if len(next.CMS) == 0 {
			next.CMS = slices.Clone(p.initialCMS)
		}
		cur := next.Nodes[me.HostID]
		if cur != nil && cur.Status != cluster.StatusLeft && cur.Addr == me.Addr && cur.InternalAddr == me.InternalAddr && cur.NativeAddr == me.NativeAddr && cur.NativePort == me.NativePort && cur.Ordinal != 0 {
			return nil, false, nil
		}
		n := *me
		n.Status = cluster.StatusNormal
		if cur != nil && cur.Ordinal != 0 {
			n.Ordinal = cur.Ordinal
		} else {
			n.Ordinal = cluster.NextOrdinal(next.Nodes)
		}
		for _, o := range next.Nodes {
			if o.HostID != me.HostID && o.Status == cluster.StatusNormal {
				n.Status = cluster.StatusJoining // there is data to stream
				break
			}
		}
		if cur != nil && cur.Status != cluster.StatusLeft {
			n.Status = cur.Status // an address change
			if i := slices.Index(next.CMS, cur.Addr); i >= 0 && cur.Addr != n.Addr {
				next.CMS = slices.Clone(next.CMS)
				next.CMS[i] = n.Addr // still a member of the metadata service
			}
		}
		next.Nodes[me.HostID] = &n
		next.logDetail = fmt.Sprintf("%s %s as %s", n.HostID, n.Addr, n.Status)
		return nil, true, nil
	})
	if err != nil {
		return fmt.Errorf("db: registering in the cluster metadata: %w", err)
	}
	if d.schema.Load().Nodes[me.HostID].Status != cluster.StatusJoining {
		return nil
	}
	if err := p.bootstrap(); err != nil {
		return err
	}
	if err := p.setNodeStatus("FINISH_JOIN", me.HostID, cluster.StatusNormal); err != nil {
		return err
	}
	return d.progress().clear("bootstrap")
}

// replace joins in place of a dead node (by address or host id): this
// node takes its tokens as JOINING, so it receives the writes of its
// ranges, streams them from the other replicas, then the dead node leaves
// and this one becomes NORMAL in one commit.
func (p *proxy) replace(me *cluster.Node, target string) error {
	d := p.d
	var old *cluster.Node
	for _, n := range d.schema.Load().Nodes {
		if (n.Addr == target || n.HostID == target) && n.HostID != me.HostID && n.Status == cluster.StatusNormal {
			old = n
		}
	}
	if old == nil {
		return fmt.Errorf("db: no normal node %s to replace", target)
	}
	// A new node's membership view may not know yet that the node died:
	// ask it directly.
	if old.Addr != p.self {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := p.m.Request(ctx, old.Addr, cluster.VerbMetadataFetch, nil)
		cancel()
		if err == nil {
			return fmt.Errorf("db: cannot replace %s: it is alive", target)
		}
	}
	if slices.Contains(p.cmsMembers(), old.Addr) && old.Addr != p.self {
		return fmt.Errorf("db: cannot replace %s: it is a member of the cluster metadata service; reconfigure it first", target)
	}
	_, err := d.change("PREPARE_REPLACE", func(next *Schema) (message.Message, bool, error) {
		o := next.Nodes[old.HostID]
		if o == nil || o.Status != cluster.StatusNormal {
			return nil, false, fmt.Errorf("db: node %s changed while preparing its replacement", target)
		}
		n := *me
		n.Tokens, n.Status, n.Replaces = slices.Clone(o.Tokens), cluster.StatusJoining, o.HostID
		n.Ordinal = cluster.NextOrdinal(next.Nodes)
		next.Nodes[me.HostID] = &n
		next.logDetail = fmt.Sprintf("%s %s replaces %s %s", n.HostID, n.Addr, o.HostID, o.Addr)
		return nil, true, nil
	})
	if err != nil {
		return fmt.Errorf("db: preparing the replacement of %s: %w", target, err)
	}
	d.local.Tokens = slices.Clone(old.Tokens)
	d.ddlMu.Lock()
	err = d.saveSchema(d.schema.Load())
	d.ddlMu.Unlock()
	if err != nil {
		return err
	}
	if err := p.bootstrap(); err != nil {
		return err
	}
	_, err = d.change("FINISH_REPLACE", func(next *Schema) (message.Message, bool, error) {
		n, o := next.Nodes[me.HostID], next.Nodes[old.HostID]
		if n == nil || n.Status != cluster.StatusJoining {
			return nil, false, nil
		}
		n.Status, n.Replaces = cluster.StatusNormal, ""
		if o != nil {
			o.Status = cluster.StatusLeft
		}
		next.logDetail = fmt.Sprintf("%s replaced %s", n.HostID, old.HostID)
		return nil, true, nil
	})
	if err != nil {
		return err
	}
	return d.progress().clear("bootstrap")
}

// setStatus commits a change of this node's state.
func (p *proxy) setStatus(kind, status string) error {
	return p.setNodeStatus(kind, p.d.local.HostID, status)
}

// setNodeStatus commits a change of a node's state (FinishJoin,
// PrepareLeave, FinishLeave, and their removenode forms).
func (p *proxy) setNodeStatus(kind, hostID, status string) error {
	_, err := p.d.change(kind, func(next *Schema) (message.Message, bool, error) {
		n := next.Nodes[hostID]
		if n == nil || n.Status == status {
			return nil, false, nil
		}
		n.Status = status
		next.logDetail = fmt.Sprintf("%s %s %s", n.HostID, n.Addr, status)
		return nil, true, nil
	})
	return err
}

// nodeList returns the directory's nodes.
func nodeList(s *Schema) []cluster.Node {
	out := make([]cluster.Node, 0, len(s.Nodes))
	for _, n := range s.Nodes {
		out = append(out, *n)
	}
	return out
}

// setOwnedRanges tells each table's storage the token ranges this node
// replicates for its keyspace, over which compaction measures densities
// and cuts shards.
func (p *proxy) setOwnedRanges(s *Schema) {
	ring := p.ring.Load()
	if ring.Empty() {
		return
	}
	// The engine's token space flips the sign bit of Murmur3's tokens.
	flip := func(t int64) uint64 { return uint64(t) ^ (1 << 63) }
	for _, t := range p.d.userTables() {
		k := s.Keyspaces[t.Keyspace]
		lt, err := p.d.engine.Table(t.Storage)
		if k == nil || err != nil {
			continue
		}
		var owned []lsm.TokenRange
		for _, r := range ring.Ranges() {
			if !containsAddr(ring.Replicas(k.Replication, r[1]), p.self) {
				continue
			}
			// (start, end]; one wrapping around the ring is two.
			if r[0] < r[1] {
				owned = append(owned, lsm.TokenRange{First: flip(r[0] + 1), Last: flip(r[1])})
				continue
			}
			if r[0] < math.MaxInt64 {
				owned = append(owned, lsm.TokenRange{First: flip(r[0] + 1), Last: flip(math.MaxInt64)})
			}
			owned = append(owned, lsm.TokenRange{First: flip(math.MinInt64), Last: flip(r[1])})
		}
		lt.SetOwnedRanges(owned)
	}
}

// metadataChanged is called when a new metadata snapshot is installed.
func (p *proxy) metadataChanged(s *Schema) {
	replaced := map[string]bool{}
	for _, n := range s.Nodes {
		if n.Status == cluster.StatusJoining && n.Replaces != "" {
			replaced[n.Replaces] = true
		}
	}
	p.replaced.Store(&replaced)
	p.ring.Store(cluster.NewRing(nodeList(s)))
	p.setOwnedRanges(s)
	p.accordTopology(s)
	p.g.Update(func(st *cluster.EndpointState) { st.Epoch = s.Epoch })
}

// membershipChanged catches up with a node that gossips a newer epoch
// (TCM's peer log fetching).
func (p *proxy) membershipChanged() {
	epoch := p.d.schema.Load().Epoch
	for _, s := range p.g.States() {
		if s.Epoch > epoch && s.Addr != p.self && p.g.IsAlive(s.Addr) {
			go func() { _ = p.cms.CatchUp(s.Addr) }()
			return
		}
	}
}

// disseminate pushes the entry of an epoch to the live nodes after a
// commit (TCM's log replication).
func (p *proxy) disseminate(epoch int64) {
	e, ok := p.d.entryAt(epoch)
	if !ok {
		return // already replaced by a snapshot: nodes catch up by gossip
	}
	var nodes []string
	for _, n := range p.d.schema.Load().Nodes {
		if n.Addr != p.self && n.Status != cluster.StatusLeft && p.g.IsAlive(n.Addr) {
			nodes = append(nodes, n.Addr)
		}
	}
	p.cms.Push(nodes, e)
}

func (p *proxy) stop() {
	close(p.stopCh)
	p.wg.Wait()
	if p.accord != nil {
		p.accord.Stop()
	}
	p.g.Stop()
	p.m.Close()
}

// background replays hints and pulls newer schemas periodically.
func (p *proxy) background() {
	defer p.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	n := 0
	for {
		select {
		case <-p.stopCh:
			return
		case <-t.C:
		}
		n++
		p.membershipChanged()
		if n%5 == 0 {
			p.replayHints()
		}
	}
}

// liveReplicas returns a partition's replicas, this node first, then the
// live ones, then the down ones; n is the number of live replicas.
func (p *proxy) replicas(k *Keyspace, token int64, cl primitive.ConsistencyLevel) (reps []*cluster.Node, live int) {
	all := p.ring.Load().Replicas(k.Replication, token)
	local := cl == primitive.ConsistencyLevelLocalOne || cl == primitive.ConsistencyLevelLocalQuorum || cl == primitive.ConsistencyLevelLocalSerial
	var up, down []*cluster.Node
	for _, r := range all {
		if local && r.DataCenter != p.d.opts.DataCenter {
			continue
		}
		if p.up(r) {
			up = append(up, r)
		} else {
			down = append(down, r)
		}
	}
	slices.SortStableFunc(up, func(a, b *cluster.Node) int {
		switch {
		case a.Addr == p.self:
			return -1
		case b.Addr == p.self:
			return 1
		}
		return 0
	})
	return append(up, down...), len(up)
}

// ackNeed is what a consistency level needs from a partition's replicas:
// a number of responses, or, for EACH_QUORUM under NetworkTopologyStrategy,
// a number per data center.
type ackNeed struct {
	total int
	perDC map[string]int
}

// acks counts responses, in total and per data center.
type acks struct {
	total int
	perDC map[string]int
}

func (a *acks) add(n *cluster.Node) {
	a.total++
	if a.perDC == nil {
		a.perDC = map[string]int{}
	}
	a.perDC[n.DataCenter]++
}

func (n ackNeed) met(a acks) bool {
	if n.perDC != nil {
		for dc, need := range n.perDC {
			if a.perDC[dc] < need {
				return false
			}
		}
		return true
	}
	return a.total >= n.total
}

// need returns what cl requires of a partition's replicas, pending ones
// included (ConsistencyLevel.blockForWrite counts every pending replica).
func (p *proxy) need(cl primitive.ConsistencyLevel, k *Keyspace, pending []*cluster.Node) ackNeed {
	_, perDC := cluster.ReplicationFactor(k.Replication)
	if cl == primitive.ConsistencyLevelEachQuorum && len(perDC) > 0 {
		n := ackNeed{perDC: map[string]int{}}
		for dc, rf := range perDC {
			n.perDC[dc] = rf/2 + 1
		}
		for _, r := range pending {
			n.perDC[r.DataCenter]++
		}
		return n
	}
	local := cl == primitive.ConsistencyLevelLocalQuorum || cl == primitive.ConsistencyLevelLocalOne || cl == primitive.ConsistencyLevelLocalSerial
	if local {
		// Only this data center's acknowledgements count
		// (DatacenterWriteResponseHandler); the others still get the write.
		dc := p.d.opts.DataCenter
		n := ackNeed{perDC: map[string]int{dc: blockFor(cl, k, dc)}}
		for _, r := range pending {
			if r.DataCenter == dc {
				n.perDC[dc]++
			}
		}
		return n
	}
	n := ackNeed{total: blockFor(cl, k, p.d.opts.DataCenter)}
	if cl != primitive.ConsistencyLevelAny {
		n.total += len(pending)
	}
	return n
}

// count is the total number of responses needed.
func (n ackNeed) count() int {
	if n.perDC == nil {
		return n.total
	}
	t := 0
	for _, x := range n.perDC {
		t += x
	}
	return t
}

// liveAcks counts the live nodes among nodes, as acks.
func (p *proxy) liveAcks(nodes []*cluster.Node) acks {
	var a acks
	for _, n := range nodes {
		if p.up(n) {
			a.add(n)
		}
	}
	return a
}

// blockFor is the number of replica responses a consistency level needs
// (ConsistencyLevel.blockFor).
func blockFor(cl primitive.ConsistencyLevel, k *Keyspace, localDC string) int {
	total, perDC := cluster.ReplicationFactor(k.Replication)
	switch cl {
	case primitive.ConsistencyLevelAny, primitive.ConsistencyLevelOne, primitive.ConsistencyLevelLocalOne:
		return 1
	case primitive.ConsistencyLevelTwo:
		return 2
	case primitive.ConsistencyLevelThree:
		return 3
	case primitive.ConsistencyLevelAll:
		return total
	case primitive.ConsistencyLevelLocalQuorum, primitive.ConsistencyLevelLocalSerial:
		if n, ok := perDC[localDC]; ok {
			return n/2 + 1
		}
	}
	return total/2 + 1 // QUORUM, EACH_QUORUM (approximated), SERIAL
}

func (p *proxy) keyspaceOf(id lsm.TableID) *Keyspace {
	s := p.d.schema.Load()
	if t := s.byStorage[id]; t != nil {
		return s.Keyspaces[t.Keyspace]
	}
	return nil
}

// ---- writes ----

func encodeWrites(ws []write) []byte {
	var b []byte
	b = binary.AppendUvarint(b, uint64(len(ws)))
	for _, w := range ws {
		b = binary.AppendUvarint(b, uint64(w.table))
		b = binary.AppendVarint(b, w.token)
		b = binary.AppendUvarint(b, uint64(len(w.key)))
		b = append(b, w.key...)
		b = binary.AppendUvarint(b, uint64(len(w.value)))
		b = append(b, w.value...)
	}
	return b
}

func decodeWrites(b []byte) ([]write, error) {
	bad := errors.New("db: corrupt mutation")
	n, k := binary.Uvarint(b)
	if k <= 0 {
		return nil, bad
	}
	b = b[k:]
	out := make([]write, 0, n)
	for i := uint64(0); i < n; i++ {
		var w write
		t, k := binary.Uvarint(b)
		if k <= 0 {
			return nil, bad
		}
		b = b[k:]
		w.table = lsm.TableID(t)
		if w.token, k = binary.Varint(b); k <= 0 {
			return nil, bad
		}
		b = b[k:]
		for _, dst := range []*[]byte{&w.key, &w.value} {
			l, k := binary.Uvarint(b)
			if k <= 0 || uint64(len(b)-k) < l {
				return nil, bad
			}
			*dst = b[k : k+int(l)]
			b = b[k+int(l):]
		}
		out = append(out, w)
	}
	return out, nil
}

func (p *proxy) handleMutation(ctx context.Context, _ string, payload []byte) ([]byte, error) {
	if f := p.testRefuseMutations.Load(); f != nil && *f {
		return nil, errors.New("db: refusing mutations (test)")
	}
	ws, err := decodeWrites(payload)
	if err != nil {
		return nil, err
	}
	return nil, p.d.applyLocal(ws)
}

// apply writes a mutation to the replicas of its partitions
// (StorageProxy.mutate): it fails as unavailable if too few replicas are
// alive, waits for blockFor acknowledgements per partition, and stores
// hints for the replicas that are down or do not answer.
func (p *proxy) apply(m *mutation, cl primitive.ConsistencyLevel) error {
	if len(m.writes) == 0 {
		return nil
	}
	type group struct {
		need ackNeed
		acks acks
		reps []*cluster.Node // natural and pending
	}
	groups := map[string]*group{} // by keyspace and token
	perNode := map[string][]write{}
	nodes := map[string]*cluster.Node{}
	writeGroups := map[string][]*group{}
	var hints []hint
	for _, w := range m.writes {
		k := p.keyspaceOf(w.table)
		if k == nil {
			return invalidf("a table was dropped while it was written")
		}
		gk := k.Name + "/" + strconv.FormatInt(w.token, 10)
		g := groups[gk]
		if g == nil {
			// Every replica gets the write, in every data center; cl only
			// decides whose acknowledgements are awaited (need).
			reps, _ := p.replicas(k, w.token, primitive.ConsistencyLevelAll)
			// Nodes joining or taking over the range get the write too, and
			// must acknowledge it on top of the consistency level.
			pending := p.pendingReplicas(k, w.token, reps)
			g = &group{need: p.need(cl, k, pending), reps: append(reps, pending...)}
			if live := p.liveAcks(g.reps); !g.need.met(live) && cl != primitive.ConsistencyLevelAny {
				return &unavailableError{cl: cl, required: g.need.count(), alive: live.total}
			}
			groups[gk] = g
			for _, r := range g.reps {
				if p.up(r) { // (a replaced node may share its address with its replacement)
					writeGroups[r.Addr] = append(writeGroups[r.Addr], g)
				}
			}
		}
		for _, r := range g.reps {
			if p.up(r) {
				nodes[r.Addr] = r
				perNode[r.Addr] = append(perNode[r.Addr], w)
			} else {
				hints = append(hints, hint{hostID: r.HostID, w: w})
			}
		}
	}
	// Replicas still working when the consistency level is met keep the
	// write timeout to finish (and are hinted if they fail).
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.WriteTimeout)
	type ack struct {
		addr string
		err  error
	}
	acks := make(chan ack, len(perNode))
	m.trace.event("Writing %d partitions at %s to %d replicas", len(groups), clName(cl), len(perNode))
	for _, sg := range p.sendGroups(perNode, nodes) {
		if len(sg.forward) > 0 {
			m.trace.event("Sending MUTATION (%d writes) to %s, to forward to %s", len(sg.ws), sg.via, strings.Join(sg.forward, ", "))
			go func() {
				ch := p.m.RequestForward(ctx, sg.via, sg.forward, cluster.VerbMutation, sg.enc)
				for range len(sg.forward) + 1 {
					r := <-ch
					acks <- ack{r.From, r.Err}
				}
			}()
			continue
		}
		m.trace.event("Sending MUTATION (%d writes) to %s", len(sg.ws), sg.via)
		go func() {
			var err error
			if sg.via == p.self {
				err = p.d.applyLocal(sg.ws)
			} else {
				_, err = p.m.Request(ctx, sg.via, cluster.VerbMutation, encodeWrites(sg.ws))
			}
			acks <- ack{sg.via, err}
		}()
	}
	satisfied := func() bool {
		for _, g := range groups {
			if !g.need.met(g.acks) {
				return false
			}
		}
		return true
	}
	pending := map[string]bool{}
	for a := range perNode {
		pending[a] = true
	}
wait:
	for len(pending) > 0 && !satisfied() {
		select {
		case a := <-acks:
			delete(pending, a.addr)
			if a.err != nil {
				m.trace.event("MUTATION to %s failed (%v): storing a hint", a.addr, a.err)
			} else {
				m.trace.event("MUTATION acknowledged by %s", a.addr)
			}
			if a.err != nil {
				for _, w := range perNode[a.addr] {
					hints = append(hints, hint{hostID: nodes[a.addr].HostID, w: w})
				}
				continue
			}
			for _, g := range writeGroups[a.addr] {
				g.acks.add(nodes[a.addr])
			}
		case <-ctx.Done():
			break wait // the rest time out and are hinted below
		}
	}
	// Replicas that have not answered yet get hints only if they fail;
	// let them finish in the background.
	if n := len(pending); n > 0 {
		go func() {
			defer cancel()
			for i := 0; i < n; i++ {
				a := <-acks
				if a.err != nil {
					var hs []hint
					for _, w := range perNode[a.addr] {
						hs = append(hs, hint{hostID: nodes[a.addr].HostID, w: w})
					}
					p.storeHints(hs)
				}
			}
		}()
	} else {
		cancel()
	}
	stored := p.storeHints(hints)
	if !satisfied() {
		// ANY is met by a hint, if one was in fact stored.
		if cl == primitive.ConsistencyLevelAny && stored > 0 {
			return nil
		}
		minAcks, need := 1<<30, 0
		for _, g := range groups {
			if !g.need.met(g.acks) && g.acks.total < minAcks {
				minAcks, need = g.acks.total, g.need.count()
			}
		}
		return &timeoutError{write: true, cl: cl, received: minAcks, blockFor: need, batch: len(groups) > 1}
	}
	return nil
}

// sendGroup is one message of a write: to via, which forwards it to the
// others in its data center.
type sendGroup struct {
	via     string
	forward []string
	ws      []write
	enc     []byte // ws encoded, for a forwarded message
}

// sendGroups plans the messages of a write (sendToHintedReplicas): one per
// replica in this data center; in each other data center, one per set of
// replicas receiving the same writes, sent to a random one of them, which
// forwards it to the others (sendMessagesToNonlocalDC), so the write
// crosses to each data center once.
func (p *proxy) sendGroups(perNode map[string][]write, nodes map[string]*cluster.Node) []sendGroup {
	var out []sendGroup
	remote := map[string][]string{} // by data center and writes
	encs := map[string][]byte{}
	var keys []string
	for addr, ws := range perNode {
		n := nodes[addr]
		if addr == p.self || n.DataCenter == p.d.opts.DataCenter {
			out = append(out, sendGroup{via: addr, ws: ws})
			continue
		}
		enc := encodeWrites(ws)
		key := n.DataCenter + "\x00" + string(enc)
		if remote[key] == nil {
			keys = append(keys, key)
			encs[key] = enc
		}
		remote[key] = append(remote[key], addr)
	}
	for _, key := range keys {
		addrs := remote[key]
		i := rand.IntN(len(addrs))
		addrs[0], addrs[i] = addrs[i], addrs[0]
		out = append(out, sendGroup{via: addrs[0], forward: addrs[1:], ws: perNode[addrs[0]], enc: encs[key]})
	}
	return out
}

// ---- hints (HintsService) ----

type hint struct {
	hostID string
	w      write
}

// storeHints stores hints for writes nodes missed, returning how many
// writes were hinted (none when hints are disabled, or a node has been
// down past the hint window).
func (p *proxy) storeHints(hs []hint) int {
	if len(hs) == 0 || p.opts.DisableHints {
		return 0
	}
	dir := p.d.schema.Load().Nodes
	by := map[string][]write{}
	for _, h := range hs {
		if n := dir[h.hostID]; n == nil || n.Status == cluster.StatusLeft || !p.withinHintWindow(n) {
			continue
		}
		by[h.hostID] = append(by[h.hostID], h.w)
	}
	var ws []write
	hinted := 0
	now := time.Now()
	for host, writes := range by {
		id, err := types.ParseUUID(host)
		if err != nil {
			continue
		}
		// A hint lives as long as the smallest gc_grace_seconds of its
		// tables: replayed later, it could resurrect deleted data.
		ttl := p.hintTTL(writes)
		if ttl == 0 {
			continue
		}
		key := append(id[:], binary.BigEndian.AppendUint64(nil, uint64(now.UnixMicro()))...)
		key = binary.BigEndian.AppendUint64(key, p.seq.Add(1))
		v := row.Append(nil, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp},
			Cells: []row.Cell{{Column: 1, Timestamp: now.UnixMicro(), TTL: uint32(ttl), LocalDeletionTime: now.Unix() + ttl, Value: encodeWrites(writes)}}})
		ws = append(ws, write{table: hintsStorage, key: key, value: v})
		hinted += len(writes)
	}
	if p.d.applyLocal(ws) != nil {
		return 0
	}
	return hinted
}

// withinHintWindow reports whether a node has been down for less than
// max_hint_window (a node down since before this node started counts
// from the start).
func (p *proxy) withinHintWindow(n *cluster.Node) bool {
	since := p.started
	if v, ok := p.downSince.Load(n.Addr); ok {
		since = v.(time.Time)
	} else if p.g.IsAlive(n.Addr) {
		return true // up (the write failed or timed out)
	}
	return time.Since(since) < p.opts.MaxHintWindow
}

// hintTTL is the smallest gc_grace_seconds of the writes' tables.
func (p *proxy) hintTTL(ws []write) int64 {
	s := p.d.schema.Load()
	ttl := int64(math.MaxInt32)
	for _, w := range ws {
		if t := s.byStorage[w.table]; t != nil {
			ttl = min(ttl, int64(t.Params.GCGraceSeconds))
		}
	}
	return ttl
}

// replayHints sends stored hints to the live nodes they are for, deleting
// the delivered ones, at hinted_handoff_throttle bytes per second shared
// by the nodes being replayed to. Hints for nodes that left are dropped.
func (p *proxy) replayHints() {
	if !p.hintMu.TryLock() {
		return
	}
	defer p.hintMu.Unlock()
	lt, err := p.d.engine.Table(hintsStorage)
	if err != nil {
		return
	}
	dir := p.d.schema.Load().Nodes
	others := 0
	for _, n := range dir {
		if n.Addr != p.self && n.Status != cluster.StatusLeft {
			others++
		}
	}
	rate := float64(p.opts.HintThrottle) / float64(max(others, 1))
	it, err := lt.NewIter(nil, nil)
	if err != nil {
		return
	}
	defer it.Close()
	nowSec := time.Now().Unix()
	var done []write
	tombstone := func(k []byte) {
		now := time.Now()
		done = append(done, write{table: hintsStorage, key: slices.Clone(k),
			value: row.Append(nil, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: now.UnixMicro(), LocalDeletionTime: now.Unix()}})})
	}
	failed := map[string]bool{}
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if len(k) < 16 {
			continue
		}
		host := types.FormatUUID(k[:16])
		n := dir[host]
		if n == nil || n.Status == cluster.StatusLeft {
			tombstone(k) // the node left: nobody will take these
			continue
		}
		if failed[host] || n.Addr == p.self || !p.g.IsAlive(n.Addr) {
			continue
		}
		r, err := row.Decode(it.Value())
		if err != nil {
			continue
		}
		live := r.Live(nowSec)
		if len(live) == 0 {
			continue // expired (purged by compaction)
		}
		payload, stale := p.liveHint(live[0].Value)
		if stale {
			// Its table was dropped or truncated since: nobody can take
			// it, and keeping it would hold up the host's later hints.
			tombstone(k)
			continue
		}
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), p.opts.WriteTimeout)
		_, err = p.m.Request(ctx, n.Addr, cluster.VerbMutation, payload)
		cancel()
		if err != nil {
			if strings.Contains(err.Error(), "was dropped while it was written") {
				tombstone(k) // dropped meanwhile, as above
				continue
			}
			failed[host] = true
			continue
		}
		tombstone(k)
		// Throttle: this hint's bytes take len/rate seconds.
		if wait := time.Duration(float64(len(payload))/rate*float64(time.Second)) - time.Since(start); wait > 0 {
			select {
			case <-time.After(wait):
			case <-p.stopCh:
				_ = p.d.applyLocal(done)
				return
			}
		}
		if len(done) >= 256 {
			_ = p.d.applyLocal(done)
			done = nil
		}
	}
	_ = p.d.applyLocal(done)
}

// liveHint drops from a hint's writes those to tables that no longer exist
// (dropped, or truncated: a truncate gives the table new storage), and
// reports whether nothing is left to deliver.
func (p *proxy) liveHint(payload []byte) ([]byte, bool) {
	ws, err := decodeWrites(payload)
	if err != nil {
		return payload, false // let the replica judge it
	}
	keep := ws[:0]
	for _, w := range ws {
		if _, err := p.d.engine.Table(w.table); errors.Is(err, lsm.ErrUnknownTable) {
			continue
		}
		keep = append(keep, w)
	}
	switch {
	case len(keep) == 0:
		return nil, true
	case len(keep) < len(ws):
		return encodeWrites(keep), false
	}
	return payload, false
}

// ---- reads ----

func (p *proxy) source(t *Table, id lsm.TableID, cl primitive.ConsistencyLevel) (source, error) {
	if _, err := p.d.engine.Table(id); err != nil {
		return nil, invalidf("table %s.%s was dropped", t.Keyspace, t.Name)
	}
	switch cl {
	case primitive.ConsistencyLevelAny:
		return nil, invalidf("ANY ConsistencyLevel is only supported for writes")
	case primitive.ConsistencyLevelSerial:
		cl = primitive.ConsistencyLevelQuorum
	case primitive.ConsistencyLevelLocalSerial:
		cl = primitive.ConsistencyLevelLocalQuorum
	}
	k := p.d.schema.Load().Keyspaces[t.Keyspace]
	if k == nil || k.virtual {
		return localStorage{p.d}.source(t, id, cl)
	}
	return &distSource{p: p, k: k, id: id, cl: cl}, nil
}

type distSource struct {
	trace *Trace
	p     *proxy
	k     *Keyspace
	id    lsm.TableID
	cl    primitive.ConsistencyLevel
}

// policy is the table's speculative_retry option.
func (s *distSource) policy() string {
	if t := s.p.d.schema.Load().byStorage[s.id]; t != nil {
		return t.Params.SpeculativeRetry
	}
	return "99p"
}

func encodeKey(id lsm.TableID, parts ...[]byte) []byte {
	b := binary.AppendUvarint(nil, uint64(id))
	for _, p := range parts {
		b = binary.AppendUvarint(b, uint64(len(p)))
		b = append(b, p...)
	}
	return b
}

func decodeParts(b []byte, n int) (lsm.TableID, [][]byte, error) {
	bad := errors.New("db: corrupt read request")
	id, k := binary.Uvarint(b)
	if k <= 0 {
		return 0, nil, bad
	}
	b = b[k:]
	out := make([][]byte, n)
	for i := range out {
		l, k := binary.Uvarint(b)
		if k <= 0 || uint64(len(b)-k) < l {
			return 0, nil, bad
		}
		out[i] = b[k : k+int(l)]
		b = b[k+int(l):]
	}
	return lsm.TableID(id), out, nil
}

func (p *proxy) handleReadRow(ctx context.Context, _ string, payload []byte) ([]byte, error) {
	id, parts, err := decodeParts(payload, 1)
	if err != nil {
		return nil, err
	}
	lt, err := p.d.engine.Table(id)
	if err != nil {
		return nil, err
	}
	v, err := lt.Get(parts[0])
	if errors.Is(err, lsm.ErrNotFound) {
		return []byte{0}, nil
	}
	if err != nil {
		return nil, err
	}
	return append([]byte{1}, v...), nil
}

// readTargets returns the live replicas to read token from, this node
// first (for EACH_QUORUM, a quorum of each data center first), and what
// the consistency level needs of them.
func (s *distSource) readTargets(token int64) ([]*cluster.Node, ackNeed, error) {
	reps, live := s.p.replicas(s.k, token, s.cl)
	reps = reps[:live]
	need := s.p.need(s.cl, s.k, nil)
	if a := s.p.liveAcks(reps); !need.met(a) {
		return nil, need, &unavailableError{cl: s.cl, required: need.count(), alive: a.total}
	}
	if need.perDC != nil {
		var first, rest []*cluster.Node
		taken := map[string]int{}
		for _, r := range reps {
			if taken[r.DataCenter] < need.perDC[r.DataCenter] {
				taken[r.DataCenter]++
				first = append(first, r)
			} else {
				rest = append(rest, r)
			}
		}
		reps = append(first, rest...)
	}
	return reps, need, nil
}

// readLatency tracks a table's replica read latencies, for its
// speculative retry policy.
type readLatency struct {
	mu      sync.Mutex
	samples [256]time.Duration
	n       int
	// pct caches percentiles, recomputed every 32 samples.
	pct map[float64]time.Duration
}

func (l *readLatency) add(d time.Duration) {
	l.mu.Lock()
	l.samples[l.n%len(l.samples)] = d
	l.n++
	if l.n%32 == 0 {
		l.pct = nil
	}
	l.mu.Unlock()
}

// percentile returns the p-th percentile of recent reads, 0 before any.
func (l *readLatency) percentile(p float64) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	if v, ok := l.pct[p]; ok {
		return v
	}
	n := min(l.n, len(l.samples))
	if n == 0 {
		return 0
	}
	c := slices.Clone(l.samples[:n])
	slices.Sort(c)
	v := c[min(int(float64(n)*p/100), n-1)]
	if l.pct == nil {
		l.pct = map[float64]time.Duration{}
	}
	l.pct[p] = v
	return v
}

// latency returns a table's read latency tracker.
func (p *proxy) latency(id lsm.TableID) *readLatency {
	v, _ := p.lats.LoadOrStore(id, &readLatency{})
	return v.(*readLatency)
}

// speculateAfter is when a read of a table asks another replica, by its
// speculative_retry option (SpeculativeRetryPolicy): "99p" (a percentile
// of recent replica reads, 50ms until there are some), "20ms", "ALWAYS",
// "NONE", and MIN(..., ...) or MAX(..., ...) of two. never is true for
// NONE.
func speculateAfter(policy string, l *readLatency, timeout time.Duration) (d time.Duration, never bool) {
	p := strings.ToUpper(strings.TrimSpace(policy))
	cap := func(d time.Duration) time.Duration { return min(max(d, 0), timeout/2) }
	switch {
	case p == "NONE":
		return 0, true
	case p == "ALWAYS":
		return 0, false
	case (strings.HasPrefix(p, "MIN(") || strings.HasPrefix(p, "MAX(")) && strings.HasSuffix(p, ")"):
		a, b, ok := strings.Cut(p[4:len(p)-1], ",")
		if !ok {
			break
		}
		da, na := speculateAfter(a, l, timeout)
		db, nb := speculateAfter(b, l, timeout)
		if p[:3] == "MIN" {
			switch {
			case na:
				return db, nb
			case nb:
				return da, na
			}
			return min(da, db), false
		}
		if na || nb {
			return 0, true
		}
		return max(da, db), false
	case strings.HasSuffix(p, "MS"):
		if v, err := strconv.ParseFloat(strings.TrimSuffix(p, "MS"), 64); err == nil {
			return cap(time.Duration(v * float64(time.Millisecond))), false
		}
	case strings.HasSuffix(p, "P") || strings.HasSuffix(p, "PERCENTILE"):
		v, err := strconv.ParseFloat(strings.TrimSuffix(strings.TrimSuffix(p, "PERCENTILE"), "P"), 64)
		if err == nil && v > 0 && v <= 100 {
			d := l.percentile(v)
			if d <= 0 {
				d = 50 * time.Millisecond
			}
			return cap(max(d, time.Millisecond)), false
		}
	}
	d = l.percentile(99) // unparsable: the default
	if d <= 0 {
		d = 50 * time.Millisecond
	}
	return cap(d), false
}

func rowDigest(found bool, v []byte) []byte {
	if !found {
		return []byte{0}
	}
	h := sha256.Sum256(v)
	return append([]byte{1}, h[:16]...)
}

func (p *proxy) handleReadDigest(ctx context.Context, from string, payload []byte) ([]byte, error) {
	b, err := p.handleReadRow(ctx, from, payload)
	if err != nil {
		return nil, err
	}
	return rowDigest(b[0] == 1, b[1:]), nil
}

// get reads a row (a ReadCommand on one partition key): the full row from
// one replica and digests from the others needed. Matching digests answer
// at once; a mismatch reads the full row from every replica that answered,
// merges, and repairs those that returned less (blocking read repair). A
// replica slower than the usual 99th percentile, or failing, gets another
// replica asked in its place (speculative retry).
func (s *distSource) get(key []byte) ([]byte, error) {
	reps, need, err := s.readTargets(tokenOf(key))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.p.opts.ReadTimeout)
	defer cancel()
	type resp struct {
		node   *cluster.Node
		data   bool
		found  bool
		v      []byte // the row, or the digest
		err    error
		digest []byte
	}
	ch := make(chan resp, len(reps))
	payload := encodeKey(s.id, key)
	lat := s.p.latency(s.id)
	ask := func(r *cluster.Node, data bool) {
		start := time.Now()
		verb := cluster.VerbReadDigest
		if data {
			verb = cluster.VerbReadRow
		}
		b, err := s.p.m.Request(ctx, r.Addr, verb, payload)
		if err == nil && len(b) == 0 {
			err = errors.New("empty response")
		}
		if err != nil {
			ch <- resp{node: r, data: data, err: err}
			return
		}
		lat.add(time.Since(start))
		if data {
			ch <- resp{node: r, data: true, found: b[0] == 1, v: b[1:], digest: rowDigest(b[0] == 1, b[1:])}
		} else {
			ch <- resp{node: r, digest: b}
		}
	}
	initial := need.count()
	s.trace.event("Reading a row at %s: data from %s, digests from %d replicas", clName(s.cl), reps[0].Addr, initial-1)
	go ask(reps[0], true)
	for _, r := range reps[1:initial] {
		go ask(r, false)
	}
	next, outstanding := initial, initial
	var got []resp
	var answered acks
	haveData := false
	after, never := speculateAfter(s.policy(), lat, s.p.opts.ReadTimeout)
	speculate := time.NewTimer(after)
	if never {
		speculate.Stop()
	}
	defer speculate.Stop()
	timeout := func() error {
		return &timeoutError{cl: s.cl, received: answered.total, blockFor: need.count()}
	}
	for !(haveData && need.met(answered)) {
		select {
		case r := <-ch:
			outstanding--
			if r.err != nil {
				// Ask another replica in its place (of the same data center
				// for EACH_QUORUM).
				for next < len(reps) && need.perDC != nil && reps[next].DataCenter != r.node.DataCenter {
					next++
				}
				if next < len(reps) {
					s.trace.event("Read from %s failed (%v): asking %s", r.node.Addr, r.err, reps[next].Addr)
					go ask(reps[next], r.data || !haveData)
					next++
					outstanding++
					continue
				}
				if outstanding == 0 {
					return nil, timeout()
				}
				continue
			}
			got = append(got, r)
			answered.add(r.node)
			haveData = haveData || r.data
		case <-speculate.C:
			if need.perDC == nil && next < len(reps) {
				s.trace.event("Speculating read retry on %s", reps[next].Addr)
				go ask(reps[next], !haveData)
				next++
				outstanding++
			}
		case <-ctx.Done():
			return nil, timeout()
		}
	}
	var data *resp
	match := true
	for i := range got {
		if got[i].data && data == nil {
			data = &got[i]
		}
	}
	for _, r := range got {
		if !bytes.Equal(r.digest, data.digest) {
			match = false
		}
	}
	if match {
		if !data.found {
			return nil, errNotFound
		}
		return data.v, nil
	}
	// Digest mismatch: full rows from everyone who answered.
	s.trace.event("Digest mismatch: reading full rows from %d replicas", len(got))
	full := make(chan resp, len(got))
	for _, r := range got {
		if r.data {
			full <- r
			continue
		}
		go ask(r.node, true)
	}
	var rows []resp
	for len(rows) < len(got) {
		select {
		case r := <-full:
			rows = append(rows, r)
		case r := <-ch:
			if r.err != nil {
				return nil, timeout()
			}
			if r.data {
				rows = append(rows, r)
			}
		case <-ctx.Done():
			return nil, timeout()
		}
	}
	var merged []byte
	for _, r := range rows {
		if !r.found {
			continue
		}
		if merged == nil {
			merged = r.v
			continue
		}
		if merged, err = row.Merge(nil, merged, r.v); err != nil {
			return nil, err
		}
	}
	if merged == nil {
		return nil, errNotFound
	}
	var stale []string
	for _, r := range rows {
		if !r.found || !bytes.Equal(r.v, merged) {
			stale = append(stale, r.node.Addr)
		}
	}
	if len(stale) > 0 && s.readRepair() {
		// Blocking: the read returns once the replicas it read agree, or
		// times out (BlockingReadRepair), so a later read at the same
		// consistency sees at least as much.
		s.trace.event("Read repair of %v", stale)
		if err := s.p.repair(stale, []write{{table: s.id, key: key, value: merged, token: tokenOf(key)}}); err != nil {
			s.trace.event("Read repair failed: %v", err)
			return nil, timeout()
		}
	}
	return merged, nil
}

// repair writes merged rows to replicas that were behind (ReadRepair).
func (p *proxy) repair(addrs []string, ws []write) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.WriteTimeout)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, len(addrs))
	for _, a := range addrs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if a == p.self {
				errs <- p.d.applyLocal(ws)
				return
			}
			_, err := p.m.Request(ctx, a, cluster.VerbMutation, encodeWrites(ws))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// readRepair reports whether reads of the table repair the replicas they
// read (read_repair 'BLOCKING', the default) or not ('NONE').
func (s *distSource) readRepair() bool {
	if t := s.p.d.schema.Load().byStorage[s.id]; t != nil {
		return t.Params.ReadRepair != "NONE"
	}
	return true
}

const rangePage = 512

func (p *proxy) handleReadRange(ctx context.Context, _ string, payload []byte) ([]byte, error) {
	id, parts, err := decodeParts(payload, 2)
	if err != nil {
		return nil, err
	}
	lt, err := p.d.engine.Table(id)
	if err != nil {
		return nil, err
	}
	lo, hi := parts[0], parts[1]
	if len(hi) == 0 {
		hi = nil
	}
	it, err := lt.NewIter(lo, hi)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []byte
	n := 0
	more := byte(0)
	for ok := it.First(); ok; ok = it.Next() {
		if n == rangePage {
			more = 1
			break
		}
		if n%64 == 0 && ctx.Err() != nil {
			return nil, ctx.Err() // the coordinator gave up
		}
		out = binary.AppendUvarint(out, uint64(len(it.Key())))
		out = append(out, it.Key()...)
		out = binary.AppendUvarint(out, uint64(len(it.Value())))
		out = append(out, it.Value()...)
		n++
	}
	return append([]byte{more}, out...), it.Error()
}

// handleReadRangeReverse serves a page of a key range from its end down.
func (p *proxy) handleReadRangeReverse(ctx context.Context, _ string, payload []byte) ([]byte, error) {
	id, parts, err := decodeParts(payload, 2)
	if err != nil {
		return nil, err
	}
	lt, err := p.d.engine.Table(id)
	if err != nil {
		return nil, err
	}
	lo, hi := parts[0], parts[1]
	if len(lo) == 0 {
		lo = nil
	}
	if len(hi) == 0 {
		hi = nil
	}
	it, err := lt.NewReverseIter(lo, hi)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []byte
	n := 0
	more := byte(0)
	for ok := it.Last(); ok; ok = it.Prev() {
		if n == rangePage {
			more = 1
			break
		}
		if n%64 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		out = binary.AppendUvarint(out, uint64(len(it.Key())))
		out = append(out, it.Key()...)
		out = binary.AppendUvarint(out, uint64(len(it.Value())))
		out = append(out, it.Value()...)
		n++
	}
	return append([]byte{more}, out...), it.Error()
}

// errNoReverseReads: a replica does not know reverse range reads (an older
// version); the query reads forward instead.
var errNoReverseReads = errors.New("db: a replica does not read backwards")

// riter reads [lo, hi) of one partition backwards across its replicas.
func (s *distSource) riter(lo, hi []byte) (revIterator, error) {
	if lo == nil || hi == nil || keyToken(lo) != keyToken(hi) {
		return nil, errNoReverseReads // only within one partition
	}
	return &revDistIter{s: s, lo: lo, hi: hi}, nil
}

// revDistIter reads a range of one partition from its end down: a page per
// replica at a time, merged in reverse order.
type revDistIter struct {
	s      *distSource
	lo, hi []byte
	buf    []kv
	i      int
	below  []byte // the next page is below this key (nil: no more)
	err    error
}

func (it *revDistIter) load(bound []byte) bool {
	it.buf, it.i, it.below = nil, 0, nil
	for {
		reps, need, err := it.s.readTargets(keyToken(it.lo))
		if err != nil {
			it.err = err
			return false
		}
		kvs, cutoff, err := it.s.fetch(reps, need, it.lo, bound, true)
		if err != nil {
			it.err = err
			return false
		}
		it.below = cutoff
		if len(kvs) > 0 {
			it.buf = kvs
			return true
		}
		if cutoff == nil {
			return false
		}
		bound = cutoff
	}
}

func (it *revDistIter) SeekLT(key []byte) bool {
	if key == nil || bytes.Compare(key, it.hi) > 0 {
		key = it.hi
	}
	return it.load(key)
}

func (it *revDistIter) Prev() bool {
	if it.buf == nil {
		return false
	}
	it.i++
	if it.i < len(it.buf) {
		return true
	}
	if it.below == nil {
		it.buf = nil
		return false
	}
	return it.load(it.below)
}

func (it *revDistIter) Valid() bool   { return it.buf != nil && it.i < len(it.buf) && it.err == nil }
func (it *revDistIter) Key() []byte   { return it.buf[it.i].k }
func (it *revDistIter) Value() []byte { return it.buf[it.i].v }
func (it *revDistIter) Error() error  { return it.err }
func (it *revDistIter) Close() error  { return nil }

func decodeKVs(b []byte) (kvs []kv, more bool, err error) {
	if len(b) == 0 {
		return nil, false, errors.New("db: corrupt range response")
	}
	more, b = b[0] == 1, b[1:]
	for len(b) > 0 {
		var e kv
		for _, dst := range []*[]byte{&e.k, &e.v} {
			l, k := binary.Uvarint(b)
			if k <= 0 || uint64(len(b)-k) < l {
				return nil, false, errors.New("db: corrupt range response")
			}
			*dst = b[k : k+int(l)]
			b = b[k+int(l):]
		}
		kvs = append(kvs, e)
	}
	return kvs, more, nil
}

func (s *distSource) iter(lo, hi []byte) (iterator, error) {
	return &distIter{s: s, lo: lo, hi: hi}, nil
}

// distIter iterates a key range across the ring: it loads one ring range
// at a time from blockFor of its replicas, a page per replica, merging
// their rows; keys past the first replica page that was cut are left for
// the next load (as range slices are paged in Cassandra).
type distIter struct {
	s          *distSource
	lo, hi     []byte
	buf        []kv
	i          int
	start, end []byte // the loaded span, [start, end); end nil = to hi
	done       bool
	err        error
}

func keyToken(k []byte) int64 {
	if len(k) < 8 {
		var b [8]byte
		copy(b[:], k)
		return tokenOf(b[:])
	}
	return tokenOf(k)
}

// load reads from key from onwards; it reports whether a row was found.
func (it *distIter) load(from []byte) bool {
	it.buf, it.i = nil, 0
	for {
		if it.hi != nil && from != nil && bytes.Compare(from, it.hi) >= 0 {
			it.done = true
			return false
		}
		tok := keyToken(from)
		if from == nil {
			tok = -1 << 63
		}
		endTok, wraps := it.s.p.ring.Load().RangeEnd(tok)
		var segEnd []byte
		if !wraps && endTok != 1<<63-1 {
			segEnd = appendToken(nil, endTok+1)
		}
		if it.hi != nil && (segEnd == nil || bytes.Compare(it.hi, segEnd) < 0) {
			segEnd = it.hi
		}
		reps, need, err := it.s.readTargets(tok)
		if err != nil {
			it.err = err
			return false
		}
		kvs, cutoff, err := it.fetch(reps, need, from, segEnd)
		if err != nil {
			it.err = err
			return false
		}
		it.start = from
		if cutoff != nil {
			it.end = append(slices.Clone(cutoff), 0)
		} else {
			it.end = segEnd
		}
		if len(kvs) > 0 {
			it.buf = kvs
			return true
		}
		if it.end == nil {
			it.done = true
			return false
		}
		from = it.end
	}
}

// fetch reads [from, to) from need replicas and merges their rows; cutoff
// is the last complete key when a replica's page was cut.
func (it *distIter) fetch(reps []*cluster.Node, need ackNeed, from, to []byte) ([]kv, []byte, error) {
	return it.s.fetch(reps, need, from, to, false)
}

// fetch reads [from, to) from need replicas and merges their rows, in key
// order or, reverse, from the end down (as Scylla's native reverse reads:
// every replica answers in reverse order, and the merge, cutoff and read
// repair are mirrored). cutoff is the last complete key when a replica's
// page was cut (the smallest one, reversed: rows below it may be missing).
func (s *distSource) fetch(reps []*cluster.Node, need ackNeed, from, to []byte, reverse bool) ([]kv, []byte, error) {
	it := &distIter{s: s}
	p := it.s.p
	verb := cluster.VerbReadRange
	if reverse {
		verb = cluster.VerbReadRangeReverse
	}
	unknownVerb := false
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.ReadTimeout)
	defer cancel()
	type resp struct {
		node *cluster.Node
		kvs  []kv
		more bool
		err  error
	}
	ch := make(chan resp, len(reps))
	ask := func(r *cluster.Node) {
		b, err := p.m.Request(ctx, r.Addr, verb, encodeKey(it.s.id, from, to))
		if err != nil {
			ch <- resp{node: r, err: err}
			return
		}
		kvs, more, err := decodeKVs(b)
		ch <- resp{node: r, kvs: kvs, more: more, err: err}
	}
	initial := need.count()
	for _, r := range reps[:initial] {
		it.s.trace.event("Reading a range from %s", r.Addr)
		go ask(r)
	}
	asked := map[string]bool{}
	for _, r := range reps[:initial] {
		asked[r.Addr] = true
	}
	var got []resp
	var answered acks
	timeout := func() error {
		return &timeoutError{cl: it.s.cl, received: answered.total, blockFor: need.count()}
	}
	outstanding := initial
	for !need.met(answered) {
		select {
		case r := <-ch:
			outstanding--
			if r.err != nil {
				if strings.Contains(r.err.Error(), "no handler for verb") {
					unknownVerb = true // an older node
				}
				// Another replica in its place, of the same data center
				// for EACH_QUORUM.
				for _, alt := range reps {
					if !asked[alt.Addr] && (need.perDC == nil || alt.DataCenter == r.node.DataCenter) {
						asked[alt.Addr] = true
						outstanding++
						go ask(alt)
						break
					}
				}
				if outstanding == 0 {
					if unknownVerb {
						return nil, nil, errNoReverseReads
					}
					return nil, nil, timeout()
				}
				continue
			}
			got = append(got, r)
			answered.add(r.node)
		case <-ctx.Done():
			return nil, nil, timeout()
		}
	}
	// Rows past the first truncated page are not complete everywhere.
	var cutoff []byte
	for _, r := range got {
		if r.more && len(r.kvs) > 0 {
			last := r.kvs[len(r.kvs)-1].k
			if cutoff == nil || (bytes.Compare(last, cutoff) < 0) != reverse {
				cutoff = last
			}
		}
	}
	merged := map[string][]byte{}
	for _, r := range got {
		for _, e := range r.kvs {
			if cutoff != nil && (bytes.Compare(e.k, cutoff) > 0) != reverse && !bytes.Equal(e.k, cutoff) {
				continue
			}
			if cur, ok := merged[string(e.k)]; ok {
				m, err := row.Merge(nil, cur, e.v)
				if err != nil {
					return nil, nil, err
				}
				merged[string(e.k)] = m
			} else {
				merged[string(e.k)] = e.v
			}
		}
	}
	out := make([]kv, 0, len(merged))
	for k, v := range merged {
		out = append(out, kv{[]byte(k), v})
	}
	sort.Slice(out, func(i, j int) bool { return (bytes.Compare(out[i].k, out[j].k) < 0) != reverse })
	// Blocking read repair: replicas that returned less get the merged rows
	// (DataResolver).
	if len(got) > 1 && it.s.readRepair() {
		for _, r := range got {
			have := make(map[string][]byte, len(r.kvs))
			for _, e := range r.kvs {
				have[string(e.k)] = e.v
			}
			var ws []write
			for _, e := range out {
				if v, ok := have[string(e.k)]; !ok || !bytes.Equal(v, e.v) {
					ws = append(ws, write{table: it.s.id, key: e.k, value: e.v, token: tokenOf(e.k)})
				}
			}
			if len(ws) > 0 {
				if err := p.repair([]string{r.node.Addr}, ws); err != nil {
					it.s.trace.event("Read repair of %s failed: %v", r.node.Addr, err)
					return nil, nil, timeout()
				}
			}
		}
	}
	return out, cutoff, nil
}

func (it *distIter) First() bool {
	it.done, it.err = false, nil
	return it.load(it.lo)
}

func (it *distIter) SeekGE(key []byte) bool {
	if it.lo != nil && bytes.Compare(key, it.lo) < 0 {
		key = it.lo
	}
	if it.buf != nil && bytes.Compare(key, it.start) >= 0 && (it.end == nil || bytes.Compare(key, it.end) < 0) {
		i := sort.Search(len(it.buf), func(i int) bool { return bytes.Compare(it.buf[i].k, key) >= 0 })
		if i < len(it.buf) {
			it.i = i
			return true
		}
		if it.end == nil {
			it.buf, it.done = nil, true
			return false
		}
		key = it.end
	}
	it.done = false
	return it.load(key)
}

func (it *distIter) Next() bool {
	if it.buf == nil {
		return false
	}
	it.i++
	if it.i < len(it.buf) {
		return true
	}
	if it.end == nil {
		it.buf, it.done = nil, true
		return false
	}
	return it.load(it.end)
}

func (it *distIter) Valid() bool   { return it.buf != nil && it.i < len(it.buf) && it.err == nil }
func (it *distIter) Key() []byte   { return it.buf[it.i].k }
func (it *distIter) Value() []byte { return it.buf[it.i].v }
func (it *distIter) Error() error  { return it.err }
func (it *distIter) Close() error  { return nil }

// ---- protocol messages between nodes ----

var fwdCodec = frame.NewCodec()

func encodeMessage(m message.Message) ([]byte, error) {
	var buf bytes.Buffer
	if err := fwdCodec.EncodeFrame(frame.NewFrame(primitive.ProtocolVersion4, 0, m), &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeMessage(b []byte) (message.Message, error) {
	f, err := fwdCodec.DecodeFrame(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	return f.Body.Message, nil
}

func (m *modification) isCounter() bool {
	return slices.ContainsFunc(m.ops, func(o setOp) bool { return o.kind == opCounter })
}

// ---- counters (CounterMutation) ----

// counterLeader is the node that applies a partition's counter updates:
// its first live replica (StorageProxy.findSuitableReplica).
func (p *proxy) counterLeader(k *Keyspace, token int64) (*cluster.Node, error) {
	reps, live := p.replicas(k, token, primitive.ConsistencyLevelAll)
	if live == 0 {
		return nil, &unavailableError{cl: primitive.ConsistencyLevelOne, required: 1, alive: 0}
	}
	for _, r := range reps[:live] {
		if r.Addr == p.self {
			return r, nil
		}
	}
	return reps[0], nil
}

// applyCounters sends counter deltas to each partition's leader, which
// turns them into its shard and replicates them.
func (p *proxy) applyCounters(m *mutation, cl primitive.ConsistencyLevel) error {
	byLeader := map[string][]write{}
	for _, w := range m.writes {
		k := p.keyspaceOf(w.table)
		if k == nil {
			return invalidf("a table was dropped while it was written")
		}
		l, err := p.counterLeader(k, w.token)
		if err != nil {
			return err
		}
		byLeader[l.Addr] = append(byLeader[l.Addr], w)
	}
	errs := make(chan error, len(byLeader))
	for addr, ws := range byLeader {
		go func() {
			if addr == p.self {
				errs <- p.d.applyCounterDeltas(ws, cl)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), p.opts.WriteTimeout*2)
			defer cancel()
			b, _ := json.Marshal(counterRequest{CL: uint16(cl), Writes: encodeWrites(ws)})
			resp, err := p.m.Request(ctx, addr, cluster.VerbCounterMutation, b)
			if err == nil && len(resp) > 0 {
				m, derr := decodeMessage(resp)
				if derr == nil {
					if e, ok := m.(message.Error); ok {
						err = fmt.Errorf("%s", e.GetErrorMessage())
						if wt, ok := m.(*message.WriteTimeout); ok {
							err = &timeoutError{write: true, cl: wt.Consistency, received: int(wt.Received), blockFor: int(wt.BlockFor)}
						} else if u, ok := m.(*message.Unavailable); ok {
							err = &unavailableError{cl: u.Consistency, required: int(u.Required), alive: int(u.Alive)}
						}
					}
				}
			} else if err != nil {
				err = &timeoutError{write: true, cl: cl, blockFor: 1}
			}
			errs <- err
		}()
	}
	var first error
	for range byLeader {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	return first
}

type counterRequest struct {
	CL     uint16
	Writes []byte
}

func (p *proxy) handleCounterMutation(ctx context.Context, _ string, payload []byte) ([]byte, error) {
	var r counterRequest
	if err := json.Unmarshal(payload, &r); err != nil {
		return nil, err
	}
	ws, err := decodeWrites(r.Writes)
	if err != nil {
		return nil, err
	}
	if err := p.d.applyCounterDeltas(ws, primitive.ConsistencyLevel(r.CL)); err != nil {
		return encodeMessage(ErrorMessage(err))
	}
	return nil, nil
}

// ---- system.peers ----

func (p *proxy) peers() []cluster.Node {
	var out []cluster.Node
	for _, n := range p.d.schema.Load().Nodes {
		if n.Addr != p.self && n.Status == cluster.StatusNormal {
			out = append(out, *n)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Addr < out[j].Addr })
	return out
}

func ipBytes(addr string) []byte {
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil
	}
	if v4 := ip.To4(); v4 != nil {
		return v4
	}
	return ip.To16()
}

func tokenStrings(ts []int64) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = strconv.FormatInt(t, 10)
	}
	return out
}

// tokenSegments are the ring's ranges as (lo, hi] in token order, the
// range that wraps around split in two.
func (p *proxy) tokenSegments() [][2]int64 {
	var out [][2]int64
	for _, r := range p.ring.Load().Ranges() {
		if r[0] < r[1] {
			out = append(out, r)
			continue
		}
		out = append(out, [2]int64{math.MinInt64, r[1]})
		if r[0] != math.MaxInt64 {
			out = append(out, [2]int64{r[0], math.MaxInt64})
		}
	}
	slices.SortFunc(out, func(a, b [2]int64) int { return cmp.Compare(a[1], b[1]) })
	if len(out) == 0 {
		out = [][2]int64{{math.MinInt64, math.MaxInt64}}
	}
	return out
}

// internalAddr is the address recorded for nodes of this data center to
// connect to (prefer_local): the listen address, when it differs from the
// broadcast address and names a host.
func (p *proxy) internalAddr() string {
	l := p.opts.ListenAddress
	if l == p.self {
		return ""
	}
	if host, _, err := net.SplitHostPort(l); err != nil || host == "" || net.ParseIP(host) != nil && net.ParseIP(host).IsUnspecified() {
		return ""
	}
	return l
}

// dialAddress is where to connect to a node known by addr: its internal
// address if it is in this data center and has one (prefer_local).
func (p *proxy) dialAddress(addr string) string {
	for _, n := range p.d.schema.Load().Nodes {
		if n.Addr == addr {
			if n.InternalAddr != "" && n.DataCenter == p.d.opts.DataCenter {
				return n.InternalAddr
			}
			break
		}
	}
	return addr
}
