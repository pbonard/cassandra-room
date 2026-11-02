package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/cluster"
	"github.com/cassandra-room/cassandra-room/internal/storage/lsm"
	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/cassandra-room/cassandra-room/internal/types"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// ClusterOptions make a DB a node of a cluster.
type ClusterOptions struct {
	// ListenAddress is the internode address, "host:port" (storage_port).
	ListenAddress string
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
	d      *DB
	m      *cluster.Messaging
	g      *cluster.Gossiper
	cms    *cluster.CMS
	lat    readLatency
	// initialCMS are the metadata service's members before the metadata
	// records any.
	initialCMS []string
	// replaced holds the host ids of nodes a joining node replaces: they
	// count as down.
	replaced atomic.Pointer[map[string]bool]
	opts   ClusterOptions
	ring   atomic.Pointer[cluster.Ring]
	self   string
	stopCh chan struct{}
	wg     sync.WaitGroup
	hintMu sync.Mutex
	seq    atomic.Uint64
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
	if _, err := d.engine.Table(hintsStorage); errors.Is(err, lsm.ErrUnknownTable) {
		if _, err := d.engine.CreateTable(hintsStorage); err != nil {
			return err
		}
	}
	if len(d.local.Tokens) == 0 {
		d.local.Tokens = cluster.RandomTokens(o.NumTokens)
		d.ddlMu.Lock()
		err := d.saveSchema(d.schema.Load())
		d.ddlMu.Unlock()
		if err != nil {
			return err
		}
	}
	m, err := cluster.NewMessaging(o.ListenAddress)
	if err != nil {
		return err
	}
	p := &proxy{d: d, m: m, opts: *o, self: m.Addr(), stopCh: make(chan struct{})}
	m.Handle(cluster.VerbMutation, p.handleMutation)
	m.Handle(cluster.VerbReadRow, p.handleReadRow)
	m.Handle(cluster.VerbReadDigest, p.handleReadDigest)
	m.Handle(cluster.VerbReadRange, p.handleReadRange)
	m.Handle(cluster.VerbExecute, p.handleExecute)
	m.Handle(cluster.VerbRepair, p.handleRepair)
	m.Handle(cluster.VerbStreamPush, p.handleStreamPush)
	m.Handle(cluster.VerbCounterMutation, p.handleCounterMutation)
	for _, v := range []cluster.Verb{cluster.VerbLWTPrepare, cluster.VerbLWTPropose, cluster.VerbLWTCommit} {
		m.Handle(v, p.handlePaxos(v))
	}
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
	p.cms = cluster.NewCMS(m, p.cmsMembers, d.currentMetadata, d.installBytes, metadataInfo)
	saved, err := d.readFile(cmsStateFileName)
	if err != nil {
		m.Close()
		return err
	}
	if err := p.cms.SetPersistence(saved, func(b []byte) error { return d.writeFileAtomic(cmsStateFileName, b) }); err != nil {
		m.Close()
		return err
	}
	p.g.OnChange(p.membershipChanged)
	p.g.OnLiveness(func(addr string, up bool) {
		if up {
			go p.replayHints()
		}
	})
	d.proxy = p
	d.store = p
	p.ring.Store(cluster.NewRing(nodeList(d.schema.Load())))
	m.Start()
	p.g.Start()
	for _, s := range o.Seeds {
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

// up reports whether a node is up: alive, and not being replaced.
func (p *proxy) up(n *cluster.Node) bool {
	if r := p.replaced.Load(); r != nil && (*r)[n.HostID] {
		return false
	}
	return n.Addr == p.self || p.g.IsAlive(n.Addr)
}

// cmsMembers returns the metadata service's members.
func (p *proxy) cmsMembers() []string {
	if m := p.d.schema.Load().CMS; len(m) > 0 {
		return m
	}
	return p.initialCMS
}

// join registers this node in the cluster metadata and, if it joins a
// cluster with data, bootstraps it: PrepareJoin, streaming, FinishJoin.
// With ReplaceAddress it takes over a dead node's tokens instead
// (PrepareReplace, streaming, FinishReplace).
func (p *proxy) join() error {
	d := p.d
	me := &cluster.Node{Addr: p.self, HostID: d.local.HostID, NativeAddr: d.opts.Address.String(), NativePort: d.opts.Port,
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
		if cur != nil && cur.Status != cluster.StatusLeft && cur.Addr == me.Addr && cur.NativeAddr == me.NativeAddr && cur.NativePort == me.NativePort {
			return nil, false, nil
		}
		n := *me
		n.Status = cluster.StatusNormal
		for _, o := range next.Nodes {
			if o.HostID != me.HostID && o.Status == cluster.StatusNormal {
				n.Status = cluster.StatusJoining // there is data to stream
				break
			}
		}
		if cur != nil && cur.Status != cluster.StatusLeft {
			n.Status = cur.Status // an address change
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
	return p.setNodeStatus("FINISH_JOIN", me.HostID, cluster.StatusNormal)
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
	return err
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

// disseminate pushes the current metadata to the live nodes after a
// commit (TCM's log replication).
func (p *proxy) disseminate() {
	var nodes []string
	for _, n := range p.d.schema.Load().Nodes {
		if n.Addr != p.self && n.Status != cluster.StatusLeft && p.g.IsAlive(n.Addr) {
			nodes = append(nodes, n.Addr)
		}
	}
	_, b := p.d.currentMetadata()
	p.cms.Push(nodes, b)
}

func (p *proxy) stop() {
	close(p.stopCh)
	p.wg.Wait()
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
	n := ackNeed{total: blockFor(cl, k, p.d.opts.DataCenter)}
	for _, r := range pending {
		local := cl == primitive.ConsistencyLevelLocalQuorum || cl == primitive.ConsistencyLevelLocalOne || cl == primitive.ConsistencyLevelLocalSerial
		if cl != primitive.ConsistencyLevelAny && (!local || r.DataCenter == p.d.opts.DataCenter) {
			n.total++
		}
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
	for _, k := range s.Keyspaces {
		for _, t := range k.Tables {
			for _, ix := range t.Indexes {
				if ix.Storage == id {
					return k
				}
			}
		}
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

func (p *proxy) handleMutation(_ string, payload []byte) ([]byte, error) {
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
			reps, _ := p.replicas(k, w.token, cl)
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
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.WriteTimeout)
	defer cancel()
	type ack struct {
		addr string
		err  error
	}
	acks := make(chan ack, len(perNode))
	for addr, ws := range perNode {
		go func() {
			var err error
			if addr == p.self {
				err = p.d.applyLocal(ws)
			} else {
				_, err = p.m.Request(ctx, addr, cluster.VerbMutation, encodeWrites(ws))
			}
			acks <- ack{addr, err}
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
	for len(pending) > 0 && !satisfied() {
		select {
		case a := <-acks:
			delete(pending, a.addr)
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
			pending = nil
		}
	}
	// Replicas that have not answered yet get hints only if they fail;
	// let them finish in the background.
	if n := len(pending); n > 0 {
		go func() {
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
	}
	p.storeHints(hints)
	if !satisfied() {
		if cl == primitive.ConsistencyLevelAny && len(hints) > 0 {
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

// ---- hints (HintsService) ----

type hint struct {
	hostID string
	w      write
}

func (p *proxy) storeHints(hs []hint) {
	if len(hs) == 0 {
		return
	}
	by := map[string][]write{}
	for _, h := range hs {
		by[h.hostID] = append(by[h.hostID], h.w)
	}
	var ws []write
	now := time.Now().UnixMicro()
	for host, writes := range by {
		id, err := types.ParseUUID(host)
		if err != nil {
			continue
		}
		key := append(id[:], binary.BigEndian.AppendUint64(nil, uint64(now))...)
		key = binary.BigEndian.AppendUint64(key, p.seq.Add(1))
		v := row.Append(nil, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp},
			Cells: []row.Cell{{Column: 1, Timestamp: now, LocalDeletionTime: row.NoDeletionTime, Value: encodeWrites(writes)}}})
		ws = append(ws, write{table: hintsStorage, key: key, value: v})
	}
	_ = p.d.applyLocal(ws)
}

// replayHints sends stored hints to the live nodes they are for, deleting
// the delivered ones.
func (p *proxy) replayHints() {
	if !p.hintMu.TryLock() {
		return
	}
	defer p.hintMu.Unlock()
	lt, err := p.d.engine.Table(hintsStorage)
	if err != nil {
		return
	}
	byHost := map[string]*cluster.Node{}
	for _, n := range p.d.schema.Load().Nodes {
		if n.Addr != p.self && p.g.IsAlive(n.Addr) {
			byHost[n.HostID] = n
		}
	}
	for host, node := range byHost {
		id, err := types.ParseUUID(host)
		if err != nil {
			continue
		}
		it, err := lt.NewIter(id[:], prefixEnd(id[:]))
		if err != nil {
			continue
		}
		var done []write
		for ok := it.First(); ok; ok = it.Next() {
			r, err := row.Decode(it.Value())
			if err != nil || len(r.Cells) == 0 {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), p.opts.WriteTimeout)
			_, err = p.m.Request(ctx, node.Addr, cluster.VerbMutation, r.Cells[0].Value)
			cancel()
			if err != nil {
				break
			}
			now := time.Now()
			done = append(done, write{table: hintsStorage, key: slices.Clone(it.Key()),
				value: row.Append(nil, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: now.UnixMicro(), LocalDeletionTime: now.Unix()}})})
		}
		it.Close()
		_ = p.d.applyLocal(done)
	}
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
	p  *proxy
	k  *Keyspace
	id lsm.TableID
	cl primitive.ConsistencyLevel
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

func (p *proxy) handleReadRow(_ string, payload []byte) ([]byte, error) {
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

// readLatency tracks replica read latencies for speculative retry.
type readLatency struct {
	mu      sync.Mutex
	samples [256]time.Duration
	n       int
	p99     atomic.Int64
}

func (l *readLatency) add(d time.Duration) {
	l.mu.Lock()
	l.samples[l.n%len(l.samples)] = d
	l.n++
	if l.n%32 == 0 {
		c := slices.Clone(l.samples[:min(l.n, len(l.samples))])
		slices.Sort(c)
		l.p99.Store(int64(c[len(c)*99/100]))
	}
	l.mu.Unlock()
}

// threshold is when to speculate: the 99th percentile of replica reads
// (speculative_retry = 99p), 50ms until enough reads were seen.
func (l *readLatency) threshold(timeout time.Duration) time.Duration {
	t := time.Duration(l.p99.Load())
	if t <= 0 {
		t = 50 * time.Millisecond
	}
	return min(max(t, time.Millisecond), timeout/2)
}

func rowDigest(found bool, v []byte) []byte {
	if !found {
		return []byte{0}
	}
	h := sha256.Sum256(v)
	return append([]byte{1}, h[:16]...)
}

func (p *proxy) handleReadDigest(from string, payload []byte) ([]byte, error) {
	b, err := p.handleReadRow(from, payload)
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
		s.p.lat.add(time.Since(start))
		if data {
			ch <- resp{node: r, data: true, found: b[0] == 1, v: b[1:], digest: rowDigest(b[0] == 1, b[1:])}
		} else {
			ch <- resp{node: r, digest: b}
		}
	}
	initial := need.count()
	go ask(reps[0], true)
	for _, r := range reps[1:initial] {
		go ask(r, false)
	}
	next, outstanding := initial, initial
	var got []resp
	var answered acks
	haveData := false
	speculate := time.NewTimer(s.p.lat.threshold(s.p.opts.ReadTimeout))
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
	if len(stale) > 0 {
		s.p.repair(stale, []write{{table: s.id, key: key, value: merged, token: tokenOf(key)}})
	}
	return merged, nil
}

// repair writes merged rows to replicas that were behind (ReadRepair).
func (p *proxy) repair(addrs []string, ws []write) {
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.WriteTimeout)
	defer cancel()
	var wg sync.WaitGroup
	for _, a := range addrs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if a == p.self {
				_ = p.d.applyLocal(ws)
				return
			}
			_, _ = p.m.Request(ctx, a, cluster.VerbMutation, encodeWrites(ws))
		}()
	}
	wg.Wait()
}

const rangePage = 512

func (p *proxy) handleReadRange(_ string, payload []byte) ([]byte, error) {
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
		out = binary.AppendUvarint(out, uint64(len(it.Key())))
		out = append(out, it.Key()...)
		out = binary.AppendUvarint(out, uint64(len(it.Value())))
		out = append(out, it.Value()...)
		n++
	}
	return append([]byte{more}, out...), it.Error()
}

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
		kvs, cutoff, err := it.fetch(reps, need.count(), from, segEnd)
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
func (it *distIter) fetch(reps []*cluster.Node, need int, from, to []byte) ([]kv, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), it.s.p.opts.ReadTimeout)
	defer cancel()
	type resp struct {
		kvs  []kv
		more bool
		err  error
	}
	ch := make(chan resp, len(reps))
	ask := func(r *cluster.Node) {
		b, err := it.s.p.m.Request(ctx, r.Addr, cluster.VerbReadRange, encodeKey(it.s.id, from, to))
		if err != nil {
			ch <- resp{err: err}
			return
		}
		kvs, more, err := decodeKVs(b)
		ch <- resp{kvs: kvs, more: more, err: err}
	}
	for _, r := range reps[:need] {
		go ask(r)
	}
	next := need
	var got []resp
	for len(got) < need {
		select {
		case r := <-ch:
			if r.err != nil {
				if next < len(reps) {
					go ask(reps[next])
					next++
					continue
				}
				return nil, nil, &timeoutError{cl: it.s.cl, received: len(got), blockFor: need}
			}
			got = append(got, r)
		case <-ctx.Done():
			return nil, nil, &timeoutError{cl: it.s.cl, received: len(got), blockFor: need}
		}
	}
	var cutoff []byte
	for _, r := range got {
		if r.more && len(r.kvs) > 0 {
			last := r.kvs[len(r.kvs)-1].k
			if cutoff == nil || bytes.Compare(last, cutoff) < 0 {
				cutoff = last
			}
		}
	}
	merged := map[string][]byte{}
	for _, r := range got {
		for _, e := range r.kvs {
			if cutoff != nil && bytes.Compare(e.k, cutoff) > 0 {
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
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].k, out[j].k) < 0 })
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

// ---- forwarded statements ----

// forwarded is a statement run on another node (index queries scattered
// to every node).
type forwarded struct {
	Keyspace  string
	Query     string
	Values    [][]byte
	Null      []bool
	Unset     []bool
	CL        uint16
	Serial    uint16 `json:",omitempty"`
	Timestamp *int64
	NowInSec  *int32
	User      string
	Local     bool             // read only this node's data (index scatter)
	Batch     []forwardedChild `json:",omitempty"`
	BatchType uint8
}

type forwardedChild struct {
	Keyspace, Query string
	Values          [][]byte
	Null, Unset     []bool
}

func packValues(vs []*primitive.Value) (vals [][]byte, null, unset []bool) {
	for _, v := range vs {
		vals = append(vals, v.Contents)
		null = append(null, v.Type == primitive.ValueTypeNull)
		unset = append(unset, v.Type == primitive.ValueTypeUnset)
	}
	return
}

func unpackValues(vals [][]byte, null, unset []bool) []*primitive.Value {
	out := make([]*primitive.Value, len(vals))
	for i := range vals {
		switch {
		case null[i]:
			out[i] = &primitive.Value{Type: primitive.ValueTypeNull}
		case unset[i]:
			out[i] = &primitive.Value{Type: primitive.ValueTypeUnset}
		default:
			out[i] = &primitive.Value{Type: primitive.ValueTypeRegular, Contents: vals[i]}
		}
	}
	return out
}

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

// forward runs a statement on another node and returns its response.
func (p *proxy) forward(to string, f *forwarded) message.Message {
	b, _ := json.Marshal(f)
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.WriteTimeout+p.opts.ReadTimeout)
	defer cancel()
	resp, err := p.m.Request(ctx, to, cluster.VerbExecute, b)
	if err != nil {
		return &message.WriteTimeout{ErrorMessage: "forwarded request failed: " + err.Error(), Consistency: primitive.ConsistencyLevel(f.CL), WriteType: primitive.WriteTypeSimple}
	}
	m, err := decodeMessage(resp)
	if err != nil {
		return &message.ServerError{ErrorMessage: err.Error()}
	}
	return m
}

func (p *proxy) handleExecute(_ string, payload []byte) ([]byte, error) {
	var f forwarded
	if err := json.Unmarshal(payload, &f); err != nil {
		return nil, err
	}
	o := &RequestOptions{Consistency: primitive.ConsistencyLevel(f.CL), SerialConsistency: primitive.ConsistencyLevel(f.Serial), Timestamp: f.Timestamp, NowInSec: f.NowInSec,
		User: f.User, Version: primitive.ProtocolVersion4, forwarded: true, local: f.Local}
	var res message.Message
	if f.Batch != nil {
		children := make([]BatchChild, len(f.Batch))
		for i, c := range f.Batch {
			pr, err := p.d.Prepare(c.Keyspace, c.Query)
			if err != nil {
				return encodeMessage(ErrorMessage(err))
			}
			children[i] = BatchChild{Prepared: pr, Values: unpackValues(c.Values, c.Null, c.Unset)}
		}
		res = p.d.ExecuteBatch(primitive.BatchType(f.BatchType), children, o)
	} else {
		pr, err := p.d.Prepare(f.Keyspace, f.Query)
		if err != nil {
			return encodeMessage(ErrorMessage(err))
		}
		o.Values = unpackValues(f.Values, f.Null, f.Unset)
		res = p.d.Execute(pr, o)
	}
	return encodeMessage(res)
}

// route decides where a statement runs: "" to run here. (Every statement
// runs on its coordinator now: conditional updates run Paxos, counter
// updates send deltas to their partitions' leaders.)
func (p *proxy) route(st *prepState, e *execCtx) string { return "" }

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

func (p *proxy) handleCounterMutation(_ string, payload []byte) ([]byte, error) {
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

// scatter runs an index query on every live node, each reading its local
// index for the partitions it is the first live replica of, and merges
// the rows (secondary indexes are local to each node, as in Cassandra).
func (p *proxy) scatter(pr *Prepared, e *execCtx, o *RequestOptions) message.Message {
	vals, null, unset := packValues(e.values)
	f := &forwarded{Keyspace: pr.Keyspace, Query: pr.Query, Values: vals, Null: null, Unset: unset,
		CL: uint16(primitive.ConsistencyLevelOne), Timestamp: o.Timestamp, NowInSec: o.NowInSec, User: o.User, Local: true}
	var nodes []string
	for _, n := range p.d.schema.Load().Nodes {
		if n.Status == cluster.StatusNormal && (n.Addr == p.self || p.g.IsAlive(n.Addr)) {
			nodes = append(nodes, n.Addr)
		}
	}
	results := make([]message.Message, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = p.forward(n, f)
		}()
	}
	wg.Wait()
	var out *message.RowsResult
	for _, r := range results {
		rows, ok := r.(*message.RowsResult)
		if !ok {
			return r
		}
		if out == nil {
			out = &message.RowsResult{Metadata: rows.Metadata, Data: message.RowSet{}}
		}
		out.Data = append(out.Data, rows.Data...)
	}
	st := pr.state.Load()
	if sel, ok := st.stmt.(*selectStmt); ok {
		if limit, ok, _ := bindInt(e, sel.limit, "limit"); ok && int64(len(out.Data)) > limit {
			out.Data = out.Data[:limit]
		}
	}
	out.Metadata.PagingState = nil
	return out
}

// ownsPrimary reports whether this node is the first live replica of a
// partition (index scatter: each partition is read by one node).
func (p *proxy) ownsPrimary(ks string, token int64) bool {
	k := p.d.schema.Load().Keyspaces[ks]
	if k == nil {
		return true
	}
	for _, r := range p.ring.Load().Replicas(k.Replication, token) {
		if p.up(r) {
			return r.Addr == p.self
		}
	}
	return false
}
