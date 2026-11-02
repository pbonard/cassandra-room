package db

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/maphash"
	mrand "math/rand"
	"sync"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/cluster"
	"github.com/cassandra-room/cassandra-room/internal/storage/lsm"
	"github.com/cassandra-room/cassandra-room/internal/storage/row"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// Lightweight transactions in a cluster: single-decree Paxos per partition
// among its replicas (Cassandra's Paxos v1, StorageProxy.cas, as ScyllaDB's
// service/paxos keeps it).
//
// A coordinator runs begin-and-repair: it prepares a ballot on the
// replicas, finishes any round a previous coordinator left accepted but
// not committed, and makes sure a quorum has learnt the most recent
// commit. Then it reads the partition at quorum, checks the conditions,
// proposes the update (its cells timestamped with the ballot, so commits
// apply in ballot order), commits it at the statement's consistency level
// and, once every replica has learnt it, prunes the Paxos state.
//
// Each replica keeps per-partition state (system.paxos) in an engine
// table: the promised ballot, the accepted proposal and the most recent
// commit, each a cell timestamped with its ballot, so out-of-order
// messages reconcile like any other write. Promises and acceptances are
// synced to the commit log before they are answered.

// paxosStorage is the engine table holding Paxos state (system.paxos).
const paxosStorage lsm.TableID = 1<<40 + 1

const (
	paxosPromise  = 0
	paxosProposal = 1
	paxosCommit   = 2
)

// ballot is a time-based unique ballot (a timeuuid): microseconds, then
// random bits to break ties.
type ballot [16]byte

func (b ballot) micros() int64 { return int64(binary.BigEndian.Uint64(b[:8])) }
func (b ballot) isZero() bool  { return b == ballot{} }
func (b ballot) cmp(o ballot) int {
	return bytes.Compare(b[:], o[:])
}

func newBallot(micros int64) ballot {
	var b ballot
	binary.BigEndian.PutUint64(b[:8], uint64(micros))
	_, _ = rand.Read(b[8:])
	return b
}

// proposal is a ballot and the update (writes) it carries.
type proposal struct {
	Ballot ballot
	Update []byte // encodeWrites
}

func (p *proposal) encode() []byte {
	return append(p.Ballot[:len(p.Ballot):len(p.Ballot)], p.Update...)
}

func decodeProposal(v []byte) (*proposal, error) {
	if len(v) < 16 {
		return nil, errors.New("db: corrupt paxos proposal")
	}
	p := &proposal{Update: v[16:]}
	copy(p.Ballot[:], v[:16])
	return p, nil
}

// paxosState is a replica's state for one partition.
type paxosState struct {
	promised ballot
	accepted *proposal
	commit   *proposal
}

var paxosLocks [256]sync.Mutex
var paxosSeed = maphash.MakeSeed()

func paxosKey(id lsm.TableID, pk []byte) []byte {
	return append(binary.BigEndian.AppendUint64(nil, uint64(id)), pk...)
}

func (d *DB) paxosTable() (*lsm.Table, error) {
	t, err := d.engine.Table(paxosStorage)
	if errors.Is(err, lsm.ErrUnknownTable) {
		return d.engine.CreateTable(paxosStorage)
	}
	return t, err
}

func (d *DB) loadPaxos(key []byte, nowSec int64) (*paxosState, error) {
	t, err := d.paxosTable()
	if err != nil {
		return nil, err
	}
	v, err := t.Get(key)
	st := &paxosState{}
	if errors.Is(err, lsm.ErrNotFound) {
		return st, nil
	}
	if err != nil {
		return nil, err
	}
	r, err := row.Decode(v)
	if err != nil {
		return nil, err
	}
	for _, c := range r.Live(nowSec) {
		switch c.Column {
		case paxosPromise:
			copy(st.promised[:], c.Value)
		case paxosProposal:
			if st.accepted, err = decodeProposal(c.Value); err != nil {
				return nil, err
			}
		case paxosCommit:
			if st.commit, err = decodeProposal(c.Value); err != nil {
				return nil, err
			}
		}
	}
	return st, nil
}

// savePaxos writes Paxos state cells, synced.
func (d *DB) savePaxos(key []byte, r *row.Row) error {
	if _, err := d.paxosTable(); err != nil {
		return err
	}
	b := lsm.NewBatch()
	b.Put(paxosStorage, key, row.Append(nil, r))
	d.tableMu.RLock()
	defer d.tableMu.RUnlock()
	return d.engine.Apply(b, lsm.Sync)
}

// paxosTTL keeps Paxos state for the table's gc_grace_seconds: a replica
// down for longer must be repaired anyway.
func (d *DB) paxosTTL(id lsm.TableID) uint32 {
	if t := d.schema.Load().byStorage[id]; t != nil && t.Params.GCGraceSeconds > 0 {
		return uint32(t.Params.GCGraceSeconds)
	}
	return 3 * 3600
}

func liveCell(col uint32, ts int64, ttl uint32, nowSec int64, v []byte) row.Cell {
	c := row.Cell{Column: col, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime, Value: v}
	if ttl > 0 {
		c.TTL, c.LocalDeletionTime = ttl, nowSec+int64(ttl)
	}
	return c
}

// ---- replica side (PaxosState) ----

type paxosRequest struct {
	Table    uint64
	Key      []byte
	Ballot   ballot
	Update   []byte `json:",omitempty"`
	Prune    bool   `json:",omitempty"`
	Response bool   `json:",omitempty"`
}

type paxosResponse struct {
	OK       bool
	Promised ballot
	Accepted *proposal `json:",omitempty"`
	Commit   *proposal `json:",omitempty"`
}

func (d *DB) paxosLock(key []byte) *sync.Mutex {
	return &paxosLocks[maphash.Bytes(paxosSeed, key)%uint64(len(paxosLocks))]
}

// paxosPrepare promises a ballot newer than any promised before, and
// returns the accepted proposal and the most recent commit.
func (d *DB) paxosPrepare(r *paxosRequest) (*paxosResponse, error) {
	key := paxosKey(lsm.TableID(r.Table), r.Key)
	mu := d.paxosLock(key)
	mu.Lock()
	defer mu.Unlock()
	// "now" is the ballot's time on every replica, so a commit expires
	// alike everywhere (CASSANDRA-12043).
	nowSec := r.Ballot.micros() / 1e6
	st, err := d.loadPaxos(key, nowSec)
	if err != nil {
		return nil, err
	}
	if r.Ballot.cmp(st.promised) <= 0 {
		return &paxosResponse{Promised: st.promised}, nil
	}
	ttl := d.paxosTTL(lsm.TableID(r.Table))
	if err := d.savePaxos(key, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp},
		Cells: []row.Cell{liveCell(paxosPromise, r.Ballot.micros(), ttl, time.Now().Unix(), r.Ballot[:])}}); err != nil {
		return nil, err
	}
	return &paxosResponse{OK: true, Promised: r.Ballot, Accepted: st.accepted, Commit: st.commit}, nil
}

// paxosAccept accepts a proposal unless a newer ballot was promised.
func (d *DB) paxosAccept(r *paxosRequest) (*paxosResponse, error) {
	key := paxosKey(lsm.TableID(r.Table), r.Key)
	mu := d.paxosLock(key)
	mu.Lock()
	defer mu.Unlock()
	st, err := d.loadPaxos(key, r.Ballot.micros()/1e6)
	if err != nil {
		return nil, err
	}
	if r.Ballot.cmp(st.promised) < 0 {
		return &paxosResponse{Promised: st.promised}, nil
	}
	ttl := d.paxosTTL(lsm.TableID(r.Table))
	p := &proposal{Ballot: r.Ballot, Update: r.Update}
	if err := d.savePaxos(key, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp}, Cells: []row.Cell{
		liveCell(paxosPromise, r.Ballot.micros(), ttl, time.Now().Unix(), r.Ballot[:]),
		liveCell(paxosProposal, r.Ballot.micros(), ttl, time.Now().Unix(), p.encode()),
	}}); err != nil {
		return nil, err
	}
	return &paxosResponse{OK: true, Promised: r.Ballot}, nil
}

// paxosCommitLocal learns a decision: applies its update, records it as
// the most recent commit and drops the accepted proposal (whose cell the
// deletion only shadows if it is not newer). With Prune, it deletes the
// proposal and commit of a decision every replica has learnt; the promise
// stays, so a proposer with an older ballot is still refused.
func (d *DB) paxosCommitLocal(r *paxosRequest) error {
	key := paxosKey(lsm.TableID(r.Table), r.Key)
	ts, nowSec := r.Ballot.micros(), time.Now().Unix()
	if r.Prune {
		return d.savePaxos(key, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp}, Cells: []row.Cell{
			{Column: paxosProposal, Timestamp: ts, LocalDeletionTime: nowSec},
			{Column: paxosCommit, Timestamp: ts, LocalDeletionTime: nowSec},
		}})
	}
	ws, err := decodeWrites(r.Update)
	if err != nil {
		return err
	}
	if len(ws) > 0 {
		b := lsm.NewBatch()
		for _, w := range ws {
			b.Put(w.table, w.key, w.value)
		}
		d.tableMu.RLock()
		err := d.engine.Apply(b, lsm.Sync)
		d.tableMu.RUnlock()
		if err != nil {
			return err
		}
		d.updateVectorIndexes(ws)
	}
	ttl := d.paxosTTL(lsm.TableID(r.Table))
	p := &proposal{Ballot: r.Ballot, Update: r.Update}
	return d.savePaxos(key, &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp}, Cells: []row.Cell{
		{Column: paxosProposal, Timestamp: ts, LocalDeletionTime: nowSec},
		liveCell(paxosCommit, ts, ttl, nowSec, p.encode()),
	}})
}

func (p *proxy) handlePaxos(verb cluster.Verb) cluster.Handler {
	return func(_ string, payload []byte) ([]byte, error) {
		var r paxosRequest
		if err := json.Unmarshal(payload, &r); err != nil {
			return nil, err
		}
		var resp *paxosResponse
		var err error
		switch verb {
		case cluster.VerbLWTPrepare:
			resp, err = p.d.paxosPrepare(&r)
		case cluster.VerbLWTPropose:
			resp, err = p.d.paxosAccept(&r)
		case cluster.VerbLWTCommit:
			err = p.d.paxosCommitLocal(&r)
			resp = &paxosResponse{OK: err == nil}
		}
		if err != nil {
			return nil, err
		}
		return json.Marshal(resp)
	}
}

// ---- coordinator side (StorageProxy.cas) ----

// paxosRound is one CAS operation on a partition.
type paxosRound struct {
	p        *proxy
	table    lsm.TableID
	pk       []byte
	token    int64
	serial   primitive.ConsistencyLevel
	commitCL primitive.ConsistencyLevel
	k        *Keyspace
	// participants are the live natural and pending replicas; required is
	// a quorum of the natural ones plus the pending ones.
	participants []*cluster.Node
	required     int
	deadline     time.Time
	contentions  int
}

// casTimeout bounds a CAS operation's retries (cas_contention_timeout).
const casTimeout = 2 * time.Second

func (p *proxy) newPaxosRound(t *Table, pk []byte, serial, commitCL primitive.ConsistencyLevel) (*paxosRound, error) {
	k := p.d.schema.Load().Keyspaces[t.Keyspace]
	if k == nil {
		return nil, invalidf("Keyspace %s does not exist", t.Keyspace)
	}
	if serial != primitive.ConsistencyLevelLocalSerial {
		serial = primitive.ConsistencyLevelSerial
	}
	r := &paxosRound{p: p, table: t.Storage, pk: pk, token: Token(pk), serial: serial, commitCL: commitCL, k: k,
		deadline: time.Now().Add(max(casTimeout, p.opts.WriteTimeout))}
	reps, live := p.replicas(k, r.token, serial)
	natural := len(reps)
	r.participants = reps[:live]
	pending := 0
	for _, n := range p.pendingReplicas(k, r.token, reps) {
		if serial == primitive.ConsistencyLevelLocalSerial && n.DataCenter != p.d.opts.DataCenter {
			continue
		}
		pending++
		if p.g.IsAlive(n.Addr) {
			r.participants = append(r.participants, n)
		}
	}
	r.required = natural/2 + 1 + pending
	if len(r.participants) < r.required {
		return nil, &unavailableError{cl: serial, required: r.required, alive: len(r.participants)}
	}
	return r, nil
}

func (r *paxosRound) timeout(received int) error {
	return &timeoutError{write: true, cas: true, cl: r.serial, received: received, blockFor: r.required}
}

type paxosReply struct {
	addr string
	resp *paxosResponse
	err  error
}

// send sends a Paxos request to nodes and collects the answers until need
// succeeded ones (or all) arrived; the others finish in the background.
func (r *paxosRound) send(verb cluster.Verb, nodes []*cluster.Node, req *paxosRequest, need int) []paxosReply {
	b, _ := json.Marshal(req)
	ctx, cancel := context.WithTimeout(context.Background(), r.p.opts.WriteTimeout)
	time.AfterFunc(r.p.opts.WriteTimeout, cancel) // the stragglers finish in the background
	ch := make(chan paxosReply, len(nodes))
	for _, n := range nodes {
		go func() {
			var out []byte
			var err error
			if n.Addr == r.p.self {
				out, err = r.p.handlePaxos(verb)("", b)
			} else {
				out, err = r.p.m.Request(ctx, n.Addr, verb, b)
			}
			rep := paxosReply{addr: n.Addr, err: err}
			if err == nil {
				rep.resp = &paxosResponse{}
				rep.err = json.Unmarshal(out, rep.resp)
			}
			ch <- rep
		}()
	}
	var out []paxosReply
	ok := 0
	for range nodes {
		rep := <-ch
		out = append(out, rep)
		if rep.err == nil && rep.resp.OK {
			ok++
		}
		if ok >= need {
			break
		}
	}
	return out
}

func (r *paxosRound) sleepContention() {
	r.contentions++
	time.Sleep(time.Duration(mrand.Intn(100)) * time.Millisecond)
}

// begin prepares a ballot on a quorum after finishing any in-progress
// round and repairing replicas that missed the most recent commit
// (beginAndRepairPaxos). It returns the promised ballot.
func (r *paxosRound) begin() (ballot, error) {
	minMicros := int64(0)
	for {
		if time.Now().After(r.deadline) {
			return ballot{}, r.timeout(0)
		}
		b := newBallot(max(time.Now().UnixMicro(), minMicros))
		replies := r.send(cluster.VerbLWTPrepare, r.participants,
			&paxosRequest{Table: uint64(r.table), Key: r.pk, Ballot: b}, r.required)
		var promised []paxosReply
		rejected := false
		for _, rep := range replies {
			if rep.err != nil {
				continue
			}
			if !rep.resp.OK {
				rejected = true
				minMicros = max(minMicros, rep.resp.Promised.micros()+1)
				continue
			}
			promised = append(promised, rep)
		}
		if len(promised) < r.required {
			if !rejected {
				return ballot{}, r.timeout(len(promised))
			}
			r.sleepContention()
			continue
		}
		var inProgress, commit *proposal
		for _, rep := range promised {
			if a := rep.resp.Accepted; a != nil && (inProgress == nil || a.Ballot.cmp(inProgress.Ballot) > 0) {
				inProgress = a
			}
			if c := rep.resp.Commit; c != nil && (commit == nil || c.Ballot.cmp(commit.Ballot) > 0) {
				commit = c
			}
		}
		// An accepted proposal newer than the last commit: a round that did
		// not finish. Finish it with our ballot, then start over.
		if inProgress != nil && (commit == nil || inProgress.Ballot.cmp(commit.Ballot) > 0) {
			refreshed := &proposal{Ballot: b, Update: inProgress.Update}
			accepted, err := r.propose(refreshed)
			if err != nil {
				return ballot{}, err
			}
			if !accepted {
				r.sleepContention()
				continue
			}
			if err := r.commit(refreshed, primitive.ConsistencyLevelQuorum); err != nil {
				return ballot{}, err
			}
			continue
		}
		// A quorum must have learnt the most recent commit before a new
		// value is proposed (CASSANDRA-5062): repair the ones that did not.
		if commit != nil {
			var missing []*cluster.Node
			for _, rep := range promised {
				if c := rep.resp.Commit; c == nil || c.Ballot != commit.Ballot {
					missing = append(missing, r.node(rep.addr))
				}
			}
			if len(missing) > 0 {
				r.send(cluster.VerbLWTCommit, missing, &paxosRequest{Table: uint64(r.table), Key: r.pk, Ballot: commit.Ballot, Update: commit.Update}, len(missing))
				continue
			}
		}
		return b, nil
	}
}

func (r *paxosRound) node(addr string) *cluster.Node {
	for _, n := range r.participants {
		if n.Addr == addr {
			return n
		}
	}
	return &cluster.Node{Addr: addr}
}

// propose asks the participants to accept a proposal. Accepted by fewer
// than a quorum but by some, the outcome is unknown: it may still be
// completed by a later round, so this is a timeout, not a retry.
func (r *paxosRound) propose(pr *proposal) (bool, error) {
	replies := r.send(cluster.VerbLWTPropose, r.participants,
		&paxosRequest{Table: uint64(r.table), Key: r.pk, Ballot: pr.Ballot, Update: pr.Update}, r.required)
	accepts := 0
	for _, rep := range replies {
		if rep.err == nil && rep.resp.OK {
			accepts++
		}
	}
	if accepts >= r.required {
		return true, nil
	}
	if accepts > 0 {
		return false, r.timeout(accepts)
	}
	return false, nil
}

// commit sends a decision to every participant, waiting for the commit
// consistency level; when all have learnt it, their Paxos state for the
// partition is pruned.
func (r *paxosRound) commit(pr *proposal, cl primitive.ConsistencyLevel) error {
	need := 0
	if cl != primitive.ConsistencyLevelAny {
		need = blockFor(cl, r.k, r.p.d.opts.DataCenter)
	}
	req := &paxosRequest{Table: uint64(r.table), Key: r.pk, Ballot: pr.Ballot, Update: pr.Update}
	all := len(r.participants)
	b, _ := json.Marshal(req)
	ch := make(chan bool, all)
	for _, n := range r.participants {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), r.p.opts.WriteTimeout)
			defer cancel()
			var err error
			if n.Addr == r.p.self {
				err = r.p.d.paxosCommitLocal(req)
			} else {
				_, err = r.p.m.Request(ctx, n.Addr, cluster.VerbLWTCommit, b)
			}
			if err != nil {
				// A replica that missed the commit learns it from the next
				// round's repair, or from a hint.
				ws, derr := decodeWrites(pr.Update)
				if derr == nil {
					var hs []hint
					for _, w := range ws {
						hs = append(hs, hint{hostID: n.HostID, w: w})
					}
					r.p.storeHints(hs)
				}
			}
			ch <- err == nil
		}()
	}
	acks, done := 0, 0
	result := make(chan error, 1)
	go func() {
		reported := false
		for done < all {
			if <-ch {
				acks++
			}
			done++
			if !reported && (acks >= need || done == all) {
				reported = true
				if acks >= need {
					result <- nil
				} else {
					result <- &timeoutError{write: true, cas: true, cl: cl, received: acks, blockFor: need}
				}
			}
		}
		if acks == all {
			// Everyone learnt it: the state is no longer needed (Scylla's
			// prune).
			prune := *req
			prune.Prune, prune.Update = true, nil
			r.send(cluster.VerbLWTCommit, r.participants, &prune, all)
		}
	}()
	if need == 0 {
		return nil
	}
	return <-result
}

// cas runs a conditional update: apply computes, from the current state
// read at quorum, whether the conditions hold and the writes to make with
// the given timestamp.
func (p *proxy) cas(t *Table, pk []byte, serial, commitCL primitive.ConsistencyLevel,
	apply func(ts int64, readCL primitive.ConsistencyLevel) (applied bool, writes []write, err error)) error {
	if commitCL == primitive.ConsistencyLevelSerial || commitCL == primitive.ConsistencyLevelLocalSerial {
		return invalidf("Invalid consistency level %v for a conditional update commit; use SERIAL/LOCAL_SERIAL as serial consistency", commitCL)
	}
	r, err := p.newPaxosRound(t, pk, serial, commitCL)
	if err != nil {
		return err
	}
	// One CAS per partition at a time from this coordinator, so its own
	// requests do not compete (ScyllaDB's get_cas_lock).
	mu := &casLocks[maphash.Bytes(casSeed, pk)%uint64(len(casLocks))]
	mu.Lock()
	defer mu.Unlock()
	for {
		b, err := r.begin()
		if err != nil {
			return err
		}
		applied, writes, err := apply(b.micros(), serialRead(serial))
		if err != nil || !applied {
			return err
		}
		pr := &proposal{Ballot: b, Update: encodeWrites(writes)}
		accepted, err := r.propose(pr)
		if err != nil {
			return err
		}
		if !accepted {
			r.sleepContention()
			continue
		}
		return r.commit(pr, commitCL)
	}
}

// serialReadBegin makes a SERIAL read linearizable: it finishes any
// in-progress round on the partition first (readWithPaxos).
func (p *proxy) serialReadBegin(t *Table, pk []byte, serial primitive.ConsistencyLevel) error {
	r, err := p.newPaxosRound(t, pk, serial, primitive.ConsistencyLevelQuorum)
	if err != nil {
		return err
	}
	_, err = r.begin()
	return err
}
