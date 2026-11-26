package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/pbonard/cassandra-room/internal/accord"
	"github.com/pbonard/cassandra-room/internal/cluster"
	"github.com/pbonard/cassandra-room/internal/storage/lsm"
	"github.com/pbonard/cassandra-room/internal/storage/row"
)

// Accord integration (Cassandra's service/accord): tables created or
// altered WITH transactional_mode = 'full' run their conditional statements
// (lightweight transactions) as Accord transactions; so do the other
// tables (Paxos was removed), reading their conditions at QUORUM.
//
//   - A transaction's key is a partition: its token and its table's
//     storage id followed by the partition key. The topology of an epoch
//     maps a key to the replicas of its keyspace in the ring of that epoch
//     (AccordTopology).
//   - The transaction carries the statements' text and bound values, so
//     that any node can execute it again when recovering it
//     (TxnUpdate, TxnRead). A replica reads the rows the conditions need
//     from its own data, once every earlier transaction on the partition
//     has applied; the coordinator checks the conditions and computes the
//     writes, timestamped with the execution timestamp's hlc (unique, and
//     increasing among conflicting transactions).
//   - Each node's command state is journaled in an engine table
//     (system.accord_journal, AccordJournal), synced before it answers.

// accordStorage is the engine table holding the Accord journal.
const accordStorage lsm.TableID = 1<<40 + 2

// TransactionalMode values (transactional_mode).
const (
	// off: conditional statements and SERIAL reads run on Accord, reading
	// at QUORUM (interop); other reads and writes do not.
	txnModeOff = "off"
	// mixed_reads: every write runs on Accord; non-SERIAL reads do not.
	txnModeMixedReads = "mixed_reads"
	// full: every read and write runs on Accord.
	txnModeFull = "full"
)

// accordManaged reports whether every write of the table goes through
// Accord (full and mixed_reads): transactions may use it.
func (t *Table) accordManaged() bool {
	return t.Params.TransactionalMode == txnModeFull || t.Params.TransactionalMode == txnModeMixedReads
}

// accordReads reports whether the table's reads go through Accord too
// (full): its data is only ever written by Accord, so a replica's own data
// is complete once earlier transactions applied there.
func (t *Table) accordReads() bool { return t.Params.TransactionalMode == txnModeFull }

// stmtSource is a statement's text and keyspace.
type stmtSource struct {
	Query    string
	Keyspace string
}

// accordUpdate is a conditional statement (or batch) as an Accord
// transaction's update.
type accordUpdate struct {
	// Mode is what the statements are: "" conditional statements (CAS),
	// "txn" a BEGIN TRANSACTION statement, "write" plain modifications,
	// "read" a plain SELECT.
	Mode  string `json:",omitempty"`
	Stmts []accordStmt
	// Timestamp, if set, is the cells' timestamp in "write" mode (a logged
	// batch's client timestamp, which may be 0) instead of the executeAt's.
	Timestamp *int64 `json:",omitempty"`
	Now       int64  // unix nanoseconds
	NowSec    int64
	// Page and Paging page a read ("read", "range"): the page size and
	// the select's paging state.
	Page   int    `json:",omitempty"`
	Paging []byte `json:",omitempty"`
	// AggPartial asks a range read's sub-ranges for partial aggregates.
	AggPartial bool `json:",omitempty"`
	// SchemaEpoch and Tables pin the schema the transaction was made
	// with: the metadata epoch, and a fingerprint of each table it uses
	// (by storage id). A replica behind that epoch catches up first; one
	// whose table differs (changed since) reads nothing, and the
	// transaction fails with a schema change instead of each replica
	// interpreting it with its own schema.
	SchemaEpoch int64             `json:",omitempty"`
	Tables      map[string]uint64 `json:",omitempty"`
}

// accordSchemaChanged is what a read returns, and the result a
// transaction ends with, when its schema changed under it.
var accordSchemaChanged = []byte(`{"SchemaChanged":true}`)

var errAccordSchemaChanged = invalidf("The schema of a table changed during the transaction; retry it")

// tableFingerprint hashes what a transaction's statements depend on in a
// table's definition.
func tableFingerprint(t *Table) uint64 {
	h := fnv.New64a()
	fmt.Fprintf(h, "%s.%s/%d/%+v", t.Keyspace, t.Name, t.Storage, t.Params)
	for _, c := range t.AllColumns() {
		fmt.Fprintf(h, "|%s:%s:%d:%d:%d:%t", c.Name, c.Type, c.Kind, c.Position, c.ID, c.Desc)
	}
	return h.Sum64()
}

// pin records the schema a transaction over these tables is made with.
func (d *DB) pin(u *accordUpdate, storages ...lsm.TableID) {
	s := d.schema.Load()
	u.SchemaEpoch = s.Epoch
	for _, id := range storages {
		if t := s.byStorage[id]; t != nil {
			if u.Tables == nil {
				u.Tables = map[string]uint64{}
			}
			u.Tables[strconv.FormatUint(uint64(id), 10)] = tableFingerprint(t)
		}
	}
}

// pinKeys pins the schema of the tables of keys.
func (d *DB) pinKeys(u *accordUpdate, keys []accord.Key) {
	var ids []lsm.TableID
	for _, k := range keys {
		if id, _, err := splitAccordKey(k); err == nil && !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	d.pin(u, ids...)
}

// schemaHistory keeps the latest metadata snapshots.
type schemaHistory struct {
	mu   sync.Mutex
	list []*Schema // oldest first
}

const schemaHistoryLen = 64

// setSchema installs a metadata snapshot, keeping it in the history.
func (d *DB) setSchema(s *Schema) {
	d.schema.Store(s)
	h := &d.schemas
	h.mu.Lock()
	h.list = append(h.list, s)
	if len(h.list) > schemaHistoryLen {
		h.list = slices.Delete(h.list, 0, len(h.list)-schemaHistoryLen)
	}
	h.mu.Unlock()
}

// pinnedSchema returns a metadata snapshot whose tables are those the
// transaction was made with (the same fingerprints): executing against it
// gives the same result on every replica and every coordinator, whatever
// changed since. A node behind the transaction's epoch catches up first (a
// transient error if it does not in time: the step is retried). changed
// means no snapshot kept here matches (the table changed long ago, or
// before this node restarted): the transaction then fails.
func (d *DB) pinnedSchema(txn *accord.Txn) (s *Schema, changed bool, err error) {
	var u struct {
		SchemaEpoch int64
		Tables      map[string]uint64
	}
	if err := json.Unmarshal(txn.Update, &u); err != nil {
		return nil, false, err
	}
	cur := d.schema.Load()
	if u.SchemaEpoch == 0 {
		return cur, false, nil
	}
	matches := func(s *Schema) bool {
		for ids, fp := range u.Tables {
			id, err := strconv.ParseUint(ids, 10, 64)
			if err != nil {
				return false
			}
			if t := s.byStorage[lsm.TableID(id)]; t == nil || tableFingerprint(t) != fp {
				return false
			}
		}
		return true
	}
	find := func() *Schema {
		if s := d.schema.Load(); matches(s) {
			return s
		}
		d.schemas.mu.Lock()
		defer d.schemas.mu.Unlock()
		for i := len(d.schemas.list) - 1; i >= 0; i-- {
			if s := d.schemas.list[i]; matches(s) {
				return s
			}
		}
		return nil
	}
	if s := find(); s != nil {
		return s, false, nil
	}
	if cur.Epoch >= u.SchemaEpoch {
		return nil, true, nil
	}
	if d.proxy != nil {
		d.proxy.membershipChanged() // fetch the newer metadata
	}
	deadline := time.Now().Add(2 * time.Second)
	for d.schema.Load().Epoch < u.SchemaEpoch {
		if time.Now().After(deadline) {
			return nil, false, fmt.Errorf("db: the schema is behind epoch %d of a transaction", u.SchemaEpoch)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if s := find(); s != nil {
		return s, false, nil
	}
	return nil, true, nil
}

// accordRows is a paged read's result.
type accordRows struct {
	Rows   message.RowSet
	Paging []byte `json:",omitempty"`
}

type accordStmt struct {
	Query    string
	Keyspace string
	Values   []accordValue
}

type accordValue struct {
	T int32  `json:",omitempty"`
	V []byte `json:",omitempty"`
}

// accordResult is the outcome of a conditional transaction.
type accordResult struct {
	Applied bool
	Failed  int      `json:",omitempty"`
	Row     [][]byte `json:",omitempty"`
}

// accordKey is the Accord key of a table's partition.
func accordKey(id lsm.TableID, pk []byte) accord.Key {
	b := binary.BigEndian.AppendUint64(nil, uint64(id))
	return accord.Key{Token: Token(pk), ID: string(append(b, pk...))}
}

func splitAccordKey(k accord.Key) (lsm.TableID, []byte, error) {
	if len(k.ID) < 8 {
		return 0, nil, errors.New("db: bad accord key")
	}
	return lsm.TableID(binary.BigEndian.Uint64([]byte(k.ID[:8]))), []byte(k.ID[8:]), nil
}

// ---- the node ----

func (p *proxy) startAccord() error {
	d := p.d
	if _, err := d.engine.Table(accordStorage); errors.Is(err, lsm.ErrUnknownTable) {
		if _, err := d.engine.CreateTable(accordStorage); err != nil {
			return err
		}
	}
	n, err := accord.NewNode(accord.Config{
		ID:        p.self,
		Now:       d.opts.Now,
		Transport: accordTransport{p},
		Data:      accordData{d},
		Agent:     accordAgent{d},
		Journal:   &accordJournal{d: d},
		// Durability rounds (RedundantBefore) and nearest-first reads.
		DurabilityRanges:   p.accordDurabilityRanges,
		IsLeader:           p.accordLeader,
		EpochRanges:        p.accordTableRanges,
		DurabilityInterval: p.opts.AccordDurabilityInterval,
		Proximity:          p.accordProximity,
	})
	if err != nil {
		return err
	}
	p.accord = n
	p.m.Handle(cluster.VerbAccord, func(ctx context.Context, from string, payload []byte) ([]byte, error) {
		return n.Handle(ctx, from, payload)
	})
	p.accordTopology(d.schema.Load())
	n.Start()
	return nil
}

// accordTopology gives the Accord node the topology of a metadata epoch.
func (p *proxy) accordTopology(s *Schema) {
	if p.accord == nil {
		return
	}
	ring := cluster.NewRing(nodeList(s))
	future := cluster.NewFutureRing(nodeList(s))
	old := cluster.NewOldRing(nodeList(s))
	replication := func(k accord.Key) map[string]string {
		id, _, err := splitAccordKey(k)
		if err != nil {
			return nil
		}
		t := s.byStorage[id]
		if t == nil {
			return nil
		}
		if ks := s.Keyspaces[t.Keyspace]; ks != nil {
			return ks.Replication
		}
		return nil
	}
	var nodes []accord.NodeID
	for _, n := range s.Nodes {
		if n.Status != cluster.StatusLeft {
			nodes = append(nodes, n.Addr)
		}
	}
	p.accord.SetTopology(&accord.Topology{Epoch: s.Epoch, Nodes: nodes,
		Replicas: func(k accord.Key) []accord.NodeID {
			repl := replication(k)
			if repl == nil {
				return nil
			}
			var out []accord.NodeID
			for _, n := range ring.Replicas(repl, k.Token) {
				out = append(out, n.Addr)
			}
			return out
		},
		RangeReplicas: func(r accord.Range) []accord.RangeShard {
			if len(r.Prefix) < 8 {
				return nil
			}
			t := s.byStorage[lsm.TableID(binary.BigEndian.Uint64([]byte(r.Prefix[:8])))]
			if t == nil || s.Keyspaces[t.Keyspace] == nil || ring.Empty() {
				return nil
			}
			repl := s.Keyspaces[t.Keyspace].Replication
			var out []accord.RangeShard
			for tok := r.Lo; ; {
				end, wraps := ring.RangeEnd(tok)
				if wraps {
					end = math.MaxInt64
				}
				end = min(end, r.Hi)
				sh := accord.RangeShard{Range: accord.Range{Prefix: r.Prefix, Lo: tok, Hi: end}}
				for _, n := range ring.Replicas(repl, tok) {
					sh.Nodes = append(sh.Nodes, n.Addr)
				}
				out = append(out, sh)
				if end >= r.Hi {
					return out
				}
				tok = end + 1
			}
		},
		// Joining nodes receive every write while they bootstrap
		// (pending replicas), so that the data they stream is not missing
		// the transactions applied meanwhile.
		Pending: func(k accord.Key) []accord.NodeID {
			repl := replication(k)
			if repl == nil {
				return nil
			}
			// The write placement: old and future owners (a leaving node
			// keeps receiving writes until its movement finishes).
			var out []accord.NodeID
			for _, r := range []*cluster.Ring{future, old} {
				for _, n := range r.Replicas(repl, k.Token) {
					if !slices.Contains(out, n.Addr) {
						out = append(out, n.Addr)
					}
				}
			}
			return out
		}})
}

type accordTransport struct{ p *proxy }

func (t accordTransport) Request(ctx context.Context, to accord.NodeID, payload []byte) ([]byte, error) {
	return t.p.m.Request(ctx, to, cluster.VerbAccord, payload)
}

// accordJournal keeps the Accord command state in an engine table, one
// row per command whose single cell holds its latest state (newer records
// have higher timestamps, so they win when the engine merges them).
type accordJournal struct {
	d    *DB
	last atomic.Int64
}

func journalKey(id accord.TxnID) []byte {
	b := binary.BigEndian.AppendUint64(nil, uint64(id.Epoch))
	b = binary.BigEndian.AppendUint64(b, uint64(id.HLC))
	b = append(b, id.Node...)
	return append(b, 0, byte(id.Kind))
}

func (j *accordJournal) nextTS() int64 {
	ts := j.d.opts.Now().UnixMicro()
	for {
		last := j.last.Load()
		ts = max(ts, last+1)
		if j.last.CompareAndSwap(last, ts) {
			return ts
		}
	}
}

// put adds a record to a batch: the command's state, or (no data) a row
// deletion newer than its records.
func (j *accordJournal) put(b *lsm.Batch, id accord.TxnID, data []byte) {
	ts := j.nextTS()
	r := &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: ts, LocalDeletionTime: j.d.opts.Now().Unix()}}
	if data != nil {
		r = &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp}, Cells: []row.Cell{liveCell(0, ts, 0, 0, data)}}
	}
	b.Put(accordStorage, journalKey(id), row.Append(nil, r))
}

func (j *accordJournal) apply(b *lsm.Batch) error {
	j.d.tableMu.RLock()
	defer j.d.tableMu.RUnlock()
	return j.d.engine.Apply(b, lsm.Sync)
}

func (j *accordJournal) Append(c *accord.Command) error {
	v, err := json.Marshal(c)
	if err != nil {
		return err
	}
	b := lsm.NewBatch()
	j.put(b, c.ID, v)
	return j.apply(b)
}

// AppendBatch writes several records with one synced commit-log write
// (the node's group commit).
func (j *accordJournal) AppendBatch(recs []accord.Record) error {
	b := lsm.NewBatch()
	for _, r := range recs {
		j.put(b, r.ID, r.Data)
	}
	return j.apply(b)
}

func (j *accordJournal) Load() ([]*accord.Command, error) {
	t, err := j.d.engine.Table(accordStorage)
	if err != nil {
		return nil, err
	}
	it, err := t.NewIter(nil, nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []*accord.Command
	for ok := it.First(); ok; ok = it.Next() {
		r, err := row.Decode(it.Value())
		if err != nil {
			return nil, err
		}
		for _, cell := range r.Cells {
			if r.Deletion.MarkedForDeleteAt != row.NoTimestamp && cell.Timestamp <= r.Deletion.MarkedForDeleteAt {
				continue // forgotten
			}
			var c accord.Command
			if err := json.Unmarshal(cell.Value, &c); err != nil {
				return nil, fmt.Errorf("db: corrupt accord journal: %w", err)
			}
			out = append(out, &c)
			j.last.Store(max(j.last.Load(), cell.Timestamp))
		}
	}
	return out, it.Error()
}

// Delete forgets a command (a row deletion newer than its record).
func (j *accordJournal) Delete(id accord.TxnID) error {
	b := lsm.NewBatch()
	j.put(b, id, nil)
	return j.apply(b)
}

// Compact is a no-op: each command has one record, replaced in place.
func (j *accordJournal) Compact([]*accord.Command) error { return nil }

// ---- executing transactions ----

// accordStatement rebuilds a "txn" or "read" transaction's statement and
// its context.
func (d *DB) accordStatement(sc *Schema, txn *accord.Txn) (statement, *execCtx, *accordUpdate, error) {
	var u accordUpdate
	if err := json.Unmarshal(txn.Update, &u); err != nil {
		return nil, nil, nil, err
	}
	if len(u.Stmts) != 1 {
		return nil, nil, &u, nil
	}
	st := u.Stmts[0]
	ps, err := d.prepareIn(sc, st.Keyspace, st.Query)
	if err != nil {
		return nil, nil, nil, err
	}
	values := make([]*primitive.Value, len(st.Values))
	for i, v := range st.Values {
		values[i] = &primitive.Value{Type: primitive.ValueType(v.T), Contents: v.V}
	}
	e := &execCtx{d: d, values: values, now: time.Unix(0, u.Now), nowSec: u.NowSec, cl: primitive.ConsistencyLevelOne, local: true, listSeq: new(uint32),
		page: u.Page, paging: u.Paging}
	return ps.stmt, e, &u, nil
}

// accordMods rebuilds a transaction's statements.
func (d *DB) accordMods(sc *Schema, txn *accord.Txn) ([]boundModification, *accordUpdate, error) {
	var u accordUpdate
	if err := json.Unmarshal(txn.Update, &u); err != nil {
		return nil, nil, err
	}
	now := time.Unix(0, u.Now)
	var mods []boundModification
	seq := new(uint32)
	for _, st := range u.Stmts {
		ps, err := d.prepareIn(sc, st.Keyspace, st.Query)
		if err != nil {
			return nil, nil, err
		}
		values := make([]*primitive.Value, len(st.Values))
		for i, v := range st.Values {
			values[i] = &primitive.Value{Type: primitive.ValueType(v.T), Contents: v.V}
		}
		e := &execCtx{d: d, values: values, now: now, nowSec: u.NowSec, cl: primitive.ConsistencyLevelOne, local: true, listSeq: seq}
		switch s := ps.stmt.(type) {
		case *modification:
			mods = append(mods, boundModification{m: s, e: e})
		case *batchStmt:
			for _, m := range s.mods {
				child := *e
				mods = append(mods, boundModification{m: m, e: &child})
			}
		default:
			return nil, nil, fmt.Errorf("db: not a modification in a transaction: %s", st.Query)
		}
	}
	return mods, &u, nil
}

type accordData struct{ d *DB }

// Read reads, from this replica's data, the current row of each
// conditional statement on the partition.
func (a accordData) Read(k accord.Key, _ accord.Timestamp, txn *accord.Txn) ([]byte, error) {
	sc, changed, err := a.d.pinnedSchema(txn)
	if err != nil || changed {
		return accordSchemaChanged, err
	}
	var mode struct{ Mode string }
	if err := json.Unmarshal(txn.Update, &mode); err != nil {
		return nil, err
	}
	switch mode.Mode {
	case "write":
		return nil, nil
	case "txn", "read":
		st, e, _, err := a.d.accordStatement(sc, txn)
		if err != nil {
			return nil, err
		}
		if tx, ok := st.(*txnStmt); ok {
			reads, err := tx.readLocal(e, &k)
			if err != nil {
				return nil, err
			}
			return json.Marshal(reads)
		}
		return selectRows(st.(*selectStmt), e)
	}
	mods, _, err := a.d.accordMods(sc, txn)
	if err != nil {
		return nil, err
	}
	rows := make([][][]byte, len(mods))
	for i, bm := range mods {
		if bm.m.cond == nil {
			continue
		}
		e := bm.e
		re := &execCtx{d: a.d, values: e.values, now: e.now, nowSec: e.nowSec, cl: primitive.ConsistencyLevelOne, local: true}
		if !bm.m.t.accordReads() {
			// Interop (AccordInteropRead): the table also takes writes
			// outside Accord, so the condition is read at QUORUM (this
			// replica first), with blocking read repair.
			re.cl, re.local = primitive.ConsistencyLevelQuorum, false
		}
		res, err := bm.m.cond.read.execute(re)
		if err != nil {
			return nil, err
		}
		if data := res.(*message.RowsResult).Data; len(data) > 0 {
			rows[i] = data[0]
		}
	}
	return json.Marshal(rows)
}

// Apply applies a transaction's writes to this replica.
func (a accordData) Apply(_ accord.Key, _ accord.Timestamp, _ *accord.Txn, w []byte) error {
	writes, err := decodeWrites(w)
	if err != nil {
		return err
	}
	return a.d.applyLocal(writes)
}

type accordAgent struct{ d *DB }

// Execute checks the conditions against the rows read and computes the
// writes, timestamped with the execution timestamp.
func (a accordAgent) Execute(txn *accord.Txn, executeAt accord.Timestamp, reads map[string][]byte) (accord.Writes, []byte, error) {
	// The outcome is decided from the reads: a replica whose table changed
	// read nothing, and the transaction writes nothing.
	for _, r := range reads {
		if bytes.Equal(r, accordSchemaChanged) {
			return accord.Writes{}, accordSchemaChanged, nil
		}
	}
	sc, changed, err := a.d.pinnedSchema(txn)
	if err != nil {
		return nil, nil, err
	}
	if changed {
		return accord.Writes{}, accordSchemaChanged, nil
	}
	var mode struct{ Mode string }
	if err := json.Unmarshal(txn.Update, &mode); err != nil {
		return nil, nil, err
	}
	switch mode.Mode {
	case "read":
		return accord.Writes{}, reads[accordKeyString(txn.Keys[0])], nil
	case "write":
		mods, _, err := a.d.accordMods(sc, txn)
		if err != nil {
			return nil, nil, err
		}
		var u struct{ Timestamp *int64 }
		if err := json.Unmarshal(txn.Update, &u); err != nil {
			return nil, nil, err
		}
		mu := &mutation{}
		for _, bm := range mods {
			bm.e.setTimestamp(executeAt.HLC)
			if u.Timestamp != nil {
				bm.e.setTimestamp(*u.Timestamp)
			}
			if err := bm.m.addMutations(bm.e, mu); err != nil {
				return nil, nil, err
			}
		}
		return groupWrites(txn.Keys, mu.writes), []byte("null"), nil
	case "txn":
		st, e, _, err := a.d.accordStatement(sc, txn)
		if err != nil {
			return nil, nil, err
		}
		tx := st.(*txnStmt)
		merged := make(txnReads, len(tx.reads()))
		for _, k := range txn.Keys {
			var r txnReads
			if b := reads[accordKeyString(k)]; b != nil {
				if err := json.Unmarshal(b, &r); err != nil {
					return nil, nil, err
				}
			}
			for i := range r {
				if i < len(merged) && r[i] != nil {
					merged[i] = r[i]
				}
			}
		}
		e.setTimestamp(executeAt.HLC)
		mu := &mutation{}
		rows, err := tx.evaluate(e, merged, mu)
		if err != nil {
			return nil, nil, err
		}
		out, err := json.Marshal(rows)
		return groupWrites(txn.Keys, mu.writes), out, err
	}
	mods, _, err := a.d.accordMods(sc, txn)
	if err != nil {
		return nil, nil, err
	}
	k := txn.Keys[0]
	var rows [][][]byte
	if err := json.Unmarshal(reads[accordKeyString(k)], &rows); err != nil {
		return nil, nil, err
	}
	idx := map[*execCtx]int{}
	for i, bm := range mods {
		idx[bm.e] = i
	}
	applied, failed, current, err := evalConditions(mods, func(bm boundModification) ([][]byte, error) {
		return rows[idx[bm.e]], nil
	})
	if err != nil {
		return nil, nil, err
	}
	res := accordResult{Applied: applied, Failed: failed, Row: current}
	out, _ := json.Marshal(res)
	if !applied || len(mods) == 0 {
		return accord.Writes{}, out, nil
	}
	mu := &mutation{}
	for _, bm := range mods {
		bm.e.setTimestamp(executeAt.HLC)
		if err := bm.m.addMutations(bm.e, mu); err != nil {
			return nil, nil, err
		}
	}
	return accord.Writes{accordKeyString(k): encodeWrites(mu.writes)}, out, nil
}

// accordBarrier runs a read-only transaction on a partition: once it has
// executed, a quorum of the partition's replicas has applied every
// transaction that executes before it, so a quorum read that follows sees
// them (a SERIAL read).
func (p *proxy) accordBarrier(t *Table, pk []byte) error {
	return p.accordBarrierKey(accordKey(t.Storage, pk))
}

func (p *proxy) accordBarrierKey(k accord.Key) error {
	return p.accordBarrierKeys([]accord.Key{k})
}

func (p *proxy) accordBarrierKeys(keys []accord.Key) error {
	keys = slices.Clone(keys)
	slices.SortFunc(keys, accord.Key.Compare)
	keys = slices.CompactFunc(keys, func(a, b accord.Key) bool { return a.Compare(b) == 0 })
	if err := p.accordAvailable(keys); err != nil {
		return err
	}
	b, _ := json.Marshal(accordUpdate{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*p.opts.ReadTimeout)
	defer cancel()
	_, err := p.accord.Coordinate(ctx, &accord.Txn{Kind: accord.Read, Keys: keys, Update: b})
	if err != nil {
		return &timeoutError{cl: primitive.ConsistencyLevelSerial}
	}
	return nil
}

// groupWrites assigns writes to the transaction keys of their partitions.
func groupWrites(keys []accord.Key, writes []write) accord.Writes {
	// By partition, not just token: two keys of a table may share one.
	prefixes := make([][]byte, len(keys))
	for i, k := range keys {
		if _, pk, err := splitAccordKey(k); err == nil {
			prefixes[i] = partitionPrefix(pk)
		}
	}
	per := map[string][]write{}
	for _, w := range writes {
		for i, k := range keys {
			id, _, err := splitAccordKey(k)
			if err == nil && id == w.table && k.Token == w.token && bytes.HasPrefix(w.key, prefixes[i]) {
				per[accordKeyString(k)] = append(per[accordKeyString(k)], w)
				break
			}
		}
	}
	out := accord.Writes{}
	for k, ws := range per {
		out[k] = encodeWrites(ws)
	}
	return out
}

// accordSource encodes the statement of e (and its values).
func accordSource(e *execCtx) (accordStmt, error) {
	if e.src == nil {
		return accordStmt{}, errors.New("db: a transaction statement without its text")
	}
	st := accordStmt{Query: e.src.Query, Keyspace: e.src.Keyspace}
	for _, v := range e.values {
		if v == nil {
			st.Values = append(st.Values, accordValue{T: int32(primitive.ValueTypeNull)})
			continue
		}
		st.Values = append(st.Values, accordValue{T: int32(v.Type), V: v.Contents})
	}
	return st, nil
}

// accordRun runs the statement of e (a transaction, or a plain read) as an
// Accord transaction over keys.
func (p *proxy) accordRun(e *execCtx, mode string, keys []accord.Key, writes bool) ([]byte, error) {
	st, err := accordSource(e)
	if err != nil {
		return nil, err
	}
	kind := accord.Write
	if !writes {
		kind = accord.Read
	}
	u := &accordUpdate{Mode: mode, Stmts: []accordStmt{st}, Now: e.now.UnixNano(), NowSec: e.nowSec}
	if mode == "read" {
		u.Page, u.Paging = e.page, e.paging
	}
	return p.accordCoordinate(u, keys, kind)
}

func (p *proxy) accordCoordinate(u *accordUpdate, keys []accord.Key, kind accord.Kind) ([]byte, error) {
	if len(keys) == 0 {
		return nil, invalidf("A transaction needs at least one partition")
	}
	if err := p.accordAvailable(keys); err != nil {
		return nil, err
	}
	p.d.pinKeys(u, keys)
	b, err := json.Marshal(u)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*p.opts.WriteTimeout)
	defer cancel()
	out, err := p.accord.Coordinate(ctx, &accord.Txn{Kind: kind, Keys: keys, Update: b})
	switch {
	case errors.Is(err, accord.ErrTimeout), errors.Is(err, accord.ErrInvalidated):
		if kind == accord.Read {
			return nil, &timeoutError{cl: primitive.ConsistencyLevelSerial}
		}
		return nil, &timeoutError{write: true, cas: true, cl: primitive.ConsistencyLevelSerial}
	case err == nil && bytes.Equal(out, accordSchemaChanged):
		return nil, errAccordSchemaChanged
	}
	return out, err
}

// accordWrite applies plain modifications to transactional tables as one
// Accord transaction (transactional_mode 'full' routes every write through
// Accord, so writes are ordered with the transactions).
func (p *proxy) accordWrite(mods []boundModification, batchCtx *execCtx) error {
	u := &accordUpdate{Mode: "write", Now: batchCtx.now.UnixNano(), NowSec: batchCtx.nowSec}
	if !mods[0].m.t.accordManaged() {
		// A logged batch on ordinary tables: Accord only makes it atomic;
		// its cells take the client's timestamp, as any write's.
		if batchCtx.tsSet {
			ts := batchCtx.ts
			u.Timestamp = &ts
		}
	}
	var keys []accord.Key
	var last *stmtSource
	for _, bm := range mods {
		if bm.m.timestamp != nil && bm.m.t.accordManaged() {
			return invalidf("Custom timestamps are not supported on tables with transactional_mode 'full'")
		}
		ks, err := stmtPartitions(bm.e, bm.m.t, bm.m.where)
		if err != nil {
			return err
		}
		for _, k := range ks {
			if !slices.Contains(keys, k) {
				keys = append(keys, k)
			}
		}
		if bm.e.src == last {
			continue
		}
		last = bm.e.src
		st, err := accordSource(bm.e)
		if err != nil {
			return err
		}
		u.Stmts = append(u.Stmts, st)
	}
	slices.SortFunc(keys, accord.Key.Compare)
	_, err := p.accordCoordinate(u, keys, accord.Write)
	return err
}

// accordKeyString is the key's name in reads and writes (accord's own).
func accordKeyString(k accord.Key) string { return fmt.Sprintf("%d/%s", k.Token, k.ID) }

// accordCAS runs conditional statements on one partition as an Accord
// transaction.
func (p *proxy) accordCAS(mods []boundModification, t *Table, pk []byte) (message.Message, error) {
	u := accordUpdate{Now: mods[0].e.now.UnixNano(), NowSec: mods[0].e.nowSec}
	var last *stmtSource
	for _, bm := range mods {
		if bm.e.src == nil {
			return nil, errors.New("db: a transaction statement without its text")
		}
		if bm.e.src == last {
			continue // another statement of the same BATCH text
		}
		last = bm.e.src
		st := accordStmt{Query: bm.e.src.Query, Keyspace: bm.e.src.Keyspace}
		for _, v := range bm.e.values {
			if v == nil {
				st.Values = append(st.Values, accordValue{T: int32(primitive.ValueTypeNull)})
				continue
			}
			st.Values = append(st.Values, accordValue{T: int32(v.Type), V: v.Contents})
		}
		u.Stmts = append(u.Stmts, st)
	}
	p.d.pin(&u, t.Storage)
	b, err := json.Marshal(u)
	if err != nil {
		return nil, err
	}
	key := accordKey(t.Storage, pk)
	if err := p.accordAvailable([]accord.Key{key}); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*p.opts.WriteTimeout)
	defer cancel()
	out, err := p.accord.Coordinate(ctx, &accord.Txn{Kind: accord.Write, Keys: []accord.Key{key}, Update: b})
	switch {
	case errors.Is(err, accord.ErrTimeout), errors.Is(err, accord.ErrInvalidated):
		return nil, &timeoutError{write: true, cas: true, cl: primitive.ConsistencyLevelSerial}
	case err != nil:
		return nil, err
	case bytes.Equal(out, accordSchemaChanged):
		return nil, errAccordSchemaChanged
	}
	var res accordResult
	if err := json.Unmarshal(out, &res); err != nil {
		return nil, err
	}
	return casResult(mods, res.Applied, res.Failed, res.Row), nil
}

// selectRows runs a select on this node's data and encodes its rows and
// paging state.
func selectRows(s *selectStmt, e *execCtx) ([]byte, error) {
	res, err := s.execute(e)
	if err != nil {
		return nil, err
	}
	rows := res.(*message.RowsResult)
	return json.Marshal(accordRows{Rows: rows.Data, Paging: rows.Metadata.PagingState})
}

// ReadRange reads a sub-range of a range read on this replica: the
// select, restricted to the sub-range's tokens.
func (a accordData) ReadRange(r accord.Range, txn *accord.Txn) ([]byte, error) {
	sc, changed, err := a.d.pinnedSchema(txn)
	if err != nil || changed {
		return accordSchemaChanged, err
	}
	st, e, _, err := a.d.accordStatement(sc, txn)
	if err != nil {
		return nil, err
	}
	sel, ok := st.(*selectStmt)
	if !ok {
		return nil, errors.New("db: a range read of a statement that is not a SELECT")
	}
	e.tokenBounds = &[2]int64{r.Lo, r.Hi}
	var u struct{ AggPartial bool }
	_ = json.Unmarshal(txn.Update, &u)
	e.aggPartial = u.AggPartial
	return selectRows(sel, e)
}

// accordRangePaging is the paging state of an Accord range read: the
// token to resume from and the paging state of the sub-range read there.
type accordRangePaging struct {
	Token int64
	Inner []byte `json:",omitempty"`
	// Remaining is what is left of the LIMIT, if any.
	Remaining int64 `json:",omitempty"`
}

// accordRangeMarker starts an Accord range read's paging state.
const accordRangeMarker = 0xAC

// accordRangeSelect runs a range read of a transactional table through
// Accord: sub-range by sub-range in token order (each an ephemeral range
// read: every write acknowledged before it is seen), until the page is
// full.
func (p *proxy) accordRangeSelect(s *selectStmt, e *execCtx) (message.Message, error) {
	lo, hi, empty, err := s.where.tokenRange(e)
	if err != nil {
		return nil, err
	}
	md := s.metadata()
	res := &message.RowsResult{Metadata: &message.RowsMetadata{ColumnCount: int32(len(md)), Columns: md}}
	if empty {
		return res, nil
	}
	var resume *accordRangePaging
	if len(e.paging) > 0 {
		if e.paging[0] != accordRangeMarker {
			return nil, invalidf("Invalid value for the paging state")
		}
		resume = &accordRangePaging{}
		if err := json.Unmarshal(e.paging[1:], resume); err != nil {
			return nil, invalidf("Invalid value for the paging state")
		}
		lo = max(lo, resume.Token)
	}
	limit, hasLimit, err := bindInt(e, s.limit, "limit")
	if err != nil {
		return nil, err
	}
	if hasLimit && resume != nil {
		limit = resume.Remaining
	}
	src, err := accordSource(e)
	if err != nil {
		return nil, err
	}
	partial := s.aggregate && len(s.groupBy) == 0
	if s.aggregate || len(s.groupBy) > 0 {
		// Aggregates are not paged: GROUP BY groups (which hold whole
		// partitions) come complete from each sub-range; a plain aggregate's
		// partial states are merged.
		e.page, resume = 0, nil
	}
	var agg *aggState
	if partial {
		agg = newAggState(s.sels)
	}
	prefix := string(binary.BigEndian.AppendUint64(nil, uint64(s.t.Storage)))
	shards := p.accord.RangeShards(accord.Range{Prefix: prefix, Lo: lo, Hi: hi})
	ctx, cancel := context.WithTimeout(context.Background(), 3*p.opts.ReadTimeout)
	defer cancel()
	for _, sh := range shards {
		want := 0
		if e.page > 0 {
			want = e.page - len(res.Data)
		}
		if hasLimit {
			left := int(limit) - len(res.Data)
			if want == 0 || left < want {
				want = left
			}
		}
		u := accordUpdate{Mode: "range", Stmts: []accordStmt{src}, Now: e.now.UnixNano(), NowSec: e.nowSec, Page: want, AggPartial: partial}
		if resume != nil && sh.Lo <= resume.Token {
			u.Paging = resume.Inner
		}
		p.d.pin(&u, s.t.Storage)
		b, err := json.Marshal(u)
		if err != nil {
			return nil, err
		}
		subs, reads, err := p.accord.ReadRange(ctx, sh.Range, &accord.Txn{Kind: accord.Read, Update: b})
		if err != nil {
			return nil, &timeoutError{cl: e.cl}
		}
		for _, sub := range subs {
			if bytes.Equal(reads[sub.Range.String()], accordSchemaChanged) {
				return nil, errAccordSchemaChanged
			}
			var rows accordRows
			if err := json.Unmarshal(reads[sub.Range.String()], &rows); err != nil {
				return nil, err
			}
			if partial {
				var p aggPartial
				if len(rows.Rows) != 1 || json.Unmarshal(rows.Rows[0][0], &p) != nil {
					return nil, errors.New("db: bad partial aggregate")
				}
				if err := agg.merge(p); err != nil {
					return nil, err
				}
				continue
			}
			res.Data = append(res.Data, rows.Rows...)
			full := e.page > 0 && len(res.Data) >= e.page
			if hasLimit && int64(len(res.Data)) >= limit {
				return res, nil
			}
			if len(rows.Paging) > 0 || full && sub.Hi < hi {
				next := accordRangePaging{Token: sub.Lo, Inner: rows.Paging, Remaining: limit - int64(len(res.Data))}
				if len(rows.Paging) == 0 {
					next.Token = sub.Hi + 1
				}
				nb, _ := json.Marshal(next)
				res.Metadata.PagingState = append([]byte{accordRangeMarker}, nb...)
				return res, nil
			}
			if hasLimit && len(res.Data) >= int(limit) {
				return res, nil
			}
		}
	}
	if partial {
		row, err := agg.result(nil)
		if err != nil {
			return nil, err
		}
		res.Data = message.RowSet{row}
	}
	return res, nil
}

// liveCell is a cell with an optional TTL.
func liveCell(col uint32, ts int64, ttl uint32, nowSec int64, v []byte) row.Cell {
	c := row.Cell{Column: col, Timestamp: ts, LocalDeletionTime: row.NoDeletionTime, Value: v}
	if ttl > 0 {
		c.TTL, c.LocalDeletionTime = ttl, nowSec+int64(ttl)
	}
	return c
}

// accordAvailable fails fast when fewer than a quorum of some key's
// replicas are alive (the transaction could not complete).
func (p *proxy) accordAvailable(keys []accord.Key) error {
	s := p.d.schema.Load()
	ring := p.ring.Load()
	for _, k := range keys {
		id, _, err := splitAccordKey(k)
		if err != nil {
			continue
		}
		t := s.byStorage[id]
		if t == nil || s.Keyspaces[t.Keyspace] == nil {
			continue
		}
		replicas := ring.Replicas(s.Keyspaces[t.Keyspace].Replication, k.Token)
		alive := 0
		for _, n := range replicas {
			if p.up(n) {
				alive++
			}
		}
		if need := len(replicas)/2 + 1; alive < need {
			return &unavailableError{cl: primitive.ConsistencyLevelSerial, required: need, alive: alive}
		}
	}
	return nil
}

// accordStatRows are the node's Accord metrics (system_views.accord_stats).
func accordStatRows(d *DB) []map[string][]byte {
	if d.proxy == nil || d.proxy.accord == nil {
		return nil
	}
	st := d.proxy.accord.Stats()
	var out []map[string][]byte
	for _, kv := range []struct {
		name string
		v    int64
	}{
		{"fast_path", st.FastPath}, {"medium_path", st.MediumPath}, {"slow_path", st.SlowPath},
		{"recoveries", st.Recoveries}, {"invalidations", st.Invalidations}, {"ephemeral_reads", st.EphemeralReads},
		{"range_reads", st.RangeReads}, {"sync_points", st.SyncPoints}, {"durability_rounds", st.DurabilityRounds},
		{"preempted", st.Preempted}, {"stale_epoch_or_barrier", st.Stale}, {"timeouts", st.Timeouts},
		{"majority_rounds", st.MajorityRounds}, {"journal_errors", st.JournalErrors}, {"persist_shed", st.PersistShed},
		{"epoch_syncs", st.EpochSyncs},
		{"commands", int64(st.Commands)}, {"keys", int64(st.Keys)}, {"barriers", int64(st.Barriers)},
		{"redundant_bounds", int64(st.Redundant)}, {"bootstrapped_ranges", int64(st.Bootstrapped)}, {"epoch", st.Epoch}, {"synced_epoch", st.SyncedEpoch},
	} {
		out = append(out, map[string][]byte{"name": textV(kv.name), "value": bigintV(kv.v)})
	}
	return out
}

// accordTxnRows are the transactions in progress on this node and what
// they wait for (system_views.accord_txns, DebugBlockedTxns).
func accordTxnRows(d *DB) []map[string][]byte {
	if d.proxy == nil || d.proxy.accord == nil {
		return nil
	}
	var out []map[string][]byte
	for _, c := range d.proxy.accord.Commands() {
		kind := map[accord.Kind]string{accord.Write: "write", accord.Read: "read", accord.SyncPoint: "sync_point"}[c.Kind]
		r := map[string][]byte{"txn_id": textV(c.ID.String()), "kind": textV(kind), "status": textV(c.Status.String()),
			"execute_at": textV(c.ExecuteAt.String()), "keys": intV(c.Keys), "deps": intV(c.Deps)}
		if c.BlockedOn != nil {
			r["blocked_on"] = textV(c.BlockedOn.String())
		}
		out = append(out, r)
	}
	return out
}

// accordLeader reports whether this node synchronises Accord's epochs:
// the live normal node with the lowest address.
func (p *proxy) accordLeader() bool {
	for _, n := range p.d.schema.Load().Nodes {
		if n.Status == cluster.StatusNormal && n.Addr < p.self && p.up(n) {
			return false
		}
	}
	return true
}

// accordDurabilityRanges are the ranges this node runs durability rounds
// over: those it is the first replica of, in every table (each node
// covers its own; ranges without transactions since their last round are
// skipped).
func (p *proxy) accordDurabilityRanges() []accord.Range {
	return p.accordOwnedRanges(p.ring.Load(), true)
}

// accordOwnedRanges are the accord ranges of every table this node
// replicates in a ring (only those it is the first replica of, if
// primary).
func (p *proxy) accordOwnedRanges(ring *cluster.Ring, primary bool) []accord.Range {
	s := p.d.schema.Load()
	if ring.Empty() {
		return nil
	}
	var out []accord.Range
	for _, t := range p.d.userTables() {
		k := s.Keyspaces[t.Keyspace]
		if k == nil {
			continue
		}
		prefix := accordPrefix(t.Storage)
		for _, r := range ring.Ranges() {
			reps := ring.Replicas(k.Replication, r[1])
			if primary && (len(reps) == 0 || reps[0].Addr != p.self) || !primary && !containsAddr(reps, p.self) {
				continue
			}
			out = append(out, accordRangesOf(prefix, r)...)
		}
	}
	return out
}

// accordRangesOf returns the Accord ranges of a ring range (start, end] of
// a table (its prefix): a range wrapping around the token space is two.
func accordRangesOf(prefix string, r [2]int64) []accord.Range {
	if r[0] < r[1] {
		return []accord.Range{{Prefix: prefix, Lo: r[0] + 1, Hi: r[1]}}
	}
	var out []accord.Range
	if r[0] < math.MaxInt64 {
		out = append(out, accord.Range{Prefix: prefix, Lo: r[0] + 1, Hi: math.MaxInt64})
	}
	return append(out, accord.Range{Prefix: prefix, Lo: math.MinInt64, Hi: r[1]})
}

// accordPrefix is a table's prefix in Accord keys and ranges.
func accordPrefix(id lsm.TableID) string {
	return string(binary.BigEndian.AppendUint64(nil, uint64(id)))
}

// accordTableRanges are the whole token ranges of the user tables.
func (p *proxy) accordTableRanges() []accord.Range {
	var out []accord.Range
	for _, t := range p.d.userTables() {
		out = append(out, accord.Range{Prefix: string(binary.BigEndian.AppendUint64(nil, uint64(t.Storage))), Lo: math.MinInt64, Hi: math.MaxInt64})
	}
	return out
}

// accordProximity ranks a node by distance: same rack, same data center,
// elsewhere.
func (p *proxy) accordProximity(addr accord.NodeID) int {
	s := p.d.schema.Load()
	me, other := s.nodeByAddr(p.self), s.nodeByAddr(addr)
	switch {
	case me == nil || other == nil:
		return 3
	case me.DataCenter == other.DataCenter && me.Rack == other.Rack:
		return 1
	case me.DataCenter == other.DataCenter:
		return 2
	}
	return 3
}
