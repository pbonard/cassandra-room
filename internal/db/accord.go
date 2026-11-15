package db

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
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
// (lightweight transactions) as Accord transactions instead of Paxos.
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
	txnModeOff  = "off"
	txnModeFull = "full"
)

func (t *Table) accordManaged() bool {
	return t.Params.TransactionalMode != "" && t.Params.TransactionalMode != txnModeOff
}

// stmtSource is a statement's text and keyspace.
type stmtSource struct {
	Query    string
	Keyspace string
}

// accordUpdate is a conditional statement (or batch) as an Accord
// transaction's update.
type accordUpdate struct {
	Stmts  []accordStmt
	Now    int64 // unix nanoseconds
	NowSec int64
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
	h := fnv.New32a()
	h.Write([]byte(d.local.HostID))
	n, err := accord.NewNode(accord.Config{
		ID:        p.self,
		Tag:       int(h.Sum32()),
		Now:       d.opts.Now,
		Transport: accordTransport{p},
		Data:      accordData{d},
		Agent:     accordAgent{d},
		Journal:   &accordJournal{d: d},
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
	p.accord.SetTopology(&accord.Topology{Epoch: s.Epoch, Replicas: func(k accord.Key) []accord.NodeID {
		id, _, err := splitAccordKey(k)
		if err != nil {
			return nil
		}
		t := s.byStorage[id]
		if t == nil {
			return nil
		}
		ks := s.Keyspaces[t.Keyspace]
		if ks == nil {
			return nil
		}
		var out []accord.NodeID
		for _, n := range ring.Replicas(ks.Replication, k.Token) {
			out = append(out, n.Addr)
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

func (j *accordJournal) Append(c *accord.Command) error {
	v, err := json.Marshal(c)
	if err != nil {
		return err
	}
	ts := j.d.opts.Now().UnixMicro()
	for {
		last := j.last.Load()
		ts = max(ts, last+1)
		if j.last.CompareAndSwap(last, ts) {
			break
		}
	}
	r := &row.Row{Deletion: row.Deletion{MarkedForDeleteAt: row.NoTimestamp}, Cells: []row.Cell{liveCell(0, ts, 0, 0, v)}}
	b := lsm.NewBatch()
	b.Put(accordStorage, journalKey(c.ID), row.Append(nil, r))
	j.d.tableMu.RLock()
	defer j.d.tableMu.RUnlock()
	return j.d.engine.Apply(b, lsm.Sync)
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

// Compact is a no-op: each command has one record, replaced in place.
func (j *accordJournal) Compact([]*accord.Command) error { return nil }

// ---- executing transactions ----

// accordMods rebuilds a transaction's statements.
func (d *DB) accordMods(txn *accord.Txn) ([]boundModification, *accordUpdate, error) {
	var u accordUpdate
	if err := json.Unmarshal(txn.Update, &u); err != nil {
		return nil, nil, err
	}
	now := time.Unix(0, u.Now)
	var mods []boundModification
	seq := new(uint32)
	for _, st := range u.Stmts {
		p, err := d.Prepare(st.Keyspace, st.Query)
		if err != nil {
			return nil, nil, err
		}
		ps, err := d.current(p)
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
	mods, _, err := a.d.accordMods(txn)
	if err != nil {
		return nil, err
	}
	rows := make([][][]byte, len(mods))
	for i, bm := range mods {
		if bm.m.cond == nil {
			continue
		}
		e := bm.e
		res, err := bm.m.cond.read.execute(&execCtx{d: a.d, values: e.values, now: e.now, nowSec: e.nowSec, cl: primitive.ConsistencyLevelOne, local: true})
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
	mods, _, err := a.d.accordMods(txn)
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
		bm.e.ts = executeAt.HLC
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
	b, _ := json.Marshal(accordUpdate{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*p.opts.ReadTimeout)
	defer cancel()
	_, err := p.accord.Coordinate(ctx, &accord.Txn{Kind: accord.Read, Keys: []accord.Key{accordKey(t.Storage, pk)}, Update: b})
	if err != nil {
		return &timeoutError{cl: primitive.ConsistencyLevelSerial}
	}
	return nil
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
	b, err := json.Marshal(u)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*p.opts.WriteTimeout)
	defer cancel()
	out, err := p.accord.Coordinate(ctx, &accord.Txn{Kind: accord.Write, Keys: []accord.Key{accordKey(t.Storage, pk)}, Update: b})
	switch {
	case errors.Is(err, accord.ErrTimeout), errors.Is(err, accord.ErrInvalidated):
		return nil, &timeoutError{write: true, cas: true, cl: primitive.ConsistencyLevelSerial}
	case err != nil:
		return nil, err
	}
	var res accordResult
	if err := json.Unmarshal(out, &res); err != nil {
		return nil, err
	}
	return casResult(mods, res.Applied, res.Failed, res.Row), nil
}
