package db

import (
	"hash/maphash"
	"sync"

	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/cassandra-room/cassandra-room/internal/types"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

// Lightweight transactions on one node: the condition is read and the
// update applied under a lock on the partition, so conditional statements
// on a partition are linearizable among themselves (what Paxos provides
// across replicas; StorageProxy.cas).

type condition struct {
	ifNotExists, ifExists bool
	conds                 []filter
	read                  *selectStmt
}

func prepareConditions(t *Table, ifExists bool, conds []cql.Condition, vs *vars) (*condition, error) {
	c := &condition{ifExists: ifExists}
	for _, cd := range conds {
		col := t.byName[cd.Column]
		if col == nil {
			return nil, invalidf("Undefined column name %s in table %s.%s", cd.Column, t.Keyspace, t.Name)
		}
		if col.Kind == PartitionKey || col.Kind == Clustering {
			return nil, invalidf("PRIMARY KEY column '%s' cannot have IF conditions", col.Name)
		}
		f := filter{col: col, op: cd.Op}
		if cd.Op == "IN" {
			vt, err := prepareValues(vs, col, cql.Relation{Columns: []string{cd.Column}, Op: "IN", Value: cd.Value})
			if err != nil {
				return nil, err
			}
			f.in = vt
		} else {
			tt, err := vs.prepareTerm(cd.Value, col.Name, col.Type)
			if err != nil {
				return nil, err
			}
			f.t = tt
		}
		c.conds = append(c.conds, f)
	}
	return c, nil
}

func (c *condition) metadata() []*message.ColumnMetadata {
	return []*message.ColumnMetadata{{Name: "[applied]", Type: types.BooleanType.DataType()}}
}

var casLocks [256]sync.Mutex
var casSeed = maphash.MakeSeed()

func (m *modification) executeConditional(e *execCtx) (message.Message, error) {
	return e.d.executeCAS([]boundModification{{m: m, e: e}}, e)
}

// casPartition returns the single partition a conditional statement
// updates.
func (m *modification) casPartition(e *execCtx) ([]byte, error) {
	t := m.t
	what := map[modKind]string{insertKind: "updates", updateKind: "updates", deleteKind: "deletions"}[m.kind]
	pks, err := m.where.partitionKeys(e, t)
	if err != nil {
		return nil, err
	}
	if len(pks) != 1 {
		return nil, invalidf("IN on the partition key is not supported with conditional %s", what)
	}
	if len(m.where.ck) > 0 && m.cond != nil {
		ranges, err := m.where.clusteringRanges(e, t)
		if err != nil {
			return nil, err
		}
		if len(ranges) != 1 || !ranges[0].point {
			return nil, invalidf("IN on the clustering key columns is not supported with conditional %s", what)
		}
	}
	return pks[0], nil
}

// executeCAS runs conditional statements on one partition (one statement,
// or a conditional batch): under the partition's lock, every condition is
// checked against the current rows, and the statements are applied only
// if all hold (ModificationStatement.executeWithCondition, BatchStatement
// with conditions).
func (d *DB) executeCAS(mods []boundModification, batchCtx *execCtx) (message.Message, error) {
	var pk []byte
	for i, bm := range mods {
		p, err := bm.m.casPartition(bm.e)
		if err != nil {
			return nil, err
		}
		if i > 0 && (bm.m.t != mods[0].m.t) {
			return nil, invalidf("Batch with conditions cannot span multiple tables")
		}
		if i > 0 && string(p) != string(pk) {
			return nil, invalidf("Batch with conditions cannot span multiple partitions")
		}
		pk = p
	}
	lock := &casLocks[maphash.Bytes(casSeed, pk)%uint64(len(casLocks))]
	lock.Lock()
	defer lock.Unlock()

	t := mods[0].m.t
	md := &message.RowsMetadata{Columns: (&condition{}).metadata()}
	out := [][]byte{{1}}
	for _, bm := range mods {
		m, e := bm.m, bm.e
		if m.cond == nil {
			continue
		}
		res, err := m.cond.read.execute(&execCtx{d: d, values: e.values, now: e.now, nowSec: e.nowSec})
		if err != nil {
			return nil, err
		}
		rows := res.(*message.RowsResult).Data
		var current map[*Column][]byte // the current row, nil if none
		if len(rows) > 0 {
			current = map[*Column][]byte{}
			for i, col := range t.AllColumns() {
				current[col] = rows[0][i]
			}
		}
		applied := true
		switch {
		case m.cond.ifNotExists:
			applied = current == nil
		case m.cond.ifExists:
			applied = current != nil
		default:
			for _, f := range m.cond.conds {
				ok, err := f.matchesCAS(e, current)
				if err != nil {
					return nil, err
				}
				if !ok {
					applied = false
					break
				}
			}
		}
		if applied {
			continue
		}
		out[0][0] = 0
		if current != nil {
			cols := t.AllColumns()
			if !m.cond.ifNotExists && !m.cond.ifExists {
				cols = nil
				for _, f := range m.cond.conds {
					cols = append(cols, f.col)
				}
			}
			for _, col := range cols {
				md.Columns = append(md.Columns, &message.ColumnMetadata{Name: col.Name, Type: col.Type.DataType()})
				out = append(out, current[col])
			}
		}
		break
	}
	for i, c := range md.Columns {
		c.Keyspace, c.Table, c.Index = t.Keyspace, t.Name, int32(i)
	}
	md.ColumnCount = int32(len(md.Columns))
	if out[0][0] == 1 {
		if err := d.applyModifications(mods, batchCtx); err != nil {
			return nil, err
		}
	}
	return &message.RowsResult{Metadata: md, Data: message.RowSet{out}}, nil
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// matchesCAS evaluates a column condition (ColumnCondition): against a
// missing row every value is null.
func (f *filter) matchesCAS(e *execCtx, r map[*Column][]byte) (bool, error) {
	v := r[f.col]
	if f.op == "IN" {
		vals, err := f.in.values(e)
		if err != nil {
			return false, err
		}
		for _, x := range vals {
			if v != nil && types.Compare(f.col.Type, v, x) == 0 {
				return true, nil
			}
		}
		return false, nil
	}
	x, st, err := f.t.bind(e)
	if err != nil {
		return false, err
	}
	if st != vSet || v == nil {
		isNull := st != vSet
		switch f.op {
		case "=":
			return isNull == (v == nil), nil
		case "!=":
			return isNull != (v == nil), nil
		}
		if isNull {
			return false, invalidf("Invalid comparison with null for operator \"%s\"", f.op)
		}
		return false, nil
	}
	c := types.Compare(f.col.Type, v, x)
	switch f.op {
	case "=":
		return c == 0, nil
	case "!=":
		return c != 0, nil
	case "<":
		return c < 0, nil
	case "<=":
		return c <= 0, nil
	case ">":
		return c > 0, nil
	case ">=":
		return c >= 0, nil
	}
	return false, invalidf("Unsupported operator %s in condition", f.op)
}
