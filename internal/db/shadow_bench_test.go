package db

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkReplicaReads measures a replica's answers to range reads of a
// clustered table without deletions, forward over many partitions (a
// token range scan's page) and reverse over one partition, and to a range
// read of a table without clustering columns.
func BenchmarkReplicaReads(b *testing.B) {
	c := newTestCluster(b, 1)
	c.ok(0, quorum, "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
	c.ok(0, quorum, "CREATE TABLE ks.t (pk int, ck int, v text, PRIMARY KEY (pk, ck))")
	c.ok(0, quorum, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	time.Sleep(50 * time.Millisecond)
	for pk := 0; pk < 100; pk++ {
		for ck := 0; ck < 20; ck++ {
			c.ok(0, one, fmt.Sprintf("INSERT INTO ks.t (pk, ck, v) VALUES (%d, %d, 'value')", pk, ck))
		}
		c.ok(0, one, fmt.Sprintf("INSERT INTO ks.kv (k, v) VALUES (%d, 'value')", pk))
	}
	d := c.nodes[0]
	if err := d.Flush(); err != nil {
		b.Fatal(err)
	}
	p := d.proxy
	t, _ := d.schema.Load().table("ks", "t")
	kv, _ := d.schema.Load().table("ks", "kv")
	p1 := partitionPrefix([]byte{0, 0, 0, 1})
	run := func(b *testing.B, h func(context.Context, string, []byte) ([]byte, error), payload []byte) {
		b.ReportAllocs()
		for range b.N {
			if _, err := h(context.Background(), "", payload); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.Run("forward-clustered", func(b *testing.B) {
		run(b, p.handleReadRange, fenced(0, encodeKey(t.Storage, nil, nil)))
	})
	b.Run("reverse-partition", func(b *testing.B) {
		run(b, p.handleReadRangeReverse, fenced(0, encodeKey(t.Storage, append(p1, 0x20), prefixEnd(p1))))
	})
	b.Run("forward-unclustered", func(b *testing.B) {
		run(b, p.handleReadRange, fenced(0, encodeKey(kv.Storage, nil, nil)))
	})
}
