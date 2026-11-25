package db

import (
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"
)

// forwarder relays TCP connections from its address to target, as a NAT
// or load balancer in front of a node does, counting them.
type forwarder struct {
	ln    net.Listener
	conns atomic.Int64
	wg    sync.WaitGroup
}

func newForwarder(t *testing.T, target string) *forwarder {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f := &forwarder{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			f.conns.Add(1)
			go func() {
				defer c.Close()
				t, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer t.Close()
				go func() { _, _ = io.Copy(t, c); t.Close() }()
				_, _ = io.Copy(c, t)
			}()
		}
	}()
	return f
}

func (f *forwarder) addr() string { return f.ln.Addr().String() }

// TestClusterBroadcastAddress: nodes known by addresses other than the ones
// they listen on (broadcast_address, here forwarders as NAT), connecting to
// nodes of their own data center at their listen addresses (prefer_local).
func TestClusterBroadcastAddress(t *testing.T) {
	dcs := []string{"dc1", "dc1", "dc2"}
	var listens []string
	var fwds []*forwarder
	for range dcs {
		l := freeAddr(t)
		listens = append(listens, l)
		fwds = append(fwds, newForwarder(t, l))
	}
	t.Cleanup(func() {
		for _, f := range fwds {
			f.ln.Close()
		}
	})
	c := &testCluster{t: t, dcs: dcs}
	for i, dc := range dcs {
		co := &ClusterOptions{ListenAddress: listens[i], BroadcastAddress: fwds[i].addr(), PreferLocal: true,
			Seeds: []string{fwds[0].addr()}, NumTokens: 8, Membership: testMembership(),
			WriteTimeout: time.Second, ReadTimeout: time.Second}
		d, err := Open("db", Options{FS: vfs.NewMem(), Address: net.IPv4(127, 0, 0, byte(1+i)), DataCenter: dc, Cluster: co})
		require.NoError(t, err)
		t.Cleanup(func() { d.Close() })
		c.nodes = append(c.nodes, d)
		c.addrs = append(c.addrs, fwds[i].addr())
	}
	c.waitConverged()
	for i, d := range c.nodes {
		require.Equal(t, fwds[i].addr(), d.proxy.self, "known by its broadcast address")
	}
	all := primitive.ConsistencyLevelAll
	c.ok(0, all, "CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1}")
	c.ok(0, all, "CREATE TABLE ks.kv (k int PRIMARY KEY, v text)")
	for i := 0; i < 10; i++ {
		c.ok(i%3, all, "INSERT INTO ks.kv (k, v) VALUES (?, 'x')", i)
	}
	require.Len(t, c.values(2, all, "SELECT k FROM ks.kv"), 10)

	// Connections within dc1 go to listen addresses; to and from dc2,
	// to broadcast addresses.
	p0 := c.nodes[0].proxy
	require.Equal(t, listens[1], p0.dialAddress(fwds[1].addr()))
	require.Equal(t, fwds[2].addr(), p0.dialAddress(fwds[2].addr()))
	require.Equal(t, fwds[0].addr(), c.nodes[2].proxy.dialAddress(fwds[0].addr()))

	// system.local and system.peers show both addresses.
	local := addrRows(t, c.nodes[0], "SELECT listen_address, listen_port, broadcast_address, broadcast_port, rpc_address FROM system.local")
	require.Equal(t, []string{"127.0.0.1", port(listens[0]), "127.0.0.1", port(fwds[0].addr()), "127.0.0.1"}, local[0])
	peers := addrRows(t, c.nodes[0], "SELECT peer_port, preferred_port, native_address FROM system.peers_v2")
	require.ElementsMatch(t, [][]string{
		{port(fwds[1].addr()), port(listens[1]), "127.0.0.2"}, // dc1: its listen address
		{port(fwds[2].addr()), port(fwds[2].addr()), "127.0.0.3"},
	}, peers)
}

func port(addr string) string {
	_, p, _ := net.SplitHostPort(addr)
	return p
}

// rowsOf runs a query on a node and renders its rows as text.
func addrRows(t *testing.T, d *DB, q string) [][]string {
	res := d.Execute(mustPrepare(t, d, q), &RequestOptions{Consistency: primitive.ConsistencyLevelOne, local: true})
	rows, ok := res.(*message.RowsResult)
	require.True(t, ok, "%v", res)
	var out [][]string
	for _, r := range rows.Data {
		var row []string
		for i, v := range r {
			switch rows.Metadata.Columns[i].Type.AsCql() {
			case "inet":
				row = append(row, net.IP(v).String())
			case "int":
				row = append(row, strconv.Itoa(int(int32(uint32(v[0])<<24|uint32(v[1])<<16|uint32(v[2])<<8|uint32(v[3])))))
			default:
				row = append(row, string(v))
			}
		}
		out = append(out, row)
	}
	return out
}
