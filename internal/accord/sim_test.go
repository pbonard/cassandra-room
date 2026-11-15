package accord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The simulation runs a cluster of nodes over an in-memory network that
// delays, drops and partitions messages, crashes and restarts nodes (from
// their journals), and checks a list-append workload (as accord's burn
// test): every transaction reads some keys and appends a unique value to
// others; afterwards every replica of a key must hold the same list, in
// execution order, and every acknowledged transaction must have read
// exactly the values appended by transactions executing before it, and
// executed after every transaction acknowledged before it started (strict
// serializability).

type simNet struct {
	mu      sync.Mutex
	nodes   map[NodeID]*Node
	down    map[NodeID]bool
	cut     map[[2]NodeID]bool
	drop    float64
	maxWait time.Duration
}

type simTransport struct {
	net  *simNet
	from NodeID
}

func (t *simTransport) Request(ctx context.Context, to NodeID, payload []byte) ([]byte, error) {
	nt := t.net
	nt.mu.Lock()
	n := nt.nodes[to]
	fail := nt.down[to] || nt.down[t.from] || nt.cut[[2]NodeID{t.from, to}] || nt.cut[[2]NodeID{to, t.from}] || rand.Float64() < nt.drop
	wait := time.Duration(rand.Int64N(int64(nt.maxWait) + 1))
	nt.mu.Unlock()
	if fail || n == nil {
		select {
		case <-time.After(wait + 5*time.Millisecond):
		case <-ctx.Done():
		}
		return nil, errors.New("sim: unreachable")
	}
	time.Sleep(wait)
	out, err := n.Handle(ctx, t.from, payload)
	if err != nil {
		return nil, err
	}
	nt.mu.Lock()
	lost := nt.down[to] || rand.Float64() < nt.drop
	nt.mu.Unlock()
	if lost {
		return nil, errors.New("sim: reply lost")
	}
	return out, nil
}

// listStore is a node's data: per key, the appended values with the
// execution timestamps that appended them.
type listStore struct {
	mu   sync.Mutex
	data map[string][]entry
}

type entry struct {
	At    Timestamp
	Value int
}

type listUpdate struct {
	Reads   []Key
	Appends map[string]int
}

func (s *listStore) Read(k Key, _ Timestamp, _ *Txn) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var vals []int
	for _, e := range s.data[keyString(k)] {
		vals = append(vals, e.Value)
	}
	return json.Marshal(vals)
}

func (s *listStore) Apply(k Key, at Timestamp, _ *Txn, w []byte) error {
	var v int
	if err := json.Unmarshal(w, &v); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ks := keyString(k)
	for _, e := range s.data[ks] {
		if e.At == at {
			return nil
		}
	}
	if l := s.data[ks]; len(l) > 0 && !l[len(l)-1].At.Less(at) {
		panic(fmt.Sprintf("out of order apply on %s: %v after %v", ks, at, l[len(l)-1].At))
	}
	s.data[ks] = append(s.data[ks], entry{at, v})
	return nil
}

type listAgent struct{}

type listResult struct {
	ExecuteAt Timestamp
	Reads     map[string][]int
}

func (listAgent) Execute(txn *Txn, executeAt Timestamp, reads map[string][]byte) (Writes, []byte, error) {
	var u listUpdate
	if err := json.Unmarshal(txn.Update, &u); err != nil {
		return nil, nil, err
	}
	res := listResult{ExecuteAt: executeAt, Reads: map[string][]int{}}
	for _, k := range u.Reads {
		var vals []int
		if err := json.Unmarshal(reads[keyString(k)], &vals); err != nil {
			return nil, nil, err
		}
		res.Reads[keyString(k)] = vals
	}
	w := Writes{}
	for k, v := range u.Appends {
		w[k], _ = json.Marshal(v)
	}
	b, err := json.Marshal(res)
	return w, b, err
}

type simCluster struct {
	t       *testing.T
	net     *simNet
	ids     []NodeID
	stores  map[NodeID]*listStore
	jours   map[NodeID]*MemJournal
	keys    []Key
	rf      int
	cfgBase Config
}

func newSimCluster(t *testing.T, nodes, rf, keys int) *simCluster {
	c := &simCluster{t: t, net: &simNet{nodes: map[NodeID]*Node{}, down: map[NodeID]bool{}, cut: map[[2]NodeID]bool{}, maxWait: time.Millisecond},
		stores: map[NodeID]*listStore{}, jours: map[NodeID]*MemJournal{}, rf: rf}
	for i := 0; i < nodes; i++ {
		id := fmt.Sprintf("n%d", i)
		c.ids = append(c.ids, id)
		c.stores[id] = &listStore{data: map[string][]entry{}}
		c.jours[id] = &MemJournal{}
	}
	for i := 0; i < keys; i++ {
		c.keys = append(c.keys, Key{Token: int64(i), ID: fmt.Sprintf("k%d", i)})
	}
	for i := range c.ids {
		c.start(i)
	}
	t.Cleanup(func() {
		for _, n := range c.net.nodes {
			n.Stop()
		}
	})
	return c
}

func (c *simCluster) replicas(k Key) []NodeID {
	var out []NodeID
	for i := 0; i < c.rf; i++ {
		out = append(out, c.ids[(int(k.Token)+i)%len(c.ids)])
	}
	return out
}

func (c *simCluster) start(i int) {
	id := c.ids[i]
	c.net.mu.Lock()
	running := c.net.nodes[id] != nil
	c.net.mu.Unlock()
	if running {
		return
	}
	n, err := NewNode(Config{ID: id, Tag: i, Transport: &simTransport{net: c.net, from: id}, Data: c.stores[id], Agent: listAgent{},
		Journal: c.jours[id], ProgressInterval: 20 * time.Millisecond, RecoverAfter: 500 * time.Millisecond,
		RequestTimeout: time.Second, RetainApplied: time.Hour})
	require.NoError(c.t, err)
	n.SetTopology(&Topology{Epoch: 1, Replicas: c.replicas, Nodes: c.ids})
	n.Start()
	c.net.mu.Lock()
	c.net.nodes[id] = n
	delete(c.net.down, id)
	c.net.mu.Unlock()
}

// crash stops a node; its journal and data survive.
func (c *simCluster) crash(i int) {
	id := c.ids[i]
	c.net.mu.Lock()
	n := c.net.nodes[id]
	c.net.down[id] = true
	delete(c.net.nodes, id)
	c.net.mu.Unlock()
	if n != nil {
		n.Stop()
	}
}

type opRecord struct {
	start, end time.Time
	appends    map[string]int
	keys       int
	res        *listResult
	err        error
}

// run executes ops transactions from clients concurrent clients.
func (c *simCluster) run(clients, ops int, chaos func(step int)) []*opRecord {
	var mu sync.Mutex
	var records []*opRecord
	var next atomic.Int64
	var wg sync.WaitGroup
	stopChaos := make(chan struct{})
	if chaos != nil {
		go func() {
			for step := 0; ; step++ {
				select {
				case <-stopChaos:
					return
				case <-time.After(50 * time.Millisecond):
					chaos(step)
				}
			}
		}()
	}
	for cl := 0; cl < clients; cl++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				v := int(next.Add(1))
				if v > ops {
					return
				}
				r := rand.New(rand.NewPCG(uint64(v), 7))
				u := listUpdate{Appends: map[string]int{}}
				nk := 1 + r.IntN(3)
				var keys []Key
				for _, i := range r.Perm(len(c.keys))[:nk] {
					keys = append(keys, c.keys[i])
				}
				slices.SortFunc(keys, Key.Compare)
				kind := Write
				for i, k := range keys {
					if i == 0 || r.IntN(2) == 0 {
						u.Reads = append(u.Reads, k)
					}
					if r.IntN(3) != 0 {
						u.Appends[keyString(k)] = v
					}
				}
				if len(u.Appends) == 0 {
					kind = Read
				}
				b, _ := json.Marshal(u)
				c.net.mu.Lock()
				var live []*Node
				for _, id := range c.ids {
					if n := c.net.nodes[id]; n != nil {
						live = append(live, n)
					}
				}
				c.net.mu.Unlock()
				coord := live[r.IntN(len(live))]
				rec := &opRecord{start: time.Now(), appends: u.Appends, keys: len(keys)}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				out, err := coord.Coordinate(ctx, &Txn{Kind: kind, Keys: keys, Update: b})
				cancel()
				rec.end, rec.err = time.Now(), err
				if err == nil {
					rec.res = &listResult{}
					require.NoError(c.t, json.Unmarshal(out, rec.res))
				}
				mu.Lock()
				records = append(records, rec)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(stopChaos)
	return records
}

// settle heals the network, restarts every node and waits for every
// replica to apply what was decided.
func (c *simCluster) settle() {
	c.net.mu.Lock()
	c.net.drop = 0
	c.net.cut = map[[2]NodeID]bool{}
	c.net.mu.Unlock()
	for i, id := range c.ids {
		c.net.mu.Lock()
		down := c.net.nodes[id] == nil
		c.net.mu.Unlock()
		if down {
			c.start(i)
		}
	}
	stuck := func(log bool) bool {
		found := false
		for id, n := range c.net.nodes {
			n.s.mu.Lock()
			for _, cmd := range n.s.cmds {
				if cmd.Status != Applied && cmd.Status != Invalidated && cmd.Status != Truncated && (cmd.Status != NotDefined || len(cmd.Keys) > 0) {
					found = true
					if !log {
						break
					}
					var blocked any
					if cmd.blockedOn != nil {
						blocked = *cmd.blockedOn
						if d := n.s.cmds[*cmd.blockedOn]; d != nil {
							blocked = fmt.Sprintf("%v (%s exec=%v)", *cmd.blockedOn, d.Status, d.ExecuteAt)
						}
					}
					c.t.Logf("%s: %v %s exec=%v promised=%v ready=%v blockedOn=%v keys=%v", id, cmd.ID, cmd.Status, cmd.ExecuteAt, cmd.Promised, cmd.ready, blocked, cmd.Keys)
				}
			}
			n.s.mu.Unlock()
		}
		return found
	}
	deadline := time.Now().Add(60 * time.Second)
	for stuck(false) {
		if time.Now().After(deadline) {
			stuck(true)
			c.t.Fatal("commands neither applied nor invalidated")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// check verifies the replicas and the acknowledged results.
func (c *simCluster) check(records []*opRecord) {
	t := c.t
	final := map[string][]entry{}
	for _, k := range c.keys {
		ks := keyString(k)
		var ref []entry
		for i, r := range c.replicas(k) {
			l := c.stores[r].data[ks]
			if i == 0 {
				ref = l
				continue
			}
			if !slices.Equal(ref, l) {
				for _, e := range append(slices.Clone(ref), l...) {
					if !slices.Contains(ref, e) || !slices.Contains(l, e) {
						c.explain(ks, e, Timestamp{})
					}
				}
			}
			require.Equal(t, ref, l, "replicas of %s agree", ks)
		}
		final[ks] = ref
	}
	byValue := map[int]Timestamp{}
	for ks, l := range final {
		for _, e := range l {
			if at, dup := byValue[e.Value]; dup && at != e.At {
				t.Fatalf("value %d appended at two timestamps (%v, %v) on %s", e.Value, at, e.At, ks)
			}
			byValue[e.Value] = e.At
		}
	}
	acked, failed := 0, 0
	var done []*opRecord
	for _, r := range records {
		if r.err != nil {
			failed++
			if errors.Is(r.err, ErrInvalidated) {
				for ks, v := range r.appends {
					for _, e := range final[ks] {
						require.NotEqual(t, v, e.Value, "an invalidated transaction's write was applied")
					}
				}
			}
			continue
		}
		acked++
		if len(r.appends) == 0 && r.keys == 1 {
			// An ephemeral read: a prefix of the key's history that holds
			// every append acknowledged before it started.
			for ks, vals := range r.res.Reads {
				var hist []int
				for _, e := range final[ks] {
					hist = append(hist, e.Value)
				}
				require.LessOrEqual(t, len(vals), len(hist))
				require.Equal(t, hist[:len(vals)], vals, "an ephemeral read of %s sees a prefix: %v of %v", ks, vals, hist)
				for _, o := range records {
					if o.err == nil && o.end.Before(r.start) {
						if v, ok := o.appends[ks]; ok {
							require.Contains(t, vals, v, "an ephemeral read sees the appends acknowledged before it")
						}
					}
				}
			}
			continue
		}
		done = append(done, r)
		at := r.res.ExecuteAt
		for ks, v := range r.appends {
			n := 0
			for _, e := range final[ks] {
				if e.Value == v {
					n++
					require.Equal(t, at, e.At)
				}
			}
			require.Equal(t, 1, n, "acknowledged append %d to %s applied once", v, ks)
		}
		for ks, vals := range r.res.Reads {
			var want []int
			for _, e := range final[ks] {
				if e.At.Less(at) {
					want = append(want, e.Value)
				}
			}
			if !slices.Equal(want, vals) {
				for _, e := range final[ks] {
					if e.At.Less(at) && !slices.Contains(vals, e.Value) {
						c.explain(ks, e, at)
					}
				}
			}
			require.Equal(t, want, vals, "read of %s at %v sees exactly the earlier appends", ks, at)
		}
	}
	// Real time: a transaction acknowledged before another started
	// executes before it.
	sort.Slice(done, func(i, j int) bool { return done[i].end.Before(done[j].end) })
	for i, a := range done {
		for _, b := range done[i+1:] {
			if a.end.Before(b.start) {
				require.True(t, a.res.ExecuteAt.Less(b.res.ExecuteAt), "strict serializability: %v before %v", a.res.ExecuteAt, b.res.ExecuteAt)
			}
		}
	}
	t.Logf("%d acknowledged, %d failed", acked, failed)
	require.Greater(t, acked, len(records)/5)
}

func TestSimNoFaults(t *testing.T) {
	c := newSimCluster(t, 5, 3, 8)
	records := c.run(8, 400, nil)
	c.settle()
	c.check(records)
	failed := 0
	for _, r := range records {
		if r.err != nil {
			failed++
		}
	}
	// Without faults only an overloaded machine (the race detector, every
	// package's tests at once) makes requests time out.
	require.Less(t, failed, len(records)/10)
}

func TestSimDrops(t *testing.T) {
	c := newSimCluster(t, 5, 3, 6)
	c.net.drop = 0.05
	records := c.run(8, 300, nil)
	c.settle()
	c.check(records)
}

func TestSimCrashes(t *testing.T) {
	c := newSimCluster(t, 5, 3, 6)
	c.net.drop = 0.02
	records := c.run(8, 300, func(step int) {
		i := (step / 4) % len(c.ids)
		switch step % 4 {
		case 0:
			c.crash(i)
		case 2:
			c.start(i)
		}
	})
	c.settle()
	c.check(records)
}

func TestSimPartitions(t *testing.T) {
	c := newSimCluster(t, 5, 3, 6)
	records := c.run(8, 300, func(step int) {
		c.net.mu.Lock()
		defer c.net.mu.Unlock()
		c.net.cut = map[[2]NodeID]bool{}
		if step%3 == 2 {
			return
		}
		a, b := c.ids[rand.IntN(len(c.ids))], c.ids[rand.IntN(len(c.ids))]
		c.net.cut[[2]NodeID{a, b}] = true
	})
	c.settle()
	c.check(records)
}

// explain logs what the replicas know of the transaction that appended e
// and of the reader at at.
func (c *simCluster) explain(ks string, e entry, at Timestamp) {
	for _, id := range c.ids {
		n := c.net.nodes[id]
		if n == nil {
			continue
		}
		n.s.mu.Lock()
		for _, cmd := range n.s.cmds {
			if cmd.ExecuteAt == e.At || cmd.ExecuteAt == at {
				role := "writer"
				if cmd.ExecuteAt == at {
					role = "reader"
				}
				has := false
				for _, x := range c.stores[id].data[ks] {
					has = has || x.At == cmd.ExecuteAt
				}
				c.t.Logf("%s %s: %s id=%v exec=%v owned=%v writes=%v inStore=%v replicas=%v ndeps=%d", id, role, cmd.Status, cmd.ID, cmd.ExecuteAt, cmd.owned, cmd.Writes, has, c.replicas(cmd.Keys[0]), len(cmd.Deps[ks]))
			}
		}
		n.s.mu.Unlock()
	}
}
