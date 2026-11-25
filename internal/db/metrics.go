package db

import (
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/pbonard/cassandra-room/internal/cluster"
)

// Metrics in the Prometheus format (Cassandra's metrics, as the metrics
// collectors export them): client requests' latency and errors as they
// happen; storage, hints, messaging, the cluster and Accord read when
// scraped.

const ns = "cassandra_room"

type dbMetrics struct {
	reg     *prometheus.Registry
	latency *prometheus.HistogramVec
	errors  *prometheus.CounterVec
}

func newMetrics(d *DB) *dbMetrics {
	m := &dbMetrics{reg: prometheus.NewRegistry()}
	m.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: "client_request_duration_seconds",
		Help:    "Client requests' latency, by kind (read, write, cas, txn, schema, other) and consistency level (ClientRequestMetrics).",
		Buckets: []float64{.0005, .001, .002, .005, .01, .02, .05, .1, .2, .5, 1, 2, 5, 10}}, []string{"request", "consistency"})
	m.errors = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: "client_request_errors_total",
		Help: "Client requests that failed, by kind and error (timeout, unavailable, overloaded, invalid, unauthorized, other)."},
		[]string{"request", "error"})
	m.reg.MustRegister(m.latency, m.errors, &collector{d: d},
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// Registry is the node's metrics, for a /metrics endpoint.
func (d *DB) Registry() *prometheus.Registry { return d.metrics.reg }

// observe records a request's outcome.
func (m *dbMetrics) observe(kind string, cl primitive.ConsistencyLevel, res message.Message, took time.Duration) {
	if m == nil {
		return
	}
	m.latency.WithLabelValues(kind, strings.ToLower(clName(cl))).Observe(took.Seconds())
	if e, ok := res.(message.Error); ok {
		m.errors.WithLabelValues(kind, errorKind(e.GetErrorCode())).Inc()
	}
}

func errorKind(code primitive.ErrorCode) string {
	switch code {
	case primitive.ErrorCodeReadTimeout, primitive.ErrorCodeWriteTimeout:
		return "timeout"
	case primitive.ErrorCodeUnavailable:
		return "unavailable"
	case primitive.ErrorCodeOverloaded:
		return "overloaded"
	case primitive.ErrorCodeInvalid, primitive.ErrorCodeSyntaxError, primitive.ErrorCodeAlreadyExists, primitive.ErrorCodeUnprepared:
		return "invalid"
	case primitive.ErrorCodeUnauthorized, primitive.ErrorCodeAuthenticationError:
		return "unauthorized"
	}
	return "other"
}

// requestKind classifies a statement as ClientRequestMetrics does.
func requestKind(s statement) string {
	switch st := s.(type) {
	case *selectStmt:
		return "read"
	case *modification:
		if st.cond != nil {
			return "cas"
		}
		return "write"
	case *batchStmt:
		return "write"
	case *txnStmt:
		return "txn"
	case *ddlStmt:
		return "schema"
	}
	return "other"
}

// collector reads the node's state when scraped.
type collector struct{ d *DB }

// Describe describes nothing: an unchecked collector, whose metrics depend
// on the tables and nodes when scraped.
func (c *collector) Describe(chan<- *prometheus.Desc) {}

func desc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc(ns+"_"+name, help, labels, nil)
}

var (
	tableBytes    = desc("table_live_bytes", "A table's data on this node, sstables and memtables.", "keyspace", "table")
	tableMemBytes = desc("table_memtable_bytes", "A table's memtables on this node.", "keyspace", "table")
	tableSSTables = desc("table_sstables", "A table's sstables on this node.", "keyspace", "table")
	tableRowsDesc = desc("table_estimated_rows", "A table's estimated rows (row fragments) on this node.", "keyspace", "table")
	flushes       = desc("flushes_total", "Memtable flushes.")
	compactions   = desc("compactions_total", "Compactions.")
	flushed       = desc("flushed_bytes_total", "Bytes written by flushes.")
	compacted     = desc("compacted_bytes_total", "Bytes written by compactions.")
	writeAmp      = desc("write_amplification", "(flushed + compacted) / flushed bytes.")
	hintsBytes    = desc("hints_bytes", "Hints stored on this node for nodes that missed writes.")
	nodes         = desc("cluster_nodes", "Nodes of the cluster by data center, status and liveness, as this node sees them.", "datacenter", "status", "up")
	epoch         = desc("cluster_metadata_epoch", "The cluster metadata's epoch on this node.")
	dropped       = desc("messages_dropped_total", "Internode requests dropped because they expired, by verb.", "verb")
	overloaded    = desc("messages_overloaded_total", "Internode requests refused for exceeding the queue limits.")
	forwards      = desc("messages_forwarded_total", "Writes sent to another data center for one of its nodes to forward.")
	relayed       = desc("messages_relayed_total", "Writes this node forwarded for another data center's coordinator.")
	stageActive   = desc("thread_pool_active_tasks", "Tasks running in a stage.", "pool")
	stageLimit    = desc("thread_pool_max_tasks", "The stage's workers.", "pool")
	stageDropped  = desc("thread_pool_dropped_total", "Requests the stage dropped.", "pool")
)

func gauge(ch chan<- prometheus.Metric, d *prometheus.Desc, v float64, labels ...string) {
	ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
}

func counter(ch chan<- prometheus.Metric, d *prometheus.Desc, v float64, labels ...string) {
	ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, labels...)
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	d := c.d
	for _, t := range d.userTables() {
		lt, err := d.engine.Table(t.Storage)
		if err != nil {
			continue
		}
		s := lt.Size()
		gauge(ch, tableBytes, float64(s.Bytes), t.Keyspace, t.Name)
		gauge(ch, tableMemBytes, float64(s.MemtableBytes), t.Keyspace, t.Name)
		gauge(ch, tableSSTables, float64(s.SSTables), t.Keyspace, t.Name)
		gauge(ch, tableRowsDesc, float64(s.Rows), t.Keyspace, t.Name)
	}
	em := d.engine.Metrics()
	counter(ch, flushes, float64(em.Flushes))
	counter(ch, compactions, float64(em.Compactions))
	counter(ch, flushed, float64(em.BytesFlushed))
	counter(ch, compacted, float64(em.BytesCompacted))
	gauge(ch, writeAmp, em.WriteAmplification)
	s := d.schema.Load()
	gauge(ch, epoch, float64(s.Epoch))
	p := d.proxy
	if p == nil {
		return
	}
	if lt, err := d.engine.Table(hintsStorage); err == nil {
		gauge(ch, hintsBytes, float64(lt.Size().Bytes))
	}
	type key struct{ dc, status, up string }
	count := map[key]int{}
	for _, n := range s.Nodes {
		if n.Status == cluster.StatusLeft {
			continue
		}
		up := "false"
		if n.Addr == p.self || p.g.IsAlive(n.Addr) {
			up = "true"
		}
		count[key{n.DataCenter, n.Status, up}]++
	}
	for k, n := range count {
		gauge(ch, nodes, float64(n), k.dc, k.status, k.up)
	}
	ms := p.m.Stats()
	for v, n := range ms.Dropped {
		counter(ch, dropped, float64(n), v.String())
	}
	counter(ch, overloaded, float64(ms.Overloaded))
	counter(ch, forwards, float64(ms.Forwards))
	counter(ch, relayed, float64(ms.Relayed))
	for _, st := range p.m.Stages() {
		gauge(ch, stageActive, float64(st.Active), st.Name)
		gauge(ch, stageLimit, float64(st.Limit), st.Name)
		counter(ch, stageDropped, float64(st.Dropped), st.Name)
	}
	if p.accord != nil {
		collectStruct(ch, "accord_", p.accord.Stats())
	}
}

// collectStruct exports a stats struct's integer fields: counters (named
// _total) for those ending in a past participle or plural, gauges for the
// others (sizes, epochs).
func collectStruct(ch chan<- prometheus.Metric, prefix string, v any) {
	rv := reflect.ValueOf(v)
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		f := rv.Field(i)
		var x float64
		switch f.Kind() {
		case reflect.Int, reflect.Int64, reflect.Int32:
			x = float64(f.Int())
		default:
			continue
		}
		name := prefix + snake(rt.Field(i).Name)
		if gaugeFields[rt.Field(i).Name] {
			gauge(ch, desc(name, "Accord: "+rt.Field(i).Name+"."), x)
		} else {
			counter(ch, desc(name+"_total", "Accord: "+rt.Field(i).Name+"."), x)
		}
	}
}

// gaugeFields are the stats that are levels, not counts of events.
var gaugeFields = map[string]bool{"Commands": true, "Keys": true, "Barriers": true, "Redundant": true, "Epoch": true, "SyncedEpoch": true}

func snake(s string) string {
	var b strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 && !unicode.IsUpper(rune(s[i-1])) {
				b.WriteByte('_')
			}
			r = unicode.ToLower(r)
		}
		b.WriteRune(r)
	}
	return b.String()
}
