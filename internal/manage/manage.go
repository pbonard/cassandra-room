// Package manage is a node's management API (Cassandra's Management API
// for Apache Cassandra, and the nodetool commands an operator needs), over
// HTTP with JSON bodies:
//
//	GET    /api/v1/health          liveness: the process answers
//	GET    /api/v1/ready           readiness: in the ring and serving clients
//	GET    /api/v1/node            this node (db.NodeInfo)
//	GET    /api/v1/nodes           every node of the cluster
//	GET    /api/v1/ring?keyspace=  ranges with their replicas (describering)
//	GET    /api/v1/keyspaces       the user keyspaces
//	POST   /api/v1/flush           flush every memtable
//	POST   /api/v1/decommission    leave the ring (asynchronous: see /node)
//	POST   /api/v1/cleanup?keyspace=
//	POST   /api/v1/removenode?node=<address or host id>
//	POST   /api/v1/cms?members=a,b reconfigure the metadata service
//	POST   /api/v1/repair          {"keyspace","table","start","end"}: a range
//	                               (start, end] when given, else everything
//	POST   /api/v1/snapshots       {"name","keyspaces"}: the snapshot's files
//	DELETE /api/v1/snapshots/{name}
//	POST   /api/v1/import          {"keyspace","table","files"}
//
// Errors are {"error": "..."} with a 4xx or 5xx status. The text endpoints
// of earlier versions (GET /status, POST /repair, /cleanup, /removenode,
// /cms/reconfigure, /decommission) remain.
package manage

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/pbonard/cassandra-room/internal/db"
)

// Handler serves the management API of a node; serving reports whether the
// node accepts client connections (for readiness).
func Handler(d *db.DB, serving func() bool) http.Handler {
	h := &handler{d: d, serving: serving}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/health", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /api/v1/ready", h.ready)
	mux.HandleFunc("GET /api/v1/node", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, d.LocalNode()) })
	mux.HandleFunc("GET /api/v1/nodes", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, d.Nodes()) })
	mux.HandleFunc("GET /api/v1/ring", h.ring)
	mux.HandleFunc("GET /api/v1/keyspaces", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, d.Keyspaces()) })
	mux.HandleFunc("POST /api/v1/flush", func(w http.ResponseWriter, _ *http.Request) { h.done(w, d.Flush()) })
	mux.HandleFunc("POST /api/v1/decommission", h.decommission)
	mux.HandleFunc("POST /api/v1/cleanup", func(w http.ResponseWriter, r *http.Request) {
		n, err := d.Cleanup(r.URL.Query().Get("keyspace"))
		h.result(w, map[string]int{"tables": n}, err)
	})
	mux.HandleFunc("POST /api/v1/removenode", func(w http.ResponseWriter, r *http.Request) {
		h.done(w, d.RemoveNode(r.URL.Query().Get("node")))
	})
	mux.HandleFunc("POST /api/v1/cms", func(w http.ResponseWriter, r *http.Request) {
		h.done(w, d.ReconfigureCMS(splitList(r.URL.Query().Get("members"))))
	})
	mux.HandleFunc("POST /api/v1/repair", h.repair)
	mux.HandleFunc("POST /api/v1/snapshots", h.snapshot)
	mux.HandleFunc("DELETE /api/v1/snapshots/{name}", func(w http.ResponseWriter, r *http.Request) {
		h.done(w, d.ClearSnapshot(r.PathValue("name")))
	})
	mux.HandleFunc("POST /api/v1/import", h.importFiles)
	legacy(mux, d)
	return mux
}

type handler struct {
	d       *db.DB
	serving func() bool

	mu             sync.Mutex
	decommissioned error // the result of a finished decommission
	decommissionOn bool
}

func (h *handler) ready(w http.ResponseWriter, _ *http.Request) {
	if !h.d.Ready() || h.serving != nil && !h.serving() {
		writeError(w, http.StatusServiceUnavailable, errors.New("not ready"))
		return
	}
	writeJSON(w, map[string]string{"status": "ready"})
}

func (h *handler) ring(w http.ResponseWriter, r *http.Request) {
	ranges, err := h.d.Ring(r.URL.Query().Get("keyspace"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, ranges)
}

// decommission starts leaving the ring and answers at once (it streams the
// node's data away first, which takes long); a second call reports the
// outcome of the first.
func (h *handler) decommission(w http.ResponseWriter, _ *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.decommissionOn {
		w.WriteHeader(http.StatusAccepted)
		writeJSON(w, map[string]string{"status": "decommissioning"})
		return
	}
	if h.decommissioned != nil {
		err := h.decommissioned
		h.decommissioned = nil
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	h.decommissionOn = true
	go func() {
		err := h.d.Decommission()
		if err != nil {
			log.Printf("decommission: %v", err)
		}
		h.mu.Lock()
		h.decommissionOn, h.decommissioned = false, err
		h.mu.Unlock()
	}()
	w.WriteHeader(http.StatusAccepted)
	writeJSON(w, map[string]string{"status": "decommissioning"})
}

// RepairRequest asks for a repair of a keyspace (or of one of its tables),
// over the range (Start, End] when both are given.
type RepairRequest struct {
	Keyspace string `json:"keyspace"`
	Table    string `json:"table,omitempty"`
	Start    string `json:"start,omitempty"`
	End      string `json:"end,omitempty"`
}

func (h *handler) repair(w http.ResponseWriter, r *http.Request) {
	var req RepairRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var st db.RepairStats
	var err error
	if req.Start == "" && req.End == "" {
		if req.Table != "" {
			writeError(w, http.StatusBadRequest, errors.New("a table repair needs a range"))
			return
		}
		st, err = h.d.Repair(req.Keyspace)
	} else {
		start, err1 := strconv.ParseInt(req.Start, 10, 64)
		end, err2 := strconv.ParseInt(req.End, 10, 64)
		if err := errors.Join(err1, err2); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("bad range: %w", err))
			return
		}
		st, err = h.d.RepairRange(req.Keyspace, req.Table, start, end)
	}
	h.result(w, st, err)
}

// SnapshotRequest asks for a snapshot of the keyspaces (all user ones if
// none).
type SnapshotRequest struct {
	Name      string   `json:"name"`
	Keyspaces []string `json:"keyspaces,omitempty"`
}

func (h *handler) snapshot(w http.ResponseWriter, r *http.Request) {
	var req SnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	tables, err := h.d.Snapshot(req.Name, req.Keyspaces)
	if err != nil {
		_ = h.d.ClearSnapshot(req.Name)
	}
	if tables == nil {
		tables = []db.SnapshotTable{}
	}
	h.result(w, tables, err)
}

// ImportRequest asks to load sstables into a table.
type ImportRequest struct {
	Keyspace string   `json:"keyspace"`
	Table    string   `json:"table"`
	Files    []string `json:"files"`
}

func (h *handler) importFiles(w http.ResponseWriter, r *http.Request) {
	var req ImportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	h.done(w, h.d.Import(req.Keyspace, req.Table, req.Files))
}

func (h *handler) done(w http.ResponseWriter, err error) {
	h.result(w, map[string]string{"status": "done"}, err)
}

func (h *handler) result(w http.ResponseWriter, v any, err error) {
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, v)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func splitList(s string) []string {
	var out []string
	for _, m := range strings.Split(s, ",") {
		if m = strings.TrimSpace(m); m != "" {
			out = append(out, m)
		}
	}
	return out
}

// legacy are the text endpoints of earlier versions.
func legacy(mux *http.ServeMux, d *db.DB) {
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, d.Status()) })
	mux.HandleFunc("POST /repair", func(w http.ResponseWriter, r *http.Request) {
		st, err := d.Repair(r.URL.Query().Get("keyspace"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "repaired %d ranges: %d mismatches, %d bytes streamed\n", st.Ranges, st.Mismatches, st.Streamed)
	})
	mux.HandleFunc("POST /cleanup", func(w http.ResponseWriter, r *http.Request) {
		n, err := d.Cleanup(r.URL.Query().Get("keyspace"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "cleaned %d tables\n", n)
	})
	mux.HandleFunc("POST /removenode", func(w http.ResponseWriter, r *http.Request) {
		if err := d.RemoveNode(r.URL.Query().Get("node")); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintln(w, "removed")
	})
	mux.HandleFunc("POST /cms/reconfigure", func(w http.ResponseWriter, r *http.Request) {
		members := splitList(r.URL.Query().Get("members"))
		if err := d.ReconfigureCMS(members); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintln(w, "the metadata service is now", strings.Join(members, ", "))
	})
	mux.HandleFunc("POST /decommission", func(w http.ResponseWriter, _ *http.Request) {
		if err := d.Decommission(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintln(w, "decommissioned: the node has left the ring and can be stopped")
	})
}
