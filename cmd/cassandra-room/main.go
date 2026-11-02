package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cassandra-room/cassandra-room/internal/db"
	"github.com/cassandra-room/cassandra-room/internal/server"
)

func main() {
	var (
		addr                  = flag.String("addr", ":9042", "CQL native transport listen address")
		maxConcurrentRequests = flag.Int("max-concurrent-requests", 0, "Global limit on concurrent active CQL requests (0 = default max(NumCPU*64, 256))")
		maxInFlightBytes      = flag.Int64("max-inflight-bytes", server.DefaultMaxInFlightBytes, "Global byte budget for in-flight request payloads")
		maxFrameSize          = flag.Int("max-frame-size", server.DefaultMaxFrameSize, "Maximum allowed CQL frame body size in bytes")
		writeTimeout          = flag.Duration("write-timeout", server.DefaultWriteTimeout, "Per-frame socket write deadline")
		shutdownTimeout       = flag.Duration("shutdown-timeout", 15*time.Second, "Graceful shutdown deadline for draining in-flight requests")
		dataDir               = flag.String("data-dir", "data", "Directory of the database (data_file_directories, commitlog_directory)")
		commitlogSync         = flag.String("commitlog-sync", "periodic", "periodic: fsync the commit log every -commitlog-sync-period; batch: fsync before acknowledging each write")
		commitlogSyncPeriod   = flag.Duration("commitlog-sync-period", 10*time.Second, "Commit log fsync period in periodic mode (commitlog_sync_period)")
		memtableSpace         = flag.Int64("memtable-space", 0, "Memtable space in bytes across all tables (memtable_heap_space); 0 uses the default")
		clusterName           = flag.String("cluster-name", "Test Cluster", "Cluster name reported in system.local")
		authenticator         = flag.String("authenticator", "AllowAllAuthenticator", "AllowAllAuthenticator or PasswordAuthenticator (default superuser cassandra/cassandra)")
		authorizer            = flag.String("authorizer", "AllowAllAuthorizer", "AllowAllAuthorizer or CassandraAuthorizer")
		listenAddress         = flag.String("listen-address", "", "Internode address host:port (listen_address and storage_port); empty runs a single node")
		seeds                 = flag.String("seeds", "", "Comma-separated internode addresses of seed nodes")
		numTokens             = flag.Int("num-tokens", 16, "Number of tokens (vnodes) of this node")
		dataCenter            = flag.String("dc", "datacenter1", "Data center of this node")
		rack                  = flag.String("rack", "rack1", "Rack of this node")
		adminAddress          = flag.String("admin-address", "", "HTTP address for nodetool-style operations: GET /status, POST /repair?keyspace=, POST /cleanup?keyspace=, POST /decommission")
	)
	flag.Parse()

	if *commitlogSync != "periodic" && *commitlogSync != "batch" {
		log.Fatalf("-commitlog-sync must be periodic or batch, got %q", *commitlogSync)
	}
	host, portStr, err := net.SplitHostPort(*addr)
	if err != nil {
		log.Fatalf("invalid -addr %q: %v", *addr, err)
	}
	port, _ := strconv.Atoi(portStr)
	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() {
		ip = net.IPv4(127, 0, 0, 1)
	}
	opts := db.Options{SyncWrites: *commitlogSync == "batch", ClusterName: *clusterName, Address: ip, Port: port,
		Authenticator: *authenticator == "PasswordAuthenticator", Authorizer: *authorizer == "CassandraAuthorizer"}
	opts.DataCenter, opts.Rack = *dataCenter, *rack
	if *listenAddress != "" {
		opts.Cluster = &db.ClusterOptions{ListenAddress: *listenAddress, NumTokens: *numTokens}
		for _, s := range strings.Split(*seeds, ",") {
			if s = strings.TrimSpace(s); s != "" {
				opts.Cluster.Seeds = append(opts.Cluster.Seeds, s)
			}
		}
	}
	opts.Engine.WALSyncPeriod = *commitlogSyncPeriod
	opts.Engine.MemTableBudget = *memtableSpace
	database, err := db.Open(*dataDir, opts)
	if err != nil {
		log.Fatalf("failed to open the database in %s: %v", *dataDir, err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			log.Printf("closing the database: %v", err)
		}
	}()

	if *adminAddress != "" {
		go serveAdmin(*adminAddress, database)
	}

	srv := server.New(database)
	if *maxConcurrentRequests > 0 {
		srv.SetMaxConcurrentRequests(*maxConcurrentRequests)
	}
	if *maxInFlightBytes > 0 {
		srv.SetMaxInFlightBytes(*maxInFlightBytes)
	}
	if *maxFrameSize > 0 {
		srv.SetMaxFrameSize(*maxFrameSize)
	}
	srv.SetWriteTimeout(*writeTimeout)

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}
	log.Printf("cassandra-room listening for CQL v3/v4/v5 connections on %s", ln.Addr())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- srv.Serve(ln)
	}()

	select {
	case err := <-serveErrCh:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			log.Fatalf("server stopped with error: %v", err)
		}
	case sig := <-sigCh:
		log.Printf("received %s, initiating graceful shutdown (timeout=%s)...", sig, *shutdownTimeout)
		ctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("graceful shutdown timed out: %v", err)
		} else {
			log.Printf("graceful shutdown complete")
		}
	}
}

//reviewed

// serveAdmin serves the operations nodetool provides over JMX.
func serveAdmin(addr string, d *db.DB) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, d.Status())
	})
	mux.HandleFunc("POST /repair", func(w http.ResponseWriter, r *http.Request) {
		st, err := d.Repair(r.URL.Query().Get("keyspace"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "repaired %d ranges: %d mismatching, %d rows streamed\n", st.Ranges, st.Mismatches, st.Streamed)
	})
	mux.HandleFunc("POST /cleanup", func(w http.ResponseWriter, r *http.Request) {
		n, err := d.Cleanup(r.URL.Query().Get("keyspace"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "cleaned %d tables\n", n)
	})
	mux.HandleFunc("POST /decommission", func(w http.ResponseWriter, _ *http.Request) {
		if err := d.Decommission(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintln(w, "decommissioned: the node has left the ring and can be stopped")
	})
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("admin endpoint: %v", err)
	}
}
