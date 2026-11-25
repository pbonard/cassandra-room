package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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

	"github.com/pbonard/cassandra-room/internal/cluster"
	"github.com/pbonard/cassandra-room/internal/db"
	"github.com/pbonard/cassandra-room/internal/server"
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
		internodeCert         = flag.String("internode-cert", "", "PEM certificate for internode TLS (with -internode-key and -internode-ca)")
		internodeKey          = flag.String("internode-key", "", "PEM private key for internode TLS")
		internodeCA           = flag.String("internode-ca", "", "PEM CA that peers' certificates must chain to")
		internodeVerifyHost   = flag.Bool("internode-verify-hostname", false, "Also check peers' certificate names (require_endpoint_verification)")
		internodeCompression  = flag.Bool("internode-compression", true, "Compress internode messages with LZ4")
		replaceAddress        = flag.String("replace-address", "", "Replace a dead node (its internode address or host id) on first boot")
		numTokens             = flag.Int("num-tokens", 16, "Number of tokens (vnodes) of this node")
		dataCenter            = flag.String("dc", "datacenter1", "Data center of this node")
		rack                  = flag.String("rack", "rack1", "Rack of this node")
		broadcastAddress      = flag.String("broadcast-address", "", "Internode address host:port other nodes connect to and know this node by (broadcast_address), if not -listen-address (NAT, another network)")
		broadcastRPCAddress   = flag.String("broadcast-rpc-address", "", "Address clients are told to connect to (broadcast_rpc_address, in system.local and system.peers); required when -addr listens on all interfaces in a cluster")
		preferLocal           = flag.Bool("prefer-local", false, "Nodes of the same data center connect to each other at their -listen-address (prefer_local)")
		clientCert            = flag.String("client-cert", "", "PEM certificate for client (native protocol) TLS, with -client-key (client_encryption_options)")
		clientKey             = flag.String("client-key", "", "PEM private key for client TLS")
		clientCA              = flag.String("client-ca", "", "PEM CA client certificates must chain to; set, clients must present one (require_client_auth)")
		clientTLSOptional     = flag.Bool("client-tls-optional", false, "Also accept unencrypted client connections on the same port (optional: true)")
		adminAddress          = flag.String("admin-address", "", "HTTP address for nodetool-style operations: GET /status, POST /repair?keyspace=, POST /cleanup?keyspace=, POST /removenode?node=, POST /cms/reconfigure?members=, POST /decommission")
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
	// The address clients are told to connect to: -broadcast-rpc-address,
	// else the one listened on (which must then name an address).
	ip := net.ParseIP(host)
	if host != "" && ip == nil {
		ip = resolve(host)
	}
	if *broadcastRPCAddress != "" {
		ip = net.ParseIP(*broadcastRPCAddress)
		if ip == nil {
			ip = resolve(*broadcastRPCAddress)
		}
	} else if ip == nil || ip.IsUnspecified() {
		if *listenAddress != "" {
			log.Fatalf("-addr %q listens on every interface: set -broadcast-rpc-address to the address clients reach this node at", *addr)
		}
		ip = net.IPv4(127, 0, 0, 1) // a single node, for local use
	}
	opts := db.Options{SyncWrites: *commitlogSync == "batch", ClusterName: *clusterName, Address: ip, Port: port,
		Authenticator: *authenticator == "PasswordAuthenticator", Authorizer: *authorizer == "CassandraAuthorizer"}
	opts.DataCenter, opts.Rack = *dataCenter, *rack
	if *listenAddress != "" {
		opts.Cluster = &db.ClusterOptions{ListenAddress: *listenAddress, NumTokens: *numTokens, ReplaceAddress: *replaceAddress,
			BroadcastAddress: resolveHostPort(*broadcastAddress), PreferLocal: *preferLocal}
		if lh, _, err := net.SplitHostPort(*listenAddress); err != nil {
			log.Fatalf("invalid -listen-address %q: %v", *listenAddress, err)
		} else if lip := net.ParseIP(lh); (lh == "" || lip != nil && lip.IsUnspecified()) && *broadcastAddress == "" {
			log.Fatalf("-listen-address %q listens on every interface: set -broadcast-address to the address other nodes reach this node at", *listenAddress)
		}
		opts.Cluster.Messaging.Compress = *internodeCompression
		if *internodeCert != "" {
			cfg, err := cluster.TLSConfig(*internodeCert, *internodeKey, *internodeCA, *internodeVerifyHost)
			if err != nil {
				log.Fatal(err)
			}
			opts.Cluster.Messaging.TLS = cfg
		}
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
	if *clientCert != "" {
		cfg, err := clientTLS(*clientCert, *clientKey, *clientCA)
		if err != nil {
			log.Fatal(err)
		}
		srv.SetTLS(cfg, *clientTLSOptional)
	}

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
		fmt.Fprintf(w, "repaired %d ranges: %d mismatching, %d bytes streamed\n", st.Ranges, st.Mismatches, st.Streamed)
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
		var members []string
		for _, m := range strings.Split(r.URL.Query().Get("members"), ",") {
			if m = strings.TrimSpace(m); m != "" {
				members = append(members, m)
			}
		}
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
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("admin endpoint: %v", err)
	}
}

// clientTLS is the TLS configuration of the native port: the node's
// certificate and, with a CA, required client certificates.
func clientTLS(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("client TLS: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("client TLS: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("client TLS: no certificate in %s", caFile)
		}
		cfg.ClientCAs, cfg.ClientAuth = pool, tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// resolve looks up a host name's address, as Cassandra resolves its
// configured addresses at startup.
func resolve(host string) net.IP {
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		log.Fatalf("cannot resolve %q: %v", host, err)
	}
	for _, ip := range ips {
		if v4 := ip.To4(); v4 != nil {
			return v4
		}
	}
	return ips[0]
}

// resolveHostPort resolves the host of a "host:port" address to an IP
// (nodes are known by IP, as in system.peers).
func resolveHostPort(addr string) string {
	if addr == "" {
		return ""
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.Fatalf("invalid address %q: %v", addr, err)
	}
	if net.ParseIP(host) == nil {
		host = resolve(host).String()
	}
	return net.JoinHostPort(host, port)
}
