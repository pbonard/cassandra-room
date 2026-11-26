package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strconv"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pbonard/cassandra-room/internal/db"
	"github.com/stretchr/testify/require"
)

// testCA issues certificates for tests.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newTestCA(t *testing.T) *testCA {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &testCA{cert: cert, key: key, pool: pool}
}

func (ca *testCA) issue(t *testing.T, name string, usage x509.ExtKeyUsage) tls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// startTLSServer serves a fresh database with client encryption.
func startTLSServer(t *testing.T, cfg *tls.Config, optional bool) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	d, err := db.Open("db", db.Options{FS: vfs.NewMem(), Port: port})
	require.NoError(t, err)
	srv := New(d)
	srv.SetTLS(cfg, optional)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = d.Close()
	})
	return port
}

// tlsSession connects as my-server does: TLS checking the server's name
// against its certificate, optionally with a client certificate.
func tlsSession(port int, ca *x509.CertPool, client *tls.Certificate) (*gocql.Session, error) {
	cluster := gocql.NewCluster("127.0.0.1:" + strconv.Itoa(port))
	cluster.Timeout, cluster.ConnectTimeout = 5*time.Second, 5*time.Second
	if ca != nil {
		cfg := &tls.Config{RootCAs: ca, ServerName: "127.0.0.1"}
		if client != nil {
			cfg.Certificates = []tls.Certificate{*client}
		}
		cluster.SslOpts = &gocql.SslOptions{Config: cfg, EnableHostVerification: true}
	}
	return cluster.CreateSession()
}

func query(t *testing.T, s *gocql.Session) {
	var v string
	require.NoError(t, s.Query("SELECT release_version FROM system.local").Scan(&v))
	require.NotEmpty(t, v)
	s.Close()
}

func TestClientTLS(t *testing.T) {
	ca := newTestCA(t)
	serverCert := ca.issue(t, "server", x509.ExtKeyUsageServerAuth)
	clientCert := ca.issue(t, "client", x509.ExtKeyUsageClientAuth)
	base := &tls.Config{Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS12}

	t.Run("required", func(t *testing.T) {
		port := startTLSServer(t, base, false)
		s, err := tlsSession(port, ca.pool, nil)
		require.NoError(t, err)
		query(t, s)
		_, err = tlsSession(port, nil, nil)
		require.Error(t, err, "an unencrypted client is refused")
		_, err = tlsSession(port, x509.NewCertPool(), nil)
		require.Error(t, err, "a client that does not trust the certificate refuses it")
	})
	t.Run("optional", func(t *testing.T) {
		port := startTLSServer(t, base, true)
		s, err := tlsSession(port, ca.pool, nil)
		require.NoError(t, err)
		query(t, s)
		s, err = tlsSession(port, nil, nil)
		require.NoError(t, err, "unencrypted clients are accepted too")
		query(t, s)
	})
	t.Run("client auth", func(t *testing.T) {
		cfg := base.Clone()
		cfg.ClientCAs, cfg.ClientAuth = ca.pool, tls.RequireAndVerifyClientCert
		port := startTLSServer(t, cfg, false)
		_, err := tlsSession(port, ca.pool, nil)
		require.Error(t, err, "a client without a certificate is refused")
		s, err := tlsSession(port, ca.pool, &clientCert)
		require.NoError(t, err)
		query(t, s)
	})
}
