package cluster

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return ca, key
}

func testNodeCert(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey) tls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "node"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// TestMessagingTLSAndCompression runs requests over mutually
// authenticated TLS with LZ4 compression, and checks that a node whose
// certificate another CA signed is refused.
func TestMessagingTLSAndCompression(t *testing.T) {
	ca, caKey := testCA(t)
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	opts := func() MessagingOptions {
		return MessagingOptions{TLS: NewTLSConfig(testNodeCert(t, ca, caKey), pool, false), Compress: true}
	}
	a, err := NewMessagingWith("127.0.0.1:0", opts())
	require.NoError(t, err)
	b, err := NewMessagingWith("127.0.0.1:0", opts())
	require.NoError(t, err)
	b.Handle(VerbMutation, func(_ context.Context, _ string, p []byte) ([]byte, error) { return append(p, p...), nil })
	a.Start()
	b.Start()
	defer a.Close()
	defer b.Close()
	big := bytes.Repeat([]byte("compressible payload "), 10000)
	for _, p := range [][]byte{[]byte("small"), big} {
		resp, err := a.Request(context.Background(), b.Addr(), VerbMutation, p)
		require.NoError(t, err)
		require.Equal(t, append(append([]byte{}, p...), p...), resp)
	}
	require.Less(t, len(func() []byte { _, x := a.pack(big); return x }()), len(big)/10)

	otherCA, otherKey := testCA(t)
	otherPool := x509.NewCertPool()
	otherPool.AddCert(otherCA)
	c, err := NewMessagingWith("127.0.0.1:0", MessagingOptions{TLS: NewTLSConfig(testNodeCert(t, otherCA, otherKey), otherPool, false)})
	require.NoError(t, err)
	c.Start()
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = c.Request(ctx, b.Addr(), VerbMutation, []byte("x"))
	require.Error(t, err, "a certificate from another CA")
}
