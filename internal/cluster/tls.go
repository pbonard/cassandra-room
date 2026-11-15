package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// TLSConfig builds the internode TLS configuration from PEM files
// (server_encryption_options with mutual authentication): the node's
// certificate and key, and the CA its peers' certificates must chain to.
// Without verifyHostname (require_endpoint_verification: false, the
// default) peers are checked against the CA only, not their names.
func TLSConfig(certFile, keyFile, caFile string, verifyHostname bool) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("cluster: loading the internode certificate: %w", err)
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("cluster: reading the internode CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("cluster: no certificate in the internode CA file")
	}
	return NewTLSConfig(cert, pool, verifyHostname), nil
}

// NewTLSConfig is TLSConfig from a loaded certificate and CA pool.
func NewTLSConfig(cert tls.Certificate, roots *x509.CertPool, verifyHostname bool) *tls.Config {
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      roots,
		ClientCAs:    roots,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}
	if !verifyHostname {
		// Verify the chain, not the name.
		cfg.InsecureSkipVerify = true
		cfg.VerifyPeerCertificate = func(raw [][]byte, _ [][]*x509.Certificate) error {
			if len(raw) == 0 {
				return errors.New("cluster: peer sent no certificate")
			}
			certs := make([]*x509.Certificate, len(raw))
			for i, b := range raw {
				c, err := x509.ParseCertificate(b)
				if err != nil {
					return err
				}
				certs[i] = c
			}
			inter := x509.NewCertPool()
			for _, c := range certs[1:] {
				inter.AddCert(c)
			}
			_, err := certs[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter,
				KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
			return err
		}
	}
	return cfg
}
