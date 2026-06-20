package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// tlsFixture holds on-disk PEM paths for a self-signed CA and a server cert it
// signed, suitable for wiring a real TLS gRPC server + a TLS client that trusts
// the CA.
type tlsFixture struct {
	caFile     string
	serverCert string // PEM cert+key bundle paths
	serverKey  string
	serverName string
}

// newTLSFixture generates a CA and a server leaf certificate valid for
// "localhost"/127.0.0.1, writing them under t.TempDir(). It is deterministic
// only in structure, not in key material (fresh keys each run).
func newTLSFixture(t *testing.T) tlsFixture {
	t.Helper()
	dir := t.TempDir()

	// CA.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "mcp-grpc test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}

	// Server leaf signed by the CA.
	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("server key: %v", err)
	}
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("server cert: %v", err)
	}

	caPEM := filepath.Join(dir, "ca.pem")
	writePEM(t, caPEM, "CERTIFICATE", caDER)

	srvCertPEM := filepath.Join(dir, "server.pem")
	writePEM(t, srvCertPEM, "CERTIFICATE", srvDER)

	srvKeyPEM := filepath.Join(dir, "server-key.pem")
	keyDER, err := x509.MarshalECPrivateKey(srvKey)
	if err != nil {
		t.Fatalf("marshal server key: %v", err)
	}
	writePEM(t, srvKeyPEM, "EC PRIVATE KEY", keyDER)

	return tlsFixture{
		caFile:     caPEM,
		serverCert: srvCertPEM,
		serverKey:  srvKeyPEM,
		serverName: "localhost",
	}
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	if err := pem.Encode(f, &pem.Block{Type: typ, Bytes: der}); err != nil {
		t.Fatalf("encode %s: %v", path, err)
	}
}
