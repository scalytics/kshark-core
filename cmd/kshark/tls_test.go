package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTlsConfigFromProps_PLAINTEXT(t *testing.T) {
	props := map[string]string{
		"security.protocol": "PLAINTEXT",
	}
	conf, desc, err := tlsConfigFromProps(props, "broker.example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if conf != nil {
		t.Error("expected nil config for PLAINTEXT")
	}
	if desc != "no TLS" {
		t.Errorf("desc = %q, want %q", desc, "no TLS")
	}
}

func TestTlsConfigFromProps_EmptyProtocol(t *testing.T) {
	props := map[string]string{}
	conf, _, err := tlsConfigFromProps(props, "broker.example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if conf != nil {
		t.Error("expected nil config when security.protocol is empty (defaults to PLAINTEXT)")
	}
}

func TestTlsConfigFromProps_SASL_SSL(t *testing.T) {
	props := map[string]string{
		"security.protocol": "SASL_SSL",
	}
	conf, desc, err := tlsConfigFromProps(props, "broker.example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if conf == nil {
		t.Fatal("expected non-nil config for SASL_SSL")
	}
	if conf.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want TLS 1.2 (%x)", conf.MinVersion, tls.VersionTLS12)
	}
	if conf.ServerName != "broker.example.com" {
		t.Errorf("ServerName = %q, want %q", conf.ServerName, "broker.example.com")
	}
	if desc != "TLS enabled" {
		t.Errorf("desc = %q, want %q", desc, "TLS enabled")
	}
}

func TestTlsConfigFromProps_SSL(t *testing.T) {
	props := map[string]string{
		"security.protocol": "SSL",
	}
	conf, _, err := tlsConfigFromProps(props, "broker.example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if conf == nil {
		t.Fatal("expected non-nil config for SSL")
	}
	if conf.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want TLS 1.2", conf.MinVersion)
	}
}

// generateSelfSignedCAPEM creates a self-signed CA certificate PEM for testing.
func generateSelfSignedCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Test CA"}},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
}

func TestTlsConfigFromProps_WithCACert(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")

	caPEM := generateSelfSignedCAPEM(t)
	if err := os.WriteFile(caPath, caPEM, 0644); err != nil {
		t.Fatal(err)
	}

	props := map[string]string{
		"security.protocol": "SSL",
		"ssl.ca.location":   caPath,
	}
	conf, _, err := tlsConfigFromProps(props, "broker.example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if conf == nil {
		t.Fatal("expected non-nil config")
	}
	if conf.RootCAs == nil {
		t.Error("expected non-nil RootCAs when CA cert is provided")
	}
}

func TestTlsConfigFromProps_NonexistentCA(t *testing.T) {
	props := map[string]string{
		"security.protocol": "SSL",
		"ssl.ca.location":   "/nonexistent/ca.pem",
	}
	_, _, err := tlsConfigFromProps(props, "broker.example.com")
	if err == nil {
		t.Fatal("expected error for nonexistent CA path")
	}
}

func TestPeerCN_EmptyPeerCerts(t *testing.T) {
	state := &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{},
	}
	got := peerCN(state)
	if got != "-" {
		t.Errorf("peerCN(empty) = %q, want %q", got, "-")
	}
}

func TestPeerCN_WithDNSNames(t *testing.T) {
	state := &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{
			{
				DNSNames: []string{"example.com", "www.example.com"},
				Subject:  pkix.Name{CommonName: "cn-name"},
			},
		},
	}
	got := peerCN(state)
	if got != "example.com" {
		t.Errorf("peerCN(with DNS) = %q, want %q", got, "example.com")
	}
}

func TestPeerCN_OnlyCommonName(t *testing.T) {
	state := &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{
			{
				DNSNames: nil,
				Subject:  pkix.Name{CommonName: "my-common-name"},
			},
		},
	}
	got := peerCN(state)
	if got != "my-common-name" {
		t.Errorf("peerCN(CN only) = %q, want %q", got, "my-common-name")
	}
}

func TestEarliestExpiry_NoCerts(t *testing.T) {
	state := &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{},
	}
	got := earliestExpiry(state)
	// Should return ~365 days from now
	expectedApprox := time.Now().Add(365 * 24 * time.Hour)
	diff := got.Sub(expectedApprox)
	if diff < -5*time.Second || diff > 5*time.Second {
		t.Errorf("earliestExpiry(no certs) = %v, expected ~%v", got, expectedApprox)
	}
}

func TestEarliestExpiry_MultipleCerts(t *testing.T) {
	earliest := time.Now().Add(30 * 24 * time.Hour)  // 30 days from now
	later := time.Now().Add(180 * 24 * time.Hour)     // 180 days from now

	state := &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{
			{NotAfter: later},
			{NotAfter: earliest},
		},
	}
	got := earliestExpiry(state)
	diff := got.Sub(earliest)
	if diff < -time.Second || diff > time.Second {
		t.Errorf("earliestExpiry() = %v, want ~%v", got, earliest)
	}
}

func TestEarliestExpiry_SingleCert(t *testing.T) {
	expiry := time.Now().Add(60 * 24 * time.Hour)
	state := &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{
			{NotAfter: expiry},
		},
	}
	got := earliestExpiry(state)
	diff := got.Sub(expiry)
	if diff < -time.Second || diff > time.Second {
		t.Errorf("earliestExpiry() = %v, want ~%v", got, expiry)
	}
}
