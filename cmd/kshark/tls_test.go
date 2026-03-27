package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
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

// ---------- context cancellation tests ----------

func TestCheckDNS_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := &Report{}
	start := time.Now()
	checkDNS(ctx, r, "this.host.will.never.resolve.example.com", "kafka")
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Errorf("checkDNS with cancelled ctx took %v, expected <2s", elapsed)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0].Status != FAIL {
		t.Errorf("status = %s, want FAIL for cancelled context", r.Rows[0].Status)
	}
}

func TestCheckTCP_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := &Report{}
	start := time.Now()
	conn := checkTCP(ctx, r, "192.0.2.1:9092", "kafka", 30*time.Second)
	elapsed := time.Since(start)

	if conn != nil {
		conn.Close()
		t.Error("expected nil conn for cancelled context")
	}
	if elapsed > 2*time.Second {
		t.Errorf("checkTCP with cancelled ctx took %v, expected <2s", elapsed)
	}
	if r.Rows[0].Status != FAIL {
		t.Errorf("status = %s, want FAIL", r.Rows[0].Status)
	}
}

// ---------- checkDNS tests ----------

func TestCheckDNS_ResolvableHost(t *testing.T) {
	r := &Report{}
	checkDNS(context.Background(), r,"localhost", "kafka")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0].Status != OK {
		t.Errorf("status = %s, want OK", r.Rows[0].Status)
	}
	if r.Rows[0].Layer != L3 {
		t.Errorf("layer = %s, want L3-Network", r.Rows[0].Layer)
	}
}

func TestCheckDNS_UnresolvableHost(t *testing.T) {
	r := &Report{}
	checkDNS(context.Background(), r,"this.host.does.not.exist.kshark.invalid", "kafka")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0].Status != FAIL {
		t.Errorf("status = %s, want FAIL", r.Rows[0].Status)
	}
	if r.Rows[0].Hint == "" {
		t.Error("expected non-empty hint for DNS failure")
	}
}

func TestCheckDNS_ComponentLabel(t *testing.T) {
	r := &Report{}
	checkDNS(context.Background(), r,"localhost", "connector-mongodb")
	if r.Rows[0].Component != "connector-mongodb" {
		t.Errorf("component = %q, want %q", r.Rows[0].Component, "connector-mongodb")
	}
}

// ---------- checkTCP tests ----------

func TestCheckTCP_OpenPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	r := &Report{}
	addr := ln.Addr().String()
	conn := checkTCP(context.Background(), r,addr, "kafka", 5*time.Second)
	if conn == nil {
		t.Fatal("expected non-nil conn for open port")
	}
	conn.Close()

	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0].Status != OK {
		t.Errorf("status = %s, want OK", r.Rows[0].Status)
	}
	if r.Rows[0].Layer != L4 {
		t.Errorf("layer = %s, want L4-TCP", r.Rows[0].Layer)
	}
}

func TestCheckTCP_ClosedPort(t *testing.T) {
	// Get a port that's not listening
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()

	r := &Report{}
	conn := checkTCP(context.Background(), r,addr, "kafka", 2*time.Second)
	if conn != nil {
		conn.Close()
		t.Error("expected nil conn for closed port")
	}

	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0].Status != FAIL {
		t.Errorf("status = %s, want FAIL", r.Rows[0].Status)
	}
}

func TestCheckTCP_Timeout(t *testing.T) {
	// Use a non-routable address to force a timeout
	// 192.0.2.1 is TEST-NET-1 (RFC 5737), not routable
	r := &Report{}
	start := time.Now()
	conn := checkTCP(context.Background(), r,"192.0.2.1:9092", "kafka", 500*time.Millisecond)
	elapsed := time.Since(start)

	if conn != nil {
		conn.Close()
		t.Error("expected nil conn for timeout")
	}
	if elapsed > 3*time.Second {
		t.Errorf("took %v, expected completion within ~500ms timeout", elapsed)
	}
	if r.Rows[0].Status != FAIL {
		t.Errorf("status = %s, want FAIL", r.Rows[0].Status)
	}
}

func TestCheckTCP_ReturnsLatencyInDetail(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	r := &Report{}
	conn := checkTCP(context.Background(), r,ln.Addr().String(), "kafka", 5*time.Second)
	if conn != nil {
		conn.Close()
	}
	if len(r.Rows) > 0 && r.Rows[0].Status == OK {
		if !strings.Contains(r.Rows[0].Detail, "Connected in") {
			t.Errorf("detail = %q, expected 'Connected in' prefix", r.Rows[0].Detail)
		}
	}
}

// ---------- wrapTLS tests ----------

// generateTestTLSCert creates a self-signed cert+key for testing.
func generateTestTLSCert(t *testing.T, notAfter time.Time) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(42),
		Subject:      pkix.Name{CommonName: "test.kshark.local"},
		DNSNames:     []string{"test.kshark.local", "127.0.0.1", "localhost"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	cert.Leaf, _ = x509.ParseCertificate(certDER)
	return cert
}

func TestWrapTLS_NilConfig_PLAINTEXT(t *testing.T) {
	// Create a dummy TCP connection pair
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			// Just hold the connection open
			time.Sleep(100 * time.Millisecond)
			conn.Close()
		}
	}()

	base, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()

	r := &Report{}
	got := wrapTLS(context.Background(), r,base, nil, "kafka", "127.0.0.1:9092")
	if got != base {
		t.Error("expected wrapTLS to return base conn when tlsConf is nil")
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0].Status != SKIP {
		t.Errorf("status = %s, want SKIP", r.Rows[0].Status)
	}
	if r.Rows[0].Layer != L56 {
		t.Errorf("layer = %s, want L5-6-TLS", r.Rows[0].Layer)
	}
}

func TestWrapTLS_SuccessfulHandshake(t *testing.T) {
	cert := generateTestTLSCert(t, time.Now().Add(365*24*time.Hour))

	// Use plain TCP listener; wrapTLS does the TLS handshake on client side
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		serverTLSConf := &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		tlsConn := tls.Server(conn, serverTLSConf)
		if err := tlsConn.Handshake(); err != nil {
			conn.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
		tlsConn.Close()
	}()

	base, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()

	certPool := x509.NewCertPool()
	certPool.AddCert(cert.Leaf)
	clientTLSConf := &tls.Config{
		RootCAs:    certPool,
		ServerName: "test.kshark.local",
	}

	r := &Report{}
	got := wrapTLS(context.Background(), r,base, clientTLSConf, "kafka", "127.0.0.1:9092")
	if got == nil {
		t.Fatal("expected non-nil conn after successful TLS handshake")
	}
	got.Close()

	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0].Status != OK {
		t.Errorf("status = %s, want OK; detail: %s", r.Rows[0].Status, r.Rows[0].Detail)
	}
	if !strings.Contains(r.Rows[0].Detail, "TLS") {
		t.Errorf("detail should mention TLS, got %q", r.Rows[0].Detail)
	}
}

func TestWrapTLS_HandshakeFailure_WrongCA(t *testing.T) {
	cert := generateTestTLSCert(t, time.Now().Add(365*24*time.Hour))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		serverTLSConf := &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		tlsConn := tls.Server(conn, serverTLSConf)
		// Handshake will fail on client side but we still need to attempt it
		tlsConn.Handshake()
		tlsConn.Close()
	}()

	base, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()

	// Client uses system CAs (won't trust self-signed)
	clientTLSConf := &tls.Config{
		ServerName: "test.kshark.local",
	}

	r := &Report{}
	got := wrapTLS(context.Background(), r,base, clientTLSConf, "kafka", "127.0.0.1:9092")
	if got != nil {
		got.Close()
		t.Error("expected nil conn for untrusted cert")
	}
	if r.Rows[0].Status != FAIL {
		t.Errorf("status = %s, want FAIL", r.Rows[0].Status)
	}
}

func TestWrapTLS_CertExpiringSoon(t *testing.T) {
	// Cert expiring in 15 days (under 30-day threshold)
	cert := generateTestTLSCert(t, time.Now().Add(15*24*time.Hour))

	// Use plain TCP listener; wrapTLS does the TLS handshake on client side
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		// Server-side TLS handshake
		serverTLSConf := &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		tlsConn := tls.Server(conn, serverTLSConf)
		if err := tlsConn.Handshake(); err != nil {
			conn.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
		tlsConn.Close()
	}()

	base, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()

	certPool := x509.NewCertPool()
	certPool.AddCert(cert.Leaf)
	clientTLSConf := &tls.Config{
		RootCAs:    certPool,
		ServerName: "test.kshark.local",
	}

	r := &Report{}
	got := wrapTLS(context.Background(), r,base, clientTLSConf, "kafka", "127.0.0.1:9092")
	if got == nil {
		t.Fatal("expected non-nil conn (cert expiring soon, but still valid)")
	}
	got.Close()

	if r.Rows[0].Status != WARN {
		t.Errorf("status = %s, want WARN for cert expiring in 15 days", r.Rows[0].Status)
	}
	if r.Rows[0].Hint == "" {
		t.Error("expected hint about cert expiry")
	}
}
