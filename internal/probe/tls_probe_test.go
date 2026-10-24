package probe

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

func selfSignedCert(t *testing.T, cn string, notAfter time.Time) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		DNSNames:              []string{cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, leaf
}

func tlsListener(t *testing.T, cert tls.Certificate) string {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				_ = c.(*tls.Conn).Handshake()
				buf := make([]byte, 1)
				_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
				_, _ = c.Read(buf)
				c.Close()
			}(c)
		}
	}()
	return ln.Addr().String()
}

func dialTLSProbe(t *testing.T, addr string, cfg *tls.Config) ProbeStep {
	t.Helper()
	conn, step := ProbeTCP(addr, 2*time.Second)
	if step.Status != StatusOK {
		t.Fatalf("tcp step = %+v", step)
	}
	tlsConn, tlsStep := ProbeTLS(conn, "db.test", cfg)
	if tlsConn != nil {
		tlsConn.Close()
	} else {
		conn.Close()
	}
	return tlsStep
}

func TestProbeTLS_Handshakes(t *testing.T) {
	goodCert, goodLeaf := selfSignedCert(t, "db.test", time.Now().Add(365*24*time.Hour))
	soonCert, soonLeaf := selfSignedCert(t, "db.test", time.Now().Add(10*24*time.Hour))
	goodAddr := tlsListener(t, goodCert)
	soonAddr := tlsListener(t, soonCert)

	pool := func(c *x509.Certificate) *x509.CertPool {
		p := x509.NewCertPool()
		p.AddCert(c)
		return p
	}

	tests := []struct {
		name   string
		addr   string
		cfg    *tls.Config
		status string
		detail string
		hint   string
	}{
		{"trusted", goodAddr, &tls.Config{ServerName: "db.test", RootCAs: pool(goodLeaf)}, StatusOK, "CN=db.test, expires", ""},
		{"expiring soon", soonAddr, &tls.Config{ServerName: "db.test", RootCAs: pool(soonLeaf)}, StatusWARN, "CN=db.test", "less than 30 days"},
		{"unknown authority", goodAddr, &tls.Config{ServerName: "db.test"}, StatusFAIL, "TLS handshake failed", "CA not trusted"},
		{"ip without san", goodAddr, &tls.Config{ServerName: "127.0.0.1", RootCAs: pool(goodLeaf)}, StatusFAIL, "TLS handshake failed", "Hostname mismatch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := dialTLSProbe(t, tt.addr, tt.cfg)
			if step.Layer != "L5-6-TLS" || step.Status != tt.status || !strings.Contains(step.Detail, tt.detail) || !strings.Contains(step.Hint, tt.hint) {
				t.Errorf("step = %+v", step)
			}
		})
	}
}

func TestProbeTLS_PlainServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			_, _ = c.Write([]byte("HTTP/1.0 400 Bad Request\r\n\r\n"))
			c.Close()
		}
	}()
	step := dialTLSProbe(t, ln.Addr().String(), &tls.Config{ServerName: "db.test"})
	if step.Status != StatusFAIL || step.Hint != "Check TLS configuration and certificate chain." {
		t.Errorf("step = %+v", step)
	}
}
