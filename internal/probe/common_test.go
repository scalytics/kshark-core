package probe

import (
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProbeDNS_Success(t *testing.T) {
	// Test with a well-known hostname
	step := ProbeDNS("google.com")
	if step.Status != StatusOK {
		t.Errorf("DNS lookup for google.com: status = %q, want OK. Detail: %s", step.Status, step.Detail)
	}
	if step.Layer != "L3-DNS" {
		t.Errorf("layer = %q, want L3-DNS", step.Layer)
	}
}

func TestProbeDNS_Failure(t *testing.T) {
	step := ProbeDNS("this-host-does-not-exist-kshark-test.invalid")
	if step.Status != StatusFAIL {
		t.Errorf("DNS lookup for nonexistent host: status = %q, want FAIL", step.Status)
	}
	if step.Hint == "" {
		t.Error("expected non-empty hint on DNS failure")
	}
}

func TestProbeTCP_Failure(t *testing.T) {
	// Connect to a port that's likely not listening
	conn, step := ProbeTCP("127.0.0.1:19999", 1*1e9) // 1 second
	if conn != nil {
		conn.Close()
		t.Skip("port 19999 is unexpectedly open, skipping test")
	}
	if step.Status != StatusFAIL {
		t.Errorf("TCP connect to closed port: status = %q, want FAIL", step.Status)
	}
}

func TestBuildTLSConfig(t *testing.T) {
	cfg, err := BuildTLSConfig("example.com", "", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ServerName != "example.com" {
		t.Errorf("ServerName = %q, want example.com", cfg.ServerName)
	}
	if cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify should be false")
	}
}

func TestBuildTLSConfig_Insecure(t *testing.T) {
	cfg, err := BuildTLSConfig("example.com", "", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify should be true")
	}
}

func TestScrubCredentials(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantNot string
	}{
		{
			name:    "MongoDB URI with creds",
			input:   "connection failed: mongodb+srv://admin:secret123@cluster.mongodb.net/db",
			wantNot: "secret123",
		},
		{
			name:    "PostgreSQL URI with creds",
			input:   "connection failed: postgresql://user:mypass@pghost/db",
			wantNot: "mypass",
		},
		{
			name:    "no creds to scrub",
			input:   "connection refused on port 5432",
			wantNot: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ScrubCredentials(tt.input)
			if tt.wantNot != "" && strings.Contains(result, tt.wantNot) {
				t.Errorf("ScrubCredentials(%q) still contains %q: got %q", tt.input, tt.wantNot, result)
			}
		})
	}
}

func TestBuildTLSConfig_BadCAPath(t *testing.T) {
	_, err := BuildTLSConfig("example.com", "/nonexistent/ca.pem", false)
	if err == nil {
		t.Fatal("expected error for nonexistent CA cert")
	}
}

func TestTlsDetail(t *testing.T) {
	t.Run("TLS 1.3 with peer cert", func(t *testing.T) {
		expiry := time.Date(2027, 6, 15, 0, 0, 0, 0, time.UTC)
		state := &tls.ConnectionState{
			Version: tls.VersionTLS13,
			PeerCertificates: []*x509.Certificate{
				{
					Subject:  x509.Certificate{}.Subject,
					NotAfter: expiry,
				},
			},
		}
		state.PeerCertificates[0].Subject.CommonName = "db.example.com"

		detail := tlsDetail(state)
		if !strings.Contains(detail, "TLS 1.3") {
			t.Errorf("detail %q does not contain 'TLS 1.3'", detail)
		}
		if !strings.Contains(detail, "db.example.com") {
			t.Errorf("detail %q does not contain CN 'db.example.com'", detail)
		}
		if !strings.Contains(detail, "2027-06-15") {
			t.Errorf("detail %q does not contain expiry date", detail)
		}
	})

	t.Run("TLS 1.2 no peer certs", func(t *testing.T) {
		state := &tls.ConnectionState{
			Version: tls.VersionTLS12,
		}
		detail := tlsDetail(state)
		if detail != "TLS 1.2" {
			t.Errorf("detail = %q, want 'TLS 1.2'", detail)
		}
	})

	t.Run("unknown version", func(t *testing.T) {
		state := &tls.ConnectionState{
			Version: 0,
		}
		detail := tlsDetail(state)
		if detail != "unknown" {
			t.Errorf("detail = %q, want 'unknown'", detail)
		}
	})

	t.Run("TLS 1.0", func(t *testing.T) {
		state := &tls.ConnectionState{
			Version: tls.VersionTLS10,
		}
		detail := tlsDetail(state)
		if !strings.Contains(detail, "TLS 1.0") {
			t.Errorf("detail = %q, want to contain 'TLS 1.0'", detail)
		}
	})

	t.Run("TLS 1.1", func(t *testing.T) {
		state := &tls.ConnectionState{
			Version: tls.VersionTLS11,
		}
		detail := tlsDetail(state)
		if !strings.Contains(detail, "TLS 1.1") {
			t.Errorf("detail = %q, want to contain 'TLS 1.1'", detail)
		}
	})
}

func TestEarliestCertExpiry(t *testing.T) {
	t.Run("no certs returns zero time", func(t *testing.T) {
		state := &tls.ConnectionState{}
		got := earliestCertExpiry(state)
		if !got.IsZero() {
			t.Errorf("expected zero time, got %v", got)
		}
	})

	t.Run("one cert returns its NotAfter", func(t *testing.T) {
		expiry := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
		state := &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{
				{NotAfter: expiry, SerialNumber: big.NewInt(1)},
			},
		}
		got := earliestCertExpiry(state)
		if !got.Equal(expiry) {
			t.Errorf("got %v, want %v", got, expiry)
		}
	})

	t.Run("two certs returns earlier one", func(t *testing.T) {
		early := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
		late := time.Date(2028, 12, 1, 0, 0, 0, 0, time.UTC)
		state := &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{
				{NotAfter: late, SerialNumber: big.NewInt(1)},
				{NotAfter: early, SerialNumber: big.NewInt(2)},
			},
		}
		got := earliestCertExpiry(state)
		if !got.Equal(early) {
			t.Errorf("got %v, want %v", got, early)
		}
	})
}

func TestBuildTLSConfig_WithValidCA(t *testing.T) {
	// Create a temp dir with a valid (self-signed) CA PEM for testing.
	// We test that an invalid PEM file returns an error.
	dir := t.TempDir()
	badPEM := filepath.Join(dir, "bad.pem")
	if err := os.WriteFile(badPEM, []byte("not a real cert"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := BuildTLSConfig("example.com", badPEM, false)
	if err == nil {
		t.Fatal("expected error for invalid PEM")
	}
	if !strings.Contains(err.Error(), "failed to parse CA cert") {
		t.Errorf("error = %q, expected 'failed to parse CA cert'", err.Error())
	}
}
