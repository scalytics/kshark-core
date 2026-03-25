package probe

import (
	"strings"
	"testing"
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
