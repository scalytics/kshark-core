package connectapi

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewConnectClient(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		auth    ConnectAuthOpts
		wantErr bool
		errSub  string
	}{
		{
			name:    "valid http URL",
			url:     "http://localhost:8083",
			auth:    ConnectAuthOpts{},
			wantErr: false,
		},
		{
			name:    "valid https URL",
			url:     "https://connect.example.com:8083",
			auth:    ConnectAuthOpts{},
			wantErr: false,
		},
		{
			name:    "invalid scheme ftp",
			url:     "ftp://connect.example.com:8083",
			auth:    ConnectAuthOpts{},
			wantErr: true,
			errSub:  "http or https",
		},
		{
			name:    "unparseable URL",
			url:     "://missing-scheme",
			auth:    ConnectAuthOpts{},
			wantErr: true,
			errSub:  "missing protocol scheme",
		},
		{
			name:    "empty CACertPath is OK",
			url:     "http://localhost:8083",
			auth:    ConnectAuthOpts{CACertPath: ""},
			wantErr: false,
		},
		{
			name:    "CACertPath nonexistent file",
			url:     "https://localhost:8083",
			auth:    ConnectAuthOpts{CACertPath: "/nonexistent/path/ca.pem"},
			wantErr: true,
			errSub:  "failed to read Connect CA cert",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewConnectClient(tt.url, tt.auth)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				if tt.errSub != "" && !strings.Contains(err.Error(), tt.errSub) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if client == nil {
				t.Fatal("expected non-nil client")
			}
		})
	}
}

func TestNewConnectClient_InvalidCACert(t *testing.T) {
	// Create a temp file that contains invalid PEM data
	dir := t.TempDir()
	badCert := filepath.Join(dir, "bad.pem")
	if err := os.WriteFile(badCert, []byte("not a valid cert"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := NewConnectClient("https://localhost:8083", ConnectAuthOpts{CACertPath: badCert})
	if err == nil {
		t.Fatal("expected error for invalid PEM data")
	}
	if !strings.Contains(err.Error(), "failed to parse Connect CA cert") {
		t.Errorf("error %q should mention failed to parse", err.Error())
	}
}

func TestIsLinkLocalIP(t *testing.T) {
	tests := []struct {
		name string
		host string
		want bool
	}{
		{
			name: "link-local metadata IP",
			host: "169.254.169.254",
			want: true,
		},
		{
			name: "link-local 169.254.1.1",
			host: "169.254.1.1",
			want: true,
		},
		{
			name: "public IP 8.8.8.8",
			host: "8.8.8.8",
			want: false,
		},
		{
			name: "unresolvable host returns false",
			host: "thishostshouldnotexist.invalid",
			want: false,
		},
		{
			name: "loopback 127.0.0.1",
			host: "127.0.0.1",
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isLinkLocalIP(tt.host)
			if got != tt.want {
				t.Errorf("isLinkLocalIP(%q) = %v, want %v", tt.host, got, tt.want)
			}
		})
	}
}

func TestIsLinkLocalIP_Localhost(t *testing.T) {
	// localhost resolves to 127.0.0.1 which is now in the deny list (loopback)
	got := isLinkLocalIP("localhost")
	if !got {
		t.Error("localhost (127.0.0.1) should be blocked as loopback")
	}
}

func TestApplyAuth_BasicAuth(t *testing.T) {
	client := &ConnectClient{
		auth: ConnectAuthOpts{BasicAuth: "user:pass"},
	}
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8083/connectors", nil)
	client.applyAuth(req)

	authHeader := req.Header.Get("Authorization")
	if authHeader == "" {
		t.Fatal("expected Authorization header to be set")
	}
	if !strings.HasPrefix(authHeader, "Basic ") {
		t.Errorf("expected Basic auth prefix, got %q", authHeader)
	}
}

func TestApplyAuth_BearerToken(t *testing.T) {
	client := &ConnectClient{
		auth: ConnectAuthOpts{BearerToken: "my-token-123"},
	}
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8083/connectors", nil)
	client.applyAuth(req)

	authHeader := req.Header.Get("Authorization")
	if authHeader != "Bearer my-token-123" {
		t.Errorf("Authorization = %q, want %q", authHeader, "Bearer my-token-123")
	}
}

func TestApplyAuth_Both(t *testing.T) {
	// When both are set, BasicAuth sets via SetBasicAuth, and BearerToken
	// overwrites the Authorization header because it runs second.
	client := &ConnectClient{
		auth: ConnectAuthOpts{
			BasicAuth:   "user:pass",
			BearerToken: "my-token",
		},
	}
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8083/connectors", nil)
	client.applyAuth(req)

	// BearerToken is applied after BasicAuth, so it overwrites the Authorization header
	authHeader := req.Header.Get("Authorization")
	if authHeader != "Bearer my-token" {
		t.Errorf("Authorization = %q, want Bearer token to take precedence", authHeader)
	}
}

func TestApplyAuth_Neither(t *testing.T) {
	client := &ConnectClient{
		auth: ConnectAuthOpts{},
	}
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8083/connectors", nil)
	client.applyAuth(req)

	authHeader := req.Header.Get("Authorization")
	if authHeader != "" {
		t.Errorf("Authorization should be empty, got %q", authHeader)
	}
}
