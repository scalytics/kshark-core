package probe

import (
	"fmt"
	"strings"
	"testing"
)

func TestParseMongoURI(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		wantHost    string
		wantPort    int
		wantUser    string
		wantPass    string
		wantDB      string
		wantSRV     bool
		wantTLS     bool
		wantErr     bool
		errContains string
	}{
		{
			name:     "standard mongodb URI",
			uri:      "mongodb://user:pass@myhost:27017/mydb",
			wantHost: "myhost",
			wantPort: 27017,
			wantUser: "user",
			wantPass: "pass",
			wantDB:   "mydb",
			wantSRV:  false,
			wantTLS:  false,
			wantErr:  false,
		},
		{
			name:     "mongodb+srv URI",
			uri:      "mongodb+srv://admin:secret@cluster0.abc.mongodb.net/analytics",
			wantHost: "cluster0.abc.mongodb.net",
			wantPort: 27017,
			wantUser: "admin",
			wantPass: "secret",
			wantDB:   "analytics",
			wantSRV:  true,
			wantTLS:  true,
			wantErr:  false,
		},
		{
			name:     "mongodb with tls=true query param",
			uri:      "mongodb://user:pass@host:27017/db?tls=true",
			wantHost: "host",
			wantPort: 27017,
			wantUser: "user",
			wantPass: "pass",
			wantDB:   "db",
			wantSRV:  false,
			wantTLS:  true,
			wantErr:  false,
		},
		{
			name:     "mongodb with ssl=true query param",
			uri:      "mongodb://user:pass@host:27017/db?ssl=true",
			wantHost: "host",
			wantPort: 27017,
			wantUser: "user",
			wantPass: "pass",
			wantDB:   "db",
			wantSRV:  false,
			wantTLS:  true,
			wantErr:  false,
		},
		{
			name:     "custom port",
			uri:      "mongodb://user:pass@host:28017/db",
			wantHost: "host",
			wantPort: 28017,
			wantUser: "user",
			wantPass: "pass",
			wantDB:   "db",
			wantSRV:  false,
			wantTLS:  false,
			wantErr:  false,
		},
		{
			name:     "no credentials",
			uri:      "mongodb://host:27017/db",
			wantHost: "host",
			wantPort: 27017,
			wantUser: "",
			wantPass: "",
			wantDB:   "db",
			wantSRV:  false,
			wantTLS:  false,
			wantErr:  false,
		},
		{
			name:        "unsupported scheme",
			uri:         "postgres://user:pass@host:5432/db",
			wantErr:     true,
			errContains: "unsupported MongoDB URI scheme",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, user, pass, db, isSRV, tlsReq, err := ParseMongoURI(tt.uri)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if host != tt.wantHost {
				t.Errorf("host = %q, want %q", host, tt.wantHost)
			}
			if port != tt.wantPort {
				t.Errorf("port = %d, want %d", port, tt.wantPort)
			}
			if user != tt.wantUser {
				t.Errorf("user = %q, want %q", user, tt.wantUser)
			}
			if pass != tt.wantPass {
				t.Errorf("pass = %q, want %q", pass, tt.wantPass)
			}
			if db != tt.wantDB {
				t.Errorf("db = %q, want %q", db, tt.wantDB)
			}
			if isSRV != tt.wantSRV {
				t.Errorf("isSRV = %v, want %v", isSRV, tt.wantSRV)
			}
			if tlsReq != tt.wantTLS {
				t.Errorf("tlsRequired = %v, want %v", tlsReq, tt.wantTLS)
			}
		})
	}
}

func TestClassifyMongoError(t *testing.T) {
	tests := []struct {
		name      string
		errMsg    string
		wantLayer string
		wantFail  bool
	}{
		{
			name:      "no such host -> L3-DNS",
			errMsg:    "lookup host.invalid: no such host",
			wantLayer: "L3-DNS",
			wantFail:  true,
		},
		{
			name:      "nxdomain -> L3-DNS",
			errMsg:    "DNS query returned NXDOMAIN",
			wantLayer: "L3-DNS",
			wantFail:  true,
		},
		{
			name:      "connection refused -> L4-TCP",
			errMsg:    "dial tcp 10.0.0.1:27017: connection refused",
			wantLayer: "L4-TCP",
			wantFail:  true,
		},
		{
			name:      "i/o timeout -> L4-TCP",
			errMsg:    "dial tcp 10.0.0.1:27017: i/o timeout",
			wantLayer: "L4-TCP",
			wantFail:  true,
		},
		{
			name:      "context deadline exceeded -> L4-TCP",
			errMsg:    "context deadline exceeded",
			wantLayer: "L4-TCP",
			wantFail:  true,
		},
		{
			name:      "server selection -> L4-TCP",
			errMsg:    "server selection error: context deadline exceeded",
			wantLayer: "L4-TCP",
			wantFail:  true,
		},
		{
			name:      "tls error -> L5-6-TLS",
			errMsg:    "tls: handshake failure",
			wantLayer: "L5-6-TLS",
			wantFail:  true,
		},
		{
			name:      "x509 certificate error -> L5-6-TLS",
			errMsg:    "x509: certificate signed by unknown authority",
			wantLayer: "L5-6-TLS",
			wantFail:  true,
		},
		{
			name:      "certificate error -> L5-6-TLS",
			errMsg:    "certificate is not valid",
			wantLayer: "L5-6-TLS",
			wantFail:  true,
		},
		{
			name:      "authentication failed -> L7-Auth",
			errMsg:    "authentication failed for user admin",
			wantLayer: "L7-Auth",
			wantFail:  true,
		},
		{
			name:      "auth error -> L7-Auth",
			errMsg:    "auth error: bad credentials",
			wantLayer: "L7-Auth",
			wantFail:  true,
		},
		{
			name:      "not authorized -> L7-Ping",
			errMsg:    "not authorized on admin to execute command",
			wantLayer: "L7-Ping",
			wantFail:  true,
		},
		{
			name:      "generic error -> L7-Ping",
			errMsg:    "some unknown error happened",
			wantLayer: "L7-Ping",
			wantFail:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := classifyMongoError(fmt.Errorf("%s", tt.errMsg))
			if step.Layer != tt.wantLayer {
				t.Errorf("layer = %q, want %q", step.Layer, tt.wantLayer)
			}
			if tt.wantFail && step.Status != StatusFAIL {
				t.Errorf("status = %q, want FAIL", step.Status)
			}
		})
	}
}

func TestBuildMongoURI(t *testing.T) {
	tests := []struct {
		name     string
		target   ProbeTarget
		wantPfx  string   // expected URI prefix
		wantParts []string // substrings that must appear
		wantAbsent []string // substrings that must not appear
	}{
		{
			name: "standard target",
			target: ProbeTarget{
				Host:     "myhost",
				Port:     27017,
				Username: "user",
				Password: "pass",
				Database: "mydb",
			},
			wantPfx: "mongodb://",
			wantParts: []string{
				"user:",
				"@myhost/mydb",
			},
		},
		{
			name: "non-default port",
			target: ProbeTarget{
				Host:     "myhost",
				Port:     28017,
				Username: "user",
				Password: "pass",
				Database: "mydb",
			},
			wantPfx: "mongodb://",
			wantParts: []string{
				"myhost:28017",
			},
		},
		{
			name: "SRV target",
			target: ProbeTarget{
				Host:     "cluster0.abc.mongodb.net",
				Port:     27017,
				Username: "admin",
				Password: "secret",
				Database: "analytics",
				SRV:      true,
			},
			wantPfx: "mongodb+srv://",
			wantParts: []string{
				"admin:",
				"@cluster0.abc.mongodb.net/analytics",
			},
		},
		{
			name: "TLS enabled non-SRV",
			target: ProbeTarget{
				Host:     "myhost",
				Port:     27017,
				Username: "user",
				Password: "pass",
				Database: "db",
				TLS:      true,
			},
			wantPfx: "mongodb://",
			wantParts: []string{
				"?tls=true",
			},
		},
		{
			name: "SRV does not add tls=true param",
			target: ProbeTarget{
				Host:     "cluster.example.com",
				Port:     27017,
				Username: "user",
				Password: "pass",
				Database: "db",
				SRV:      true,
				TLS:      true,
			},
			wantPfx: "mongodb+srv://",
			wantAbsent: []string{
				"?tls=true",
			},
		},
		{
			name: "no credentials",
			target: ProbeTarget{
				Host:     "myhost",
				Port:     27017,
				Database: "db",
			},
			wantPfx: "mongodb://",
			wantParts: []string{
				"mongodb://myhost/db",
			},
			wantAbsent: []string{
				"@",
			},
		},
		{
			name: "empty database defaults to admin",
			target: ProbeTarget{
				Host:     "myhost",
				Port:     27017,
				Username: "user",
				Password: "pass",
			},
			wantParts: []string{
				"/admin",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uri := buildMongoURI(tt.target)
			if tt.wantPfx != "" && !strings.HasPrefix(uri, tt.wantPfx) {
				t.Errorf("URI %q does not start with %q", uri, tt.wantPfx)
			}
			for _, part := range tt.wantParts {
				if !strings.Contains(uri, part) {
					t.Errorf("URI %q does not contain %q", uri, part)
				}
			}
			for _, absent := range tt.wantAbsent {
				if strings.Contains(uri, absent) {
					t.Errorf("URI %q should not contain %q", uri, absent)
				}
			}
		})
	}
}
