package probe

import (
	"fmt"
	"strings"
	"testing"
)

func TestPgQuote(t *testing.T) {
	tests := []struct {
		name string
		val  string
		want string
	}{
		{
			name: "simple value no quoting needed",
			val:  "localhost",
			want: "localhost",
		},
		{
			name: "value with space",
			val:  "my host",
			want: "'my host'",
		},
		{
			name: "value with single quote",
			val:  "it's",
			want: `'it\'s'`,
		},
		{
			name: "value with backslash",
			val:  `path\to\thing`,
			want: `'path\\to\\thing'`,
		},
		{
			name: "empty string",
			val:  "",
			want: "''",
		},
		{
			name: "value with newline",
			val:  "line1\nline2",
			want: "'line1\nline2'",
		},
		{
			name: "value with tab",
			val:  "col1\tcol2",
			want: "'col1\tcol2'",
		},
		{
			name: "value with both quote and backslash",
			val:  `it\'s`,
			want: `'it\\\'s'`,
		},
		{
			name: "numeric value",
			val:  "5432",
			want: "5432",
		},
		{
			name: "hostname with dots and hyphens",
			val:  "my-db.example.com",
			want: "my-db.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pgQuote(tt.val)
			if got != tt.want {
				t.Errorf("pgQuote(%q) = %q, want %q", tt.val, got, tt.want)
			}
		})
	}
}

func TestBuildPostgresDSN(t *testing.T) {
	tests := []struct {
		name       string
		target     ProbeTarget
		wantParts  []string // substrings that must appear
		wantAbsent []string // substrings that must not appear
	}{
		{
			name: "normal target",
			target: ProbeTarget{
				Host:     "db.example.com",
				Port:     5432,
				Username: "admin",
				Password: "secret",
				Database: "mydb",
				SSLMode:  "require",
			},
			wantParts: []string{
				"host=db.example.com",
				"port=5432",
				"user=admin",
				"password=secret",
				"dbname=mydb",
				"sslmode=require",
			},
		},
		{
			name: "password with spaces is quoted",
			target: ProbeTarget{
				Host:     "localhost",
				Port:     5432,
				Username: "user",
				Password: "my secret pass",
				Database: "testdb",
				SSLMode:  "prefer",
			},
			wantParts: []string{
				"password='my secret pass'",
			},
		},
		{
			name: "empty SSLMode defaults to prefer",
			target: ProbeTarget{
				Host:     "localhost",
				Port:     5432,
				Username: "user",
				Password: "pass",
				Database: "db",
				SSLMode:  "",
			},
			wantParts: []string{
				"sslmode=prefer",
			},
		},
		{
			name: "with sslrootcert extra prop",
			target: ProbeTarget{
				Host:       "localhost",
				Port:       5432,
				Username:   "user",
				Password:   "pass",
				Database:   "db",
				SSLMode:    "verify-full",
				ExtraProps: map[string]string{"sslrootcert": "/etc/ssl/ca.pem"},
			},
			wantParts: []string{
				"sslmode=verify-full",
				"sslrootcert=/etc/ssl/ca.pem",
			},
		},
		{
			name: "without sslrootcert extra prop",
			target: ProbeTarget{
				Host:     "localhost",
				Port:     5432,
				Username: "user",
				Password: "pass",
				Database: "db",
				SSLMode:  "require",
			},
			wantAbsent: []string{
				"sslrootcert",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := buildPostgresDSN(tt.target)
			for _, part := range tt.wantParts {
				if !strings.Contains(dsn, part) {
					t.Errorf("DSN %q does not contain %q", dsn, part)
				}
			}
			for _, absent := range tt.wantAbsent {
				if strings.Contains(dsn, absent) {
					t.Errorf("DSN %q should not contain %q", dsn, absent)
				}
			}
		})
	}
}

func TestClassifyPgError(t *testing.T) {
	target := ProbeTarget{
		Host:     "db.example.com",
		Port:     5432,
		Username: "admin",
		Database: "mydb",
	}

	tests := []struct {
		name      string
		errMsg    string
		wantLayer string
		wantFail  bool
	}{
		{
			name:      "no such host -> L3-DNS FAIL",
			errMsg:    "lookup db.example.com: no such host",
			wantLayer: "L3-DNS",
			wantFail:  true,
		},
		{
			name:      "connection refused -> L4-TCP FAIL",
			errMsg:    "dial tcp 10.0.0.1:5432: connection refused",
			wantLayer: "L4-TCP",
			wantFail:  true,
		},
		{
			name:      "password authentication failed -> L7-Auth FAIL",
			errMsg:    "password authentication failed for user \"admin\"",
			wantLayer: "L7-Auth",
			wantFail:  true,
		},
		{
			name:      "database does not exist -> L7-Startup FAIL",
			errMsg:    "FATAL: database \"mydb\" does not exist",
			wantLayer: "L7-Startup",
			wantFail:  true,
		},
		{
			name:      "no pg_hba.conf entry -> L7-Auth FAIL",
			errMsg:    "no pg_hba.conf entry for host \"10.0.0.1\", user \"admin\", database \"mydb\"",
			wantLayer: "L7-Auth",
			wantFail:  true,
		},
		{
			name:      "certificate error -> L5-6-TLS FAIL",
			errMsg:    "certificate signed by unknown authority",
			wantLayer: "L5-6-TLS",
			wantFail:  true,
		},
		{
			name:      "tls error -> L5-6-TLS FAIL",
			errMsg:    "tls: handshake failure",
			wantLayer: "L5-6-TLS",
			wantFail:  true,
		},
		{
			name:      "i/o timeout -> L4-TCP FAIL",
			errMsg:    "dial tcp 10.0.0.1:5432: i/o timeout",
			wantLayer: "L4-TCP",
			wantFail:  true,
		},
		{
			name:      "context deadline exceeded -> L4-TCP FAIL",
			errMsg:    "context deadline exceeded",
			wantLayer: "L4-TCP",
			wantFail:  true,
		},
		{
			name:      "role does not exist -> L7-Startup FAIL",
			errMsg:    "FATAL: role \"admin\" does not exist",
			wantLayer: "L7-Startup",
			wantFail:  true,
		},
		{
			name:      "generic error -> L7-Ready FAIL",
			errMsg:    "some unknown error happened",
			wantLayer: "L7-Ready",
			wantFail:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := classifyPgError(fmt.Errorf("%s", tt.errMsg), target)
			if step.Layer != tt.wantLayer {
				t.Errorf("layer = %q, want %q", step.Layer, tt.wantLayer)
			}
			if tt.wantFail && step.Status != StatusFAIL {
				t.Errorf("status = %q, want FAIL", step.Status)
			}
		})
	}
}
