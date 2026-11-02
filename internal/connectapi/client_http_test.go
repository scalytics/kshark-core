package connectapi

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func routableLoopback(t *testing.T) {
	t.Helper()
	prev := lookupHost
	lookupHost = func(host string) ([]string, error) {
		if host == "127.0.0.1" {
			return []string{"10.0.0.10"}, nil
		}
		return prev(host)
	}
	t.Cleanup(func() { lookupHost = prev })
}

func connectServer(t *testing.T, handler http.HandlerFunc) *ConnectClient {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c, err := NewConnectClient(srv.URL+"/", ConnectAuthOpts{BasicAuth: "admin:secret"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestGetConnectorConfig_Responses(t *testing.T) {
	routableLoopback(t)
	tests := []struct {
		name    string
		code    int
		body    string
		want    map[string]string
		wantErr string
	}{
		{"ok", http.StatusOK, `{"connector.class":"JdbcSink","tasks.max":"1"}`, map[string]string{"connector.class": "JdbcSink", "tasks.max": "1"}, ""},
		{"bad json", http.StatusOK, `{"tasks.max":1}`, nil, "failed to decode connector config"},
		{"unauthorized", http.StatusUnauthorized, "", nil, "Connect API auth failed (HTTP 401)"},
		{"forbidden", http.StatusForbidden, "", nil, "Connect API auth failed (HTTP 403)"},
		{"not found", http.StatusNotFound, "", nil, "connector 'my sink' not found"},
		{"server error", http.StatusInternalServerError, "boom", nil, "Connect API returned HTTP 500: boom"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := connectServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.EscapedPath() != "/connectors/my%20sink/config" {
					t.Errorf("path = %q", r.URL.EscapedPath())
				}
				if u, p, ok := r.BasicAuth(); !ok || u != "admin" || p != "secret" {
					t.Errorf("basic auth = %q/%q/%v", u, p, ok)
				}
				if r.Header.Get("Accept") != "application/json" {
					t.Errorf("accept = %q", r.Header.Get("Accept"))
				}
				w.WriteHeader(tt.code)
				_, _ = w.Write([]byte(tt.body))
			})
			got, err := c.GetConnectorConfig(context.Background(), "my sink")
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("config = %v, want %v", got, tt.want)
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("config[%s] = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

func TestGetConnectorConfig_Blocked(t *testing.T) {
	c, err := NewConnectClient("http://169.254.169.254", ConnectAuthOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetConnectorConfig(context.Background(), "x"); err == nil || !strings.Contains(err.Error(), "link-local") {
		t.Errorf("err = %v, want link-local block", err)
	}
}

func TestGetConnectorConfig_Unreachable(t *testing.T) {
	routableLoopback(t)
	c, err := NewConnectClient("http://127.0.0.1:1", ConnectAuthOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetConnectorConfig(context.Background(), "x"); err == nil || !strings.Contains(err.Error(), "Connect API unreachable") {
		t.Errorf("err = %v", err)
	}
	if _, err := c.ListConnectors(context.Background()); err == nil || !strings.Contains(err.Error(), "Connect API unreachable") {
		t.Errorf("err = %v", err)
	}
}

func TestGetConnectorConfig_RedirectNotFollowed(t *testing.T) {
	routableLoopback(t)
	c := connectServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data", http.StatusFound)
	})
	if _, err := c.GetConnectorConfig(context.Background(), "x"); err == nil || !strings.Contains(err.Error(), "redirects are not followed") {
		t.Errorf("err = %v, want redirect block", err)
	}
}

func TestListConnectors(t *testing.T) {
	tests := []struct {
		name    string
		code    int
		body    string
		want    []string
		wantErr string
	}{
		{"ok", http.StatusOK, `["a","b"]`, []string{"a", "b"}, ""},
		{"error status", http.StatusServiceUnavailable, "", nil, "Connect API returned HTTP 503"},
		{"bad body", http.StatusOK, `{}`, nil, "failed to decode connector list"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotAuth string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotAuth = r.Header.Get("Authorization")
				if r.URL.Path != "/connectors" {
					t.Errorf("path = %q", r.URL.Path)
				}
				w.WriteHeader(tt.code)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()
			c, err := NewConnectClient(srv.URL, ConnectAuthOpts{BearerToken: "tok"})
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.ListConnectors(context.Background())
			if gotAuth != "Bearer tok" {
				t.Errorf("Authorization = %q", gotAuth)
			}
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Errorf("got %v, %v; want %v", got, err, tt.want)
			}
		})
	}
}

func TestNewConnectClient_CACert(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`["secure"]`))
	}))
	defer srv.Close()
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	certPEM := pemEncodeCert(srv.Certificate().Raw)
	if err := os.WriteFile(caPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := NewConnectClient(srv.URL, ConnectAuthOpts{CACertPath: caPath})
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.ListConnectors(context.Background())
	if err != nil || len(got) != 1 || got[0] != "secure" {
		t.Errorf("got %v, %v", got, err)
	}

	badPath := filepath.Join(dir, "bad.pem")
	if err := os.WriteFile(badPath, []byte("not a cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewConnectClient(srv.URL, ConnectAuthOpts{CACertPath: badPath}); err == nil || !strings.Contains(err.Error(), "failed to parse Connect CA cert") {
		t.Errorf("err = %v", err)
	}
	if _, err := NewConnectClient(srv.URL, ConnectAuthOpts{CACertPath: filepath.Join(dir, "missing.pem")}); err == nil || !strings.Contains(err.Error(), "failed to read Connect CA cert") {
		t.Errorf("err = %v", err)
	}
}

func TestParseJDBCURL_OracleForms(t *testing.T) {
	tests := []struct {
		url      string
		host     string
		port     int
		database string
		wantErr  string
	}{
		{"jdbc:oracle:thin:@db.example.com:1522/ORCLPDB", "db.example.com", 1522, "ORCLPDB", ""},
		{"jdbc:oracle:thin:@//db.example.com:1523/svc", "db.example.com", 1523, "svc", ""},
		{"jdbc:oracle:thin:@db.example.com/svc", "db.example.com", 1521, "svc", ""},
		{"jdbc:oracle:thin:@db.example.com:1524:ORCL", "db.example.com", 1524, "ORCL", ""},
		{"jdbc:oracle:thin:@db.example.com:1525", "db.example.com", 1525, "", ""},
		{"jdbc:oracle:thin:@db.example.com:ORCL", "db.example.com", 1521, "ORCL", ""},
		{"jdbc:oracle:thin:@db.example.com", "db.example.com", 1521, "", ""},
		{"jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=tns.example.com)(PORT=1530))(CONNECT_DATA=(SERVICE_NAME=sales)))", "tns.example.com", 1530, "sales", ""},
		{"jdbc:oracle:thin:@db.example.com:abc/svc", "", 0, "", "invalid port in Oracle URL: abc"},
		{"jdbc:oracle:thin:@db.example.com:abc:ORCL", "", 0, "", "invalid port in Oracle URL: abc"},
	}
	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			p, err := ParseJDBCURL(tt.url)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if p.Dialect != "oracle" || p.Host != tt.host || p.Port != tt.port || p.Database != tt.database || p.Props["driverType"] != "thin" {
				t.Errorf("parsed = %+v", p)
			}
		})
	}
}

func TestRedactMongoURI_Unparseable(t *testing.T) {
	got := RedactMongoURI("mongodb://user:p%zz@host:27017/db")
	if got != "mongodb://[REDACTED]@host:27017/db" {
		t.Errorf("got %q", got)
	}
	if got := RedactMongoURI("::%zz"); got != "::%zz" {
		t.Errorf("got %q", got)
	}
}

func pemEncodeCert(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
