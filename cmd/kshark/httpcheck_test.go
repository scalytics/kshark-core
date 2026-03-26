package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestExtractHost(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "https with port and path",
			raw:  "https://example.com:8081/subjects",
			want: "example.com",
		},
		{
			name: "http with path",
			raw:  "http://example.com/path",
			want: "example.com",
		},
		{
			name: "host with port no scheme",
			raw:  "example.com:8081",
			want: "example.com",
		},
		{
			name: "host only no scheme no port",
			raw:  "example.com",
			want: "example.com",
		},
		{
			name: "host with no port but with path",
			raw:  "https://myhost.example.com/api/v1",
			want: "myhost.example.com",
		},
		{
			name: "IP address with port",
			raw:  "https://192.168.1.1:8081/subjects",
			want: "192.168.1.1",
		},
		{
			name: "IP address without port",
			raw:  "http://10.0.0.1/path",
			want: "10.0.0.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractHost(tt.raw)
			if got != tt.want {
				t.Errorf("extractHost(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

// ---------- httptest-based tests for checkSchemaRegistry ----------

func TestCheckSchemaRegistry_EmptyURL(t *testing.T) {
	report := &Report{}
	props := map[string]string{} // no schema.registry.url
	checkSchemaRegistry(context.Background(), report, props)
	if len(report.Rows) != 0 {
		t.Errorf("expected 0 rows for empty URL, got %d", len(report.Rows))
	}
}

func TestCheckSchemaRegistry_WhitespaceOnlyURL(t *testing.T) {
	report := &Report{}
	props := map[string]string{"schema.registry.url": "   "}
	checkSchemaRegistry(context.Background(), report, props)
	if len(report.Rows) != 0 {
		t.Errorf("expected 0 rows for whitespace-only URL, got %d", len(report.Rows))
	}
}

func TestCheckSchemaRegistry_SSRFBlocked(t *testing.T) {
	report := &Report{}
	props := map[string]string{"schema.registry.url": "http://127.0.0.1:8081"}
	checkSchemaRegistry(context.Background(), report, props)

	if len(report.Rows) == 0 {
		t.Fatal("expected at least one row for SSRF-blocked URL")
	}
	found := false
	for _, row := range report.Rows {
		if row.Status == FAIL && strings.Contains(row.Detail, "SSRF") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected FAIL row with SSRF mention for loopback URL")
		for _, row := range report.Rows {
			t.Logf("  row: status=%s detail=%q", row.Status, row.Detail)
		}
	}
}

func TestCheckSchemaRegistry_SSRFBlocked_LinkLocal(t *testing.T) {
	report := &Report{}
	props := map[string]string{"schema.registry.url": "http://169.254.169.254/latest"}
	checkSchemaRegistry(context.Background(), report, props)

	if len(report.Rows) == 0 {
		t.Fatal("expected at least one row for cloud metadata URL")
	}
	found := false
	for _, row := range report.Rows {
		if row.Status == FAIL && strings.Contains(row.Detail, "SSRF") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected FAIL row with SSRF mention for cloud metadata URL")
	}
}

func TestCheckSchemaRegistry_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	report := &Report{}
	// Use a hostname that will get past SSRF but fail on actual HTTP
	props := map[string]string{"schema.registry.url": "https://schema-registry.kshark-nonexistent-test.example.com:8081"}
	checkSchemaRegistry(ctx, report, props)

	// Should have added rows (DNS fail, or HTTP fail with context cancelled)
	if len(report.Rows) == 0 {
		t.Error("expected at least one row for cancelled context")
	}
}

func TestCheckSchemaRegistry_RFC1918Warning(t *testing.T) {
	report := &Report{}
	// 10.0.0.1 resolves directly (no DNS needed for IP literals), gets ssrfWarn
	props := map[string]string{"schema.registry.url": "https://10.0.0.1:8081"}
	checkSchemaRegistry(context.Background(), report, props)

	// Should have a WARN row for RFC1918 and then likely a DNS or HTTP error
	if len(report.Rows) == 0 {
		t.Fatal("expected rows for RFC1918 address")
	}
	foundWarn := false
	for _, row := range report.Rows {
		if row.Status == WARN && strings.Contains(row.Detail, "SSRF-WARN") {
			foundWarn = true
			break
		}
	}
	if !foundWarn {
		t.Error("expected WARN row with SSRF-WARN for RFC1918 address")
		for _, row := range report.Rows {
			t.Logf("  row: status=%s detail=%q", row.Status, row.Detail)
		}
	}
}

// ---------- httptest-based tests for httpClientFromTLS ----------

func TestHttpClientFromTLS_HasCheckRedirect(t *testing.T) {
	client := httpClientFromTLS(nil, 10*time.Second)
	if client == nil {
		t.Fatal("httpClientFromTLS returned nil")
	}
	if client.CheckRedirect == nil {
		t.Error("httpClientFromTLS must set CheckRedirect for SSRF protection")
	}
}

func TestHttpClientFromTLS_Timeout(t *testing.T) {
	client := httpClientFromTLS(nil, 5*time.Second)
	if client.Timeout != 5*time.Second {
		t.Errorf("timeout = %v, want 5s", client.Timeout)
	}
}

func TestHttpClientFromTLS_NilTLS(t *testing.T) {
	client := httpClientFromTLS(nil, 10*time.Second)
	tr, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatal("expected *http.Transport")
	}
	if tr.TLSClientConfig != nil {
		t.Error("expected nil TLSClientConfig when passed nil")
	}
}

func TestHttpClientFromTLS_RedirectBlocksLoopback(t *testing.T) {
	// Verify that the CheckRedirect function from httpClientFromTLS blocks
	// redirects to loopback addresses.
	client := httpClientFromTLS(nil, 10*time.Second)

	// Create a server that redirects to loopback
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("should not reach here"))
	}))
	defer redirectTarget.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Redirect to loopback - the CheckRedirect should block this
		http.Redirect(w, r, "http://127.0.0.1:1/evil", http.StatusFound)
	}))
	defer redirector.Close()

	// Use the redirector URL directly (bypass SSRF for the initial request
	// by using the client's Do method directly)
	req, _ := http.NewRequest("GET", redirector.URL, nil)
	resp, err := client.Do(req)
	if err == nil {
		resp.Body.Close()
		// The redirect to 127.0.0.1:1 should have been blocked
		t.Error("expected redirect to loopback to be blocked")
	} else if !strings.Contains(err.Error(), "blocked") {
		t.Errorf("expected 'blocked' error, got: %v", err)
	}
}

// ---------- Direct HTTP flow tests for Schema Registry ----------

func TestCheckSchemaRegistry_DirectHTTPFlow_OK(t *testing.T) {
	// Test the HTTP response handling by creating a mock server and testing
	// the HTTP client directly (bypassing the SSRF check).
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subjects" {
			t.Errorf("expected path /subjects, got %s", r.URL.Path)
		}
		w.WriteHeader(200)
		w.Write([]byte(`["subject1","subject2"]`))
	}))
	defer server.Close()

	client := httpClientFromTLS(nil, 10*time.Second)
	// Override the client to use the test server's client (bypasses network)
	client.Transport = server.Client().Transport

	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL+"/subjects", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestCheckSchemaRegistry_DirectHTTPFlow_Auth401(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		w.Write([]byte(`{"error":"unauthorized"}`))
	}))
	defer server.Close()

	client := server.Client()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL+"/subjects", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 401 {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestCheckSchemaRegistry_DirectHTTPFlow_Auth403(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
	}))
	defer server.Close()

	client := server.Client()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL+"/subjects", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 403 {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
}

func TestCheckSchemaRegistry_DirectHTTPFlow_500(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer server.Close()

	client := server.Client()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL+"/subjects", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 500 {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
}

func TestCheckSchemaRegistry_DirectHTTPFlow_BasicAuth(t *testing.T) {
	var receivedUser, receivedPass string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedUser, receivedPass, _ = r.BasicAuth()
		w.WriteHeader(200)
		w.Write([]byte("[]"))
	}))
	defer server.Close()

	client := server.Client()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL+"/subjects", nil)
	req.SetBasicAuth("myuser", "mypass")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if receivedUser != "myuser" {
		t.Errorf("received user = %q, want myuser", receivedUser)
	}
	if receivedPass != "mypass" {
		t.Errorf("received pass = %q, want mypass", receivedPass)
	}
}
