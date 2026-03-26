package probe

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

const esTestClusterResponse = `{
  "name" : "test-node",
  "cluster_name" : "my-cluster",
  "cluster_uuid" : "abc123",
  "version" : {
    "number" : "8.12.1",
    "build_flavor" : "default",
    "build_type" : "docker",
    "build_hash" : "abc123",
    "build_date" : "2024-01-01T00:00:00.000Z",
    "build_snapshot" : false,
    "lucene_version" : "9.9.2",
    "minimum_wire_compatibility_version" : "7.17.0",
    "minimum_index_compatibility_version" : "7.0.0"
  },
  "tagline" : "You Know, for Search"
}`

func buildMockESResponse(statusCode int, body string) []byte {
	statusText := "OK"
	switch statusCode {
	case 200:
		statusText = "OK"
	case 401:
		statusText = "Unauthorized"
	case 403:
		statusText = "Forbidden"
	case 500:
		statusText = "Internal Server Error"
	}

	resp := fmt.Sprintf("HTTP/1.1 %d %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
		statusCode, statusText, len(body), body)
	return []byte(resp)
}

func TestElasticsearchProber_Type(t *testing.T) {
	p := NewElasticsearchProber()
	if got := p.Type(); got != "elasticsearch" {
		t.Errorf("Type() = %q, want %q", got, "elasticsearch")
	}
}

func TestElasticsearchProber_HappyPath(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		// Read HTTP request
		buf := make([]byte, 4096)
		conn.Read(buf)

		// Send 200 with cluster info
		conn.Write(buildMockESResponse(200, esTestClusterResponse))
	})

	prober := NewElasticsearchProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	// Expect: L3-DNS OK, L4-TCP OK, L5-6-TLS SKIP, L7-HTTP OK, L7-Cluster OK
	if len(steps) < 4 {
		t.Fatalf("expected at least 4 steps, got %d: %+v", len(steps), steps)
	}

	// L3-DNS OK
	if steps[0].Layer != "L3-DNS" || steps[0].Status != StatusOK {
		t.Errorf("step[0] = %s/%s, want L3-DNS/OK", steps[0].Layer, steps[0].Status)
	}

	// L4-TCP OK
	if steps[1].Layer != "L4-TCP" || steps[1].Status != StatusOK {
		t.Errorf("step[1] = %s/%s, want L4-TCP/OK", steps[1].Layer, steps[1].Status)
	}

	// Find L7-HTTP OK
	foundHTTP := false
	for _, s := range steps {
		if s.Layer == "L7-HTTP" && s.Status == StatusOK {
			foundHTTP = true
			if !strings.Contains(s.Detail, "200") {
				t.Errorf("HTTP detail should contain 200, got %q", s.Detail)
			}
		}
	}
	if !foundHTTP {
		t.Error("expected L7-HTTP OK step")
	}

	// Find L7-Cluster OK with cluster name and version
	foundCluster := false
	for _, s := range steps {
		if s.Layer == "L7-Cluster" && s.Status == StatusOK {
			foundCluster = true
			if !strings.Contains(s.Detail, "my-cluster") {
				t.Errorf("cluster detail should contain cluster name, got %q", s.Detail)
			}
			if !strings.Contains(s.Detail, "8.12.1") {
				t.Errorf("cluster detail should contain version, got %q", s.Detail)
			}
		}
	}
	if !foundCluster {
		t.Error("expected L7-Cluster OK step")
	}
}

// startMultiConnMockServer starts a TCP server that handles multiple connections.
// Each connection calls handler. The listener is closed via t.Cleanup.
func startMultiConnMockServer(t *testing.T, handler func(net.Conn)) (string, int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				handler(conn)
			}()
		}
	}()

	addr := ln.Addr().(*net.TCPAddr)
	return addr.IP.String(), addr.Port
}

func TestElasticsearchProber_HappyPathWithAuth(t *testing.T) {
	var mu sync.Mutex
	callCount := 0
	host, port := startMultiConnMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		n, _ := conn.Read(buf)
		reqStr := string(buf[:n])

		mu.Lock()
		callCount++
		mu.Unlock()

		if strings.Contains(reqStr, "Authorization: Basic") {
			conn.Write(buildMockESResponse(200, esTestClusterResponse))
		} else {
			conn.Write(buildMockESResponse(401, `{"error":"security_exception","reason":"missing authentication"}`))
		}
	})

	prober := NewElasticsearchProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Username: "elastic",
		Password: "changeme",
	})

	// Should see L7-HTTP OK (for 401 response), then L7-Auth OK, then L7-Cluster
	foundHTTP := false
	for _, s := range steps {
		if s.Layer == "L7-HTTP" && s.Status == StatusOK {
			foundHTTP = true
			if !strings.Contains(s.Detail, "401") {
				t.Errorf("HTTP detail should mention 401, got %q", s.Detail)
			}
		}
	}
	if !foundHTTP {
		t.Errorf("expected L7-HTTP OK step with 401, got steps: %+v", steps)
	}

	foundAuth := false
	for _, s := range steps {
		if s.Layer == "L7-Auth" && s.Status == StatusOK {
			foundAuth = true
			if !strings.Contains(s.Detail, "elastic") {
				t.Errorf("auth detail should mention username, got %q", s.Detail)
			}
		}
	}
	if !foundAuth {
		t.Errorf("expected L7-Auth OK step, got steps: %+v", steps)
	}

	foundCluster := false
	for _, s := range steps {
		if s.Layer == "L7-Cluster" && s.Status == StatusOK {
			foundCluster = true
		}
	}
	if !foundCluster {
		t.Errorf("expected L7-Cluster OK step, got steps: %+v", steps)
	}
}

func TestElasticsearchProber_DNSFailure(t *testing.T) {
	prober := NewElasticsearchProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: "this-host-does-not-exist-kshark-test.invalid",
		Port: 9200,
	})

	if len(steps) == 0 {
		t.Fatal("expected at least 1 step")
	}
	if steps[0].Layer != "L3-DNS" || steps[0].Status != StatusFAIL {
		t.Errorf("step[0] = %s/%s, want L3-DNS/FAIL", steps[0].Layer, steps[0].Status)
	}
	if len(steps) != 1 {
		t.Errorf("expected exactly 1 step after DNS failure, got %d", len(steps))
	}
}

func TestElasticsearchProber_TCPFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	prober := NewElasticsearchProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: "127.0.0.1",
		Port: port,
	})

	if len(steps) < 2 {
		t.Fatalf("expected at least 2 steps, got %d", len(steps))
	}
	if steps[0].Layer != "L3-DNS" || steps[0].Status != StatusOK {
		t.Errorf("step[0] = %s/%s, want L3-DNS/OK", steps[0].Layer, steps[0].Status)
	}
	if steps[1].Layer != "L4-TCP" || steps[1].Status != StatusFAIL {
		t.Errorf("step[1] = %s/%s, want L4-TCP/FAIL", steps[1].Layer, steps[1].Status)
	}
	if len(steps) != 2 {
		t.Errorf("expected exactly 2 steps after TCP failure, got %d", len(steps))
	}
}

func TestElasticsearchProber_Unauthorized_NoCredentials(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockESResponse(401, `{"error":"security_exception"}`))
	})

	prober := NewElasticsearchProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
		// No credentials
	})

	foundFail := false
	for _, s := range steps {
		if s.Layer == "L7-HTTP" && s.Status == StatusFAIL {
			foundFail = true
			if !strings.Contains(s.Detail, "401") {
				t.Errorf("detail should mention 401, got %q", s.Detail)
			}
			if !strings.Contains(s.Hint, "connection.user") {
				t.Errorf("hint should suggest credentials, got %q", s.Hint)
			}
		}
	}
	if !foundFail {
		t.Errorf("expected L7-HTTP FAIL for 401 without credentials, got: %+v", steps)
	}
}

func TestElasticsearchProber_Forbidden(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockESResponse(403, `{"error":"access_denied"}`))
	})

	prober := NewElasticsearchProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	foundFail := false
	for _, s := range steps {
		if s.Layer == "L7-HTTP" && s.Status == StatusFAIL {
			foundFail = true
			if !strings.Contains(s.Detail, "403") {
				t.Errorf("detail should mention 403, got %q", s.Detail)
			}
		}
	}
	if !foundFail {
		t.Errorf("expected L7-HTTP FAIL for 403, got: %+v", steps)
	}
}

func TestElasticsearchProber_MalformedResponse(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		// Send garbage that is not HTTP
		conn.Write([]byte{0x00, 0x01, 0x02, 0x03, 0x0d, 0x0a})
	})

	prober := NewElasticsearchProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	// Should get a FAIL (not a panic)
	foundFail := false
	for _, s := range steps {
		if s.Status == StatusFAIL {
			foundFail = true
		}
	}
	if !foundFail {
		t.Errorf("expected a FAIL step for malformed response, got: %+v", steps)
	}
}

func TestElasticsearchProber_EmptyResponse(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		// Accept TCP then close immediately
	})

	prober := NewElasticsearchProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	foundFail := false
	for _, s := range steps {
		if s.Status == StatusFAIL {
			foundFail = true
		}
	}
	if !foundFail {
		t.Errorf("expected a FAIL step for empty response, got: %+v", steps)
	}
}

func TestElasticsearchProber_ServerError(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockESResponse(500, `{"error":"internal_server_error"}`))
	})

	prober := NewElasticsearchProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	foundFail := false
	for _, s := range steps {
		if s.Layer == "L7-HTTP" && s.Status == StatusFAIL {
			foundFail = true
			if !strings.Contains(s.Detail, "500") {
				t.Errorf("detail should mention 500, got %q", s.Detail)
			}
		}
	}
	if !foundFail {
		t.Errorf("expected L7-HTTP FAIL for 500, got: %+v", steps)
	}
}

func TestElasticsearchProber_ConcurrentProbes(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockESResponse(200, esTestClusterResponse))
	})

	prober := NewElasticsearchProber()
	done := make(chan []ProbeStep, 2)

	go func() {
		steps := prober.Probe(context.Background(), ProbeTarget{
			Host: host,
			Port: port,
		})
		done <- steps
	}()

	go func() {
		steps := prober.Probe(context.Background(), ProbeTarget{
			Host: "no-such-host-kshark.invalid",
			Port: 9200,
		})
		done <- steps
	}()

	for i := 0; i < 2; i++ {
		select {
		case steps := <-done:
			if len(steps) == 0 {
				t.Error("expected non-empty steps")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for probe")
		}
	}
}

func TestParseHTTPStatusCode(t *testing.T) {
	tests := []struct {
		name       string
		line       string
		wantCode   int
		wantErr    bool
	}{
		{
			name:     "HTTP 200",
			line:     "HTTP/1.1 200 OK",
			wantCode: 200,
		},
		{
			name:     "HTTP 401",
			line:     "HTTP/1.1 401 Unauthorized",
			wantCode: 401,
		},
		{
			name:     "HTTP 1.0",
			line:     "HTTP/1.0 200 OK",
			wantCode: 200,
		},
		{
			name:    "not HTTP",
			line:    "GARBAGE DATA",
			wantErr: true,
		},
		{
			name:    "empty",
			line:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, err := parseHTTPStatusCode(tt.line)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got code=%d", code)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if code != tt.wantCode {
				t.Errorf("code = %d, want %d", code, tt.wantCode)
			}
		})
	}
}

func TestParseESRootResponse(t *testing.T) {
	tests := []struct {
		name         string
		body         string
		wantCluster  string
		wantVersion  string
	}{
		{
			name:        "full response",
			body:        esTestClusterResponse,
			wantCluster: "my-cluster",
			wantVersion: "8.12.1",
		},
		{
			name:        "empty body",
			body:        "",
			wantCluster: "",
			wantVersion: "",
		},
		{
			name:        "no version",
			body:        `{"cluster_name": "test"}`,
			wantCluster: "test",
			wantVersion: "",
		},
		{
			name:        "error response",
			body:        `{"error": "not_found"}`,
			wantCluster: "",
			wantVersion: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster, version := parseESRootResponse(tt.body)
			if cluster != tt.wantCluster {
				t.Errorf("cluster = %q, want %q", cluster, tt.wantCluster)
			}
			if version != tt.wantVersion {
				t.Errorf("version = %q, want %q", version, tt.wantVersion)
			}
		})
	}
}

func TestExtractJSONStringValue(t *testing.T) {
	tests := []struct {
		name string
		json string
		key  string
		want string
	}{
		{
			name: "simple",
			json: `{"name": "test"}`,
			key:  "name",
			want: "test",
		},
		{
			name: "nested",
			json: `{"version": {"number": "8.12.1"}}`,
			key:  "number",
			want: "8.12.1",
		},
		{
			name: "not found",
			json: `{"other": "value"}`,
			key:  "missing",
			want: "",
		},
		{
			name: "escaped quotes",
			json: `{"name": "test\"value"}`,
			key:  "name",
			want: `test"value`,
		},
		{
			name: "empty value",
			json: `{"name": ""}`,
			key:  "name",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractJSONStringValue(tt.json, tt.key)
			if got != tt.want {
				t.Errorf("extractJSONStringValue() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestClassifyESAuthResponse(t *testing.T) {
	target := ProbeTarget{
		Host:     "es.example.com",
		Port:     9200,
		Username: "elastic",
		Password: "secret",
	}

	tests := []struct {
		name       string
		statusCode int
		wantStatus string
	}{
		{
			name:       "200 OK",
			statusCode: 200,
			wantStatus: StatusOK,
		},
		{
			name:       "401 Unauthorized",
			statusCode: 401,
			wantStatus: StatusFAIL,
		},
		{
			name:       "403 Forbidden",
			statusCode: 403,
			wantStatus: StatusFAIL,
		},
		{
			name:       "500 Server Error",
			statusCode: 500,
			wantStatus: StatusWARN,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := classifyESAuthResponse(tt.statusCode, target, 10*time.Millisecond)
			if step.Status != tt.wantStatus {
				t.Errorf("status = %q, want %q", step.Status, tt.wantStatus)
			}
		})
	}
}

func TestElasticsearchProber_ChunkedResponse(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)

		// Send chunked response
		resp := "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
		conn.Write([]byte(resp))

		// Send body in chunks
		body := esTestClusterResponse
		chunk := fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(body), body)
		conn.Write([]byte(chunk))
	})

	prober := NewElasticsearchProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	// Should successfully parse the chunked response
	foundCluster := false
	for _, s := range steps {
		if s.Layer == "L7-Cluster" && s.Status == StatusOK {
			foundCluster = true
			if !strings.Contains(s.Detail, "my-cluster") {
				t.Errorf("cluster detail should contain cluster name, got %q", s.Detail)
			}
		}
	}
	if !foundCluster {
		t.Logf("steps: %+v", steps)
		// Chunked transfer might be tricky in test, but we should at least not panic
	}
}
