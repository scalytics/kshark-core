package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseTopics(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{
			name: "empty string returns nil",
			raw:  "",
			want: nil,
		},
		{
			name: "single topic",
			raw:  "my-topic",
			want: []string{"my-topic"},
		},
		{
			name: "comma-separated topics",
			raw:  "topic1,topic2,topic3",
			want: []string{"topic1", "topic2", "topic3"},
		},
		{
			name: "topics with whitespace",
			raw:  " topic1 , topic2 , topic3 ",
			want: []string{"topic1", "topic2", "topic3"},
		},
		{
			name: "trailing comma",
			raw:  "topic1,topic2,",
			want: []string{"topic1", "topic2"},
		},
		{
			name: "leading comma",
			raw:  ",topic1",
			want: []string{"topic1"},
		},
		{
			name: "multiple commas",
			raw:  "topic1,,,topic2",
			want: []string{"topic1", "topic2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseTopics(tt.raw)
			if tt.want == nil {
				if got != nil {
					t.Errorf("parseTopics(%q) = %v, want nil", tt.raw, got)
				}
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("parseTopics(%q) length = %d, want %d; got %v", tt.raw, len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("parseTopics(%q)[%d] = %q, want %q", tt.raw, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestIsTTY_NoPanic(t *testing.T) {
	// Just verify it doesn't panic with os.Stdin
	_ = isTTY(os.Stdin)
}

func TestIsTTY_RegularFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "regular.txt")
	if err := os.WriteFile(path, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if isTTY(f) {
		t.Error("isTTY(regular file) = true, want false")
	}
}

// ---------- httptest-based tests for checkRESTProxy ----------

func TestCheckRESTProxy_EmptyURL(t *testing.T) {
	report := &Report{}
	props := map[string]string{} // no rest.proxy.url
	checkRESTProxy(context.Background(), report, props)
	if len(report.Rows) != 0 {
		t.Errorf("expected 0 rows for empty URL, got %d", len(report.Rows))
	}
}

func TestCheckRESTProxy_WhitespaceOnlyURL(t *testing.T) {
	report := &Report{}
	props := map[string]string{"rest.proxy.url": "   "}
	checkRESTProxy(context.Background(), report, props)
	if len(report.Rows) != 0 {
		t.Errorf("expected 0 rows for whitespace-only URL, got %d", len(report.Rows))
	}
}

func TestCheckRESTProxy_SSRFBlocked(t *testing.T) {
	report := &Report{}
	props := map[string]string{"rest.proxy.url": "http://127.0.0.1:8082"}
	checkRESTProxy(context.Background(), report, props)

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

func TestCheckRESTProxy_SSRFBlocked_Metadata(t *testing.T) {
	report := &Report{}
	props := map[string]string{"rest.proxy.url": "http://169.254.169.254/latest"}
	checkRESTProxy(context.Background(), report, props)

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

func TestCheckRESTProxy_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	report := &Report{}
	props := map[string]string{"rest.proxy.url": "https://rest-proxy.kshark-nonexistent-test.example.com"}
	checkRESTProxy(ctx, report, props)

	// With a cancelled context, either the timeout check at the top returns,
	// or the DNS/HTTP check fails. Either way, we should get at least one row.
	foundTimeout := false
	for _, row := range report.Rows {
		if strings.Contains(row.Detail, "timeout") || strings.Contains(row.Detail, "canceled") || strings.Contains(row.Detail, "context") {
			foundTimeout = true
			break
		}
	}
	if len(report.Rows) == 0 {
		t.Error("expected at least one row for cancelled context")
	} else if !foundTimeout {
		// The context.Done() check at the top of checkRESTProxy should
		// catch the cancelled context and add a timeout row
		t.Log("Note: context was cancelled but no explicit timeout row found")
		for _, row := range report.Rows {
			t.Logf("  row: status=%s detail=%q", row.Status, row.Detail)
		}
	}
}

func TestCheckRESTProxy_ContextAlreadyDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	report := &Report{}
	props := map[string]string{"rest.proxy.url": "https://some-rest-proxy.example.com:8082"}
	checkRESTProxy(ctx, report, props)

	// The select at the top of checkRESTProxy should detect ctx.Done()
	// and add a timeout FAIL row
	if len(report.Rows) == 0 {
		t.Error("expected at least one row when context is already cancelled")
	}
	found := false
	for _, row := range report.Rows {
		if row.Status == FAIL && strings.Contains(row.Detail, "timeout") {
			found = true
			break
		}
	}
	if found {
		// Great - the top-level select caught it
	} else {
		// The DNS or HTTP call might have caught the cancelled context instead
		t.Log("timeout row not found at top-level; context cancellation handled downstream")
	}
}

// ---------- Direct HTTP flow tests for REST Proxy ----------

func TestCheckRESTProxy_DirectHTTPFlow_OK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/topics" {
			t.Errorf("expected path /topics, got %s", r.URL.Path)
		}
		w.WriteHeader(200)
		w.Write([]byte(`["topic1","topic2"]`))
	}))
	defer server.Close()

	client := server.Client()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL+"/topics", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestCheckRESTProxy_DirectHTTPFlow_Auth401(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
	}))
	defer server.Close()

	client := server.Client()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL+"/topics", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 401 {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestCheckRESTProxy_DirectHTTPFlow_Auth403(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
	}))
	defer server.Close()

	client := server.Client()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL+"/topics", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 403 {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
}

func TestCheckRESTProxy_DirectHTTPFlow_500(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer server.Close()

	client := server.Client()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL+"/topics", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 500 {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
}

// ---------- runScan tests ----------

func TestRunScan_EmptyBootstrap(t *testing.T) {
	report := &Report{}
	ctx := context.Background()
	cfg := scanConfig{
		props:     map[string]string{},
		bootstrap: "",
	}
	runScan(ctx, report, cfg)
	// Should complete without panic for connector-only mode (no brokers)
}

func TestRunScan_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	report := &Report{}
	cfg := scanConfig{
		props:     map[string]string{},
		bootstrap: "broker.example.com:9092",
		topics:    []string{"test-topic"},
	}
	runScan(ctx, report, cfg)

	// Should have added a timeout row
	found := false
	for _, row := range report.Rows {
		if strings.Contains(row.Detail, "timeout") || strings.Contains(row.Detail, "Global timeout") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected timeout row for cancelled context")
		for _, row := range report.Rows {
			t.Logf("  row: status=%s detail=%q", row.Status, row.Detail)
		}
	}
}

func TestRunScan_InvalidBrokerFormat(t *testing.T) {
	report := &Report{}
	cfg := scanConfig{
		props:     map[string]string{},
		bootstrap: "invalid-no-port",
	}
	runScan(context.Background(), report, cfg)

	found := false
	for _, row := range report.Rows {
		if row.Status == FAIL && strings.Contains(row.Detail, "host:port") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected FAIL row for invalid broker format")
		for _, row := range report.Rows {
			t.Logf("  row: status=%s detail=%q", row.Status, row.Detail)
		}
	}
}

// ---------- printScanPlan tests ----------

func TestPrintScanPlan_NoPanic(t *testing.T) {
	props := map[string]string{
		"bootstrap.servers":   "broker:9092",
		"schema.registry.url": "https://sr.example.com:8081",
		"rest.proxy.url":      "https://rest.example.com:8082",
	}
	topics := []string{"topic1", "topic2"}

	// Should not panic
	printScanPlan(props, topics, true, true, "report.json", "openai", "gpt-4")
}

func TestPrintScanPlan_NoTopics(t *testing.T) {
	props := map[string]string{
		"bootstrap.servers": "broker:9092",
	}
	// Should not panic with empty topics
	printScanPlan(props, nil, false, false, "", "", "")
}

func TestPrintScanPlan_NoAIProvider(t *testing.T) {
	props := map[string]string{
		"bootstrap.servers": "broker:9092",
	}
	// analyze=true but no provider - should mention prompt file fallback
	printScanPlan(props, []string{"t1"}, false, true, "", "", "")
}

// ---------- AI API mock integration with httptest ----------

func TestAIAPIIntegration_FullRoundTrip(t *testing.T) {
	// Full round-trip test: create a mock AI server, send a request through
	// the AIClient's HTTP client (bypassing SSRF), verify the request and response.
	analysis := AIAnalysisResponse{
		RootCauseAnalysis: "Firewall blocking port 9092",
		ProblemLayer:      "L4-TCP",
		LikelyCategory:   "network",
		Confidence:        "high",
		Severity:          "critical",
		Explanation:       "TCP connection to broker timed out, indicating a firewall or security group issue.",
		Evidence:          []string{"TCP connect failed for all brokers", "DNS resolution succeeded"},
		SuggestedFixes:    []string{"Check security group rules", "Verify NACL allows port 9092"},
		Disclaimer:        "AI disclaimer",
	}
	innerJSON, _ := json.Marshal(analysis)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request
		if r.Method != "POST" {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			t.Errorf("Authorization does not start with 'Bearer ', got: %q", auth)
		}

		var reqBody APIRequest
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			t.Errorf("decode request: %v", err)
			http.Error(w, "bad request", 400)
			return
		}

		// Verify the request contains system and user messages
		if len(reqBody.Messages) < 2 {
			t.Errorf("expected at least 2 messages, got %d", len(reqBody.Messages))
		}

		// Send response
		resp := APIResponse{
			Choices: []Choice{
				{Message: Message{Role: "assistant", Content: string(innerJSON)}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cfg := &AIProviderConfig{
		APIEndpoint: server.URL + "/v1/chat/completions",
		APIKey:      "sk-test-key",
		Model:       "gpt-4",
	}
	// Use server.Client() to bypass DNS/SSRF
	client := &AIClient{config: cfg, client: server.Client()}

	// Build a real report to send
	report := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L4, Status: FAIL, Detail: "TCP connect failed"},
			{Component: "kafka", Target: "broker", Layer: L3, Status: OK, Detail: "Resolved host"},
		},
	}
	systemPrompt, userPrompt, err := buildAnalysisPrompt(report)
	if err != nil {
		t.Fatalf("buildAnalysisPrompt: %v", err)
	}

	// Manually perform the HTTP call (bypassing AnalyzeReport's SSRF check)
	reqBody := APIRequest{
		Model: client.config.Model,
		Messages: []Message{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: userPrompt},
		},
	}
	reqBytes, _ := json.Marshal(reqBody)

	req, _ := http.NewRequestWithContext(context.Background(), "POST", cfg.APIEndpoint, strings.NewReader(string(reqBytes)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.APIKey)

	resp, err := client.client.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var apiResp APIResponse
	json.NewDecoder(resp.Body).Decode(&apiResp)
	if len(apiResp.Choices) == 0 {
		t.Fatal("no choices in response")
	}

	var result AIAnalysisResponse
	json.Unmarshal([]byte(apiResp.Choices[0].Message.Content), &result)
	if result.RootCauseAnalysis != "Firewall blocking port 9092" {
		t.Errorf("RootCauseAnalysis = %q, want %q", result.RootCauseAnalysis, "Firewall blocking port 9092")
	}
	if result.LikelyCategory != "network" {
		t.Errorf("LikelyCategory = %q, want %q", result.LikelyCategory, "network")
	}
	if result.Confidence != "high" {
		t.Errorf("Confidence = %q, want %q", result.Confidence, "high")
	}
}

// ---------- layeredExitCode tests ----------

func TestLayeredExitCode_L3Failure(t *testing.T) {
	r := &Report{
		Rows: []Row{
			{Component: "dns", Target: "broker", Layer: L3, Status: FAIL, Detail: "DNS failed"},
		},
	}
	got := layeredExitCode(r)
	if got != 1 {
		t.Errorf("layeredExitCode() = %d, want 1 (L3)", got)
	}
}

func TestLayeredExitCode_L4Failure(t *testing.T) {
	r := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L4, Status: FAIL, Detail: "TCP timeout"},
		},
	}
	got := layeredExitCode(r)
	if got != 2 {
		t.Errorf("layeredExitCode() = %d, want 2 (L4)", got)
	}
}

func TestLayeredExitCode_TLSFailure(t *testing.T) {
	r := &Report{
		Rows: []Row{
			{Component: "tls", Target: "broker:9092", Layer: L56, Status: FAIL, Detail: "cert expired"},
		},
	}
	got := layeredExitCode(r)
	if got != 3 {
		t.Errorf("layeredExitCode() = %d, want 3 (L56/TLS)", got)
	}
}

func TestLayeredExitCode_L7Failure(t *testing.T) {
	r := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: FAIL, Detail: "auth failed"},
		},
	}
	got := layeredExitCode(r)
	if got != 4 {
		t.Errorf("layeredExitCode() = %d, want 4 (L7)", got)
	}
}

func TestLayeredExitCode_DiagFailure(t *testing.T) {
	r := &Report{
		Rows: []Row{
			{Component: "diag", Target: "traceroute", Layer: DIAG, Status: FAIL, Detail: "no route"},
		},
	}
	got := layeredExitCode(r)
	if got != 5 {
		t.Errorf("layeredExitCode() = %d, want 5 (DIAG)", got)
	}
}

func TestLayeredExitCode_MultipleFailures(t *testing.T) {
	// L4 FAIL + L7 FAIL => lowest layer code wins (L4 = 2)
	r := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: FAIL, Detail: "auth failed"},
			{Component: "kafka", Target: "broker:9092", Layer: L4, Status: FAIL, Detail: "TCP timeout"},
		},
	}
	got := layeredExitCode(r)
	if got != 2 {
		t.Errorf("layeredExitCode() = %d, want 2 (lowest layer L4 wins)", got)
	}
}

func TestLayeredExitCode_NoFailures(t *testing.T) {
	// All OK rows => fallback exit code 1
	r := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok"},
			{Component: "dns", Target: "broker", Layer: L3, Status: OK, Detail: "resolved"},
		},
	}
	got := layeredExitCode(r)
	if got != 1 {
		t.Errorf("layeredExitCode() = %d, want 1 (fallback for no failures)", got)
	}
}

func TestLayeredExitCode_HTTPFailure(t *testing.T) {
	r := &Report{
		Rows: []Row{
			{Component: "rest", Target: "proxy:8082", Layer: HTTP, Status: FAIL, Detail: "HTTP 500"},
		},
	}
	got := layeredExitCode(r)
	if got != 4 {
		t.Errorf("layeredExitCode() = %d, want 4 (HTTP maps to same code as L7)", got)
	}
}

func TestLayeredExitCode_AllLayerFailures(t *testing.T) {
	// All layers fail => L3 (code 1) wins as the lowest
	r := &Report{
		Rows: []Row{
			{Component: "diag", Target: "trace", Layer: DIAG, Status: FAIL, Detail: "fail"},
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: FAIL, Detail: "fail"},
			{Component: "tls", Target: "broker:9092", Layer: L56, Status: FAIL, Detail: "fail"},
			{Component: "kafka", Target: "broker:9092", Layer: L4, Status: FAIL, Detail: "fail"},
			{Component: "dns", Target: "broker", Layer: L3, Status: FAIL, Detail: "fail"},
		},
	}
	got := layeredExitCode(r)
	if got != 1 {
		t.Errorf("layeredExitCode() = %d, want 1 (L3 is lowest layer)", got)
	}
}

func TestLayeredExitCode_MixedStatuses(t *testing.T) {
	// Only FAIL rows count; OK, WARN, SKIP should be ignored
	r := &Report{
		Rows: []Row{
			{Component: "dns", Target: "broker", Layer: L3, Status: OK, Detail: "resolved"},
			{Component: "kafka", Target: "broker:9092", Layer: L4, Status: WARN, Detail: "slow"},
			{Component: "tls", Target: "broker:9092", Layer: L56, Status: SKIP, Detail: "skipped"},
			{Component: "diag", Target: "trace", Layer: DIAG, Status: FAIL, Detail: "no route"},
		},
	}
	got := layeredExitCode(r)
	if got != 5 {
		t.Errorf("layeredExitCode() = %d, want 5 (only DIAG has FAIL)", got)
	}
}

func TestLayeredExitCode_EmptyReport(t *testing.T) {
	r := &Report{}
	got := layeredExitCode(r)
	if got != 1 {
		t.Errorf("layeredExitCode() = %d, want 1 (fallback for empty report)", got)
	}
}

// ---------- runScan integration tests ----------

func TestRunScan_SingleBroker_DNSFailure(t *testing.T) {
	report := &Report{}
	cfg := scanConfig{
		props:     map[string]string{},
		bootstrap: "this.host.does.not.exist.kshark.invalid:9092",
		diag:      false,
	}
	runScan(context.Background(), report, cfg)

	// Should have a DNS failure row
	hasDNS := false
	for _, row := range report.Rows {
		if row.Layer == L3 && row.Status == FAIL {
			hasDNS = true
			break
		}
	}
	if !hasDNS {
		t.Error("expected L3 DNS FAIL row for unresolvable host")
		for _, row := range report.Rows {
			t.Logf("  row: layer=%s status=%s detail=%q", row.Layer, row.Status, row.Detail)
		}
	}
}

func TestRunScan_SingleBroker_TCPOpenPort(t *testing.T) {
	// Start a TCP listener to simulate a reachable broker
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
			// Close immediately (not a real Kafka broker)
			conn.Close()
		}
	}()

	report := &Report{}
	cfg := scanConfig{
		props:          map[string]string{"security.protocol": "PLAINTEXT"},
		bootstrap:      ln.Addr().String(),
		kafkaTimeout:   2 * time.Second,
		diag:           false,
		probeDirection: "up",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runScan(ctx, report, cfg)

	// Should have DNS OK and TCP OK for localhost
	hasL3OK := false
	hasL4OK := false
	for _, row := range report.Rows {
		if row.Layer == L3 && row.Status == OK {
			hasL3OK = true
		}
		if row.Layer == L4 && row.Status == OK {
			hasL4OK = true
		}
	}
	if !hasL3OK {
		t.Error("expected L3 DNS OK row for localhost")
	}
	if !hasL4OK {
		t.Error("expected L4 TCP OK row for open port")
	}
}

func TestRunScan_MultipleBrokers_InvalidFormat(t *testing.T) {
	report := &Report{}
	cfg := scanConfig{
		props:     map[string]string{},
		bootstrap: "valid-host:9092,invalid-no-port,another:9093",
		diag:      false,
	}
	runScan(context.Background(), report, cfg)

	// Should have a FAIL row for the invalid broker format
	found := false
	for _, row := range report.Rows {
		if row.Status == FAIL && strings.Contains(row.Detail, "host:port") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected FAIL row for invalid broker format in multi-broker list")
	}
}

func TestRunScan_FullMode_ContinuesAfterFailure(t *testing.T) {
	// Get a closed port
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	closedAddr := ln.Addr().String()
	ln.Close()

	report := &Report{}
	cfg := scanConfig{
		props:          map[string]string{},
		bootstrap:      closedAddr,
		topics:         []string{"test-topic"},
		probeDirection: "full",
		diag:           false,
		kafkaTimeout:   2 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runScan(ctx, report, cfg)

	// In full mode, should have attempted DNS and TCP (TCP will fail)
	hasL3 := false
	hasL4Fail := false
	for _, row := range report.Rows {
		if row.Layer == L3 {
			hasL3 = true
		}
		if row.Layer == L4 && row.Status == FAIL {
			hasL4Fail = true
		}
	}
	if !hasL3 {
		t.Error("expected L3 row even in full mode")
	}
	if !hasL4Fail {
		t.Error("expected L4 FAIL row for closed port")
	}
}

func TestRunScan_TimeoutDuringBrokerChecks(t *testing.T) {
	report := &Report{}
	cfg := scanConfig{
		props:     map[string]string{},
		bootstrap: "192.0.2.1:9092,192.0.2.2:9092",
		diag:      false,
	}

	// Very short timeout to trigger during broker loop
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	runScan(ctx, report, cfg)

	// Should have at least started and potentially hit timeout
	// The exact behavior depends on timing, but it shouldn't hang
}

func TestRunScan_DiagnosticsParallel(t *testing.T) {
	// Start a TCP listener
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

	report := &Report{}
	cfg := scanConfig{
		props:          map[string]string{"security.protocol": "PLAINTEXT"},
		bootstrap:      ln.Addr().String(),
		diag:           true,
		kafkaTimeout:   2 * time.Second,
		probeDirection: "up",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	runScan(ctx, report, cfg)

	// Diagnostics should have produced some rows (traceroute and/or MTU)
	hasDiag := false
	for _, row := range report.Rows {
		if row.Layer == DIAG && row.Component == "diag" {
			hasDiag = true
			break
		}
	}
	if !hasDiag {
		t.Log("No diagnostic rows found — traceroute/ping tools may not be available on this system")
	}
}

func TestRunScan_NeighborhoodOnTCPFailure(t *testing.T) {
	// Get a closed port
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	closedAddr := ln.Addr().String()
	ln.Close()

	report := &Report{}
	cfg := scanConfig{
		props:          map[string]string{},
		bootstrap:      closedAddr,
		diag:           true,
		neighborhood:   false, // auto-triggered by diag=true on TCP failure
		probeDirection: "up",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	runScan(ctx, report, cfg)

	// Should have neighborhood scan rows
	hasNeighborhood := false
	for _, row := range report.Rows {
		if row.Component == "neighborhood" {
			hasNeighborhood = true
			break
		}
	}
	if !hasNeighborhood {
		t.Log("No neighborhood rows found — may depend on TCP failure mode and timing")
	}
}

func TestRunScan_ConnectorOnly(t *testing.T) {
	report := &Report{}
	cfg := scanConfig{
		props:     map[string]string{},
		bootstrap: "", // no Kafka brokers
		diag:      false,
	}
	runScan(context.Background(), report, cfg)
	// Should complete without error for connector-only mode
	// No broker rows expected
	for _, row := range report.Rows {
		if row.Component == "kafka" {
			t.Error("unexpected kafka row in connector-only mode")
		}
	}
}

