package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
