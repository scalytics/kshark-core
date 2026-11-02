package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newStatusServer(t *testing.T, routes map[string]int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if code, ok := routes[r.URL.Path]; ok {
			w.WriteHeader(code)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestCheckSchemaRegistry_Statuses(t *testing.T) {
	allowLoopback(t)
	tests := []struct {
		name   string
		code   int
		status CheckStatus
		detail string
	}{
		{"ok", http.StatusOK, OK, "GET /subjects OK"},
		{"unauthorized", http.StatusUnauthorized, FAIL, "Auth 401"},
		{"forbidden", http.StatusForbidden, FAIL, "Auth 403"},
		{"server error", http.StatusInternalServerError, WARN, "HTTP 500"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotUser, gotPass string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotUser, gotPass, _ = r.BasicAuth()
				if r.URL.Path != "/subjects" {
					t.Errorf("path = %q, want /subjects", r.URL.Path)
				}
				w.WriteHeader(tt.code)
			}))
			defer srv.Close()
			r := &Report{}
			checkSchemaRegistry(context.Background(), r, map[string]string{
				"schema.registry.url":  srv.URL + "/",
				"basic.auth.user.info": "sr-user:sr-pass",
			})
			requireRow(t, r, "127.0.0.1", L3, OK, "Resolved host")
			requireRow(t, r, srv.URL+"/", HTTP, tt.status, tt.detail)
			if gotUser != "sr-user" || gotPass != "sr-pass" {
				t.Errorf("basic auth = %q/%q", gotUser, gotPass)
			}
		})
	}
}

func TestCheckSchemaRegistry_Errors(t *testing.T) {
	t.Run("no url is a no-op", func(t *testing.T) {
		r := &Report{}
		checkSchemaRegistry(context.Background(), r, map[string]string{})
		if len(rowsOf(r)) != 0 {
			t.Errorf("rows = %+v", rowsOf(r))
		}
	})
	t.Run("loopback blocked by default", func(t *testing.T) {
		r := &Report{}
		checkSchemaRegistry(context.Background(), r, map[string]string{"schema.registry.url": "http://127.0.0.1:1"})
		requireRow(t, r, "http://127.0.0.1:1", HTTP, FAIL, "URL blocked (SSRF protection)")
	})
	t.Run("unresolvable host and refused connection", func(t *testing.T) {
		allowLoopback(t)
		r := &Report{}
		checkSchemaRegistry(context.Background(), r, map[string]string{"schema.registry.url": "http://sr.invalid:8081"})
		requireRow(t, r, "sr.invalid", L3, FAIL, "DNS failed")
		requireRow(t, r, "http://sr.invalid:8081", HTTP, FAIL, "GET /subjects failed")
	})
	t.Run("bad tls material", func(t *testing.T) {
		allowLoopback(t)
		r := &Report{}
		checkSchemaRegistry(context.Background(), r, map[string]string{
			"schema.registry.url":     "https://127.0.0.1:1",
			"security.protocol":       "SSL",
			"ssl.ca.location":         "/nonexistent/ca.pem",
			"ssl.truststore.location": "/nonexistent/ca.pem",
		})
		requireRow(t, r, "https://127.0.0.1:1", HTTP, FAIL, "TLS config err")
	})
}

func TestProbeSubject_Statuses(t *testing.T) {
	allowLoopback(t)
	srv := newStatusServer(t, map[string]int{
		"/subjects/present-value/versions": http.StatusOK,
		"/subjects/locked-value/versions":  http.StatusForbidden,
		"/subjects/flaky-value/versions":   http.StatusBadGateway,
	})
	tests := []struct {
		subject string
		status  CheckStatus
		detail  string
	}{
		{"present-value", OK, "subject registered"},
		{"absent-value", FAIL, "subject not found (404)"},
		{"locked-value", FAIL, "Auth 403"},
		{"flaky-value", WARN, "HTTP 502"},
	}
	for _, tt := range tests {
		t.Run(tt.subject, func(t *testing.T) {
			r := &Report{}
			probeSubject(context.Background(), r, map[string]string{"basic.auth.user.info": "u:p"}, srv.URL, tt.subject)
			requireRow(t, r, tt.subject, HTTP, tt.status, tt.detail)
		})
	}
}

func TestProbeSubject_Errors(t *testing.T) {
	r := &Report{}
	probeSubject(context.Background(), r, map[string]string{}, "http://127.0.0.1:1", "s")
	requireRow(t, r, "s", HTTP, FAIL, "URL blocked (SSRF protection)")

	allowLoopback(t)
	r = &Report{}
	probeSubject(context.Background(), r, map[string]string{}, "http://127.0.0.1:1", "s")
	requireRow(t, r, "s", HTTP, FAIL, "GET subject failed")

	r = &Report{}
	probeSubject(context.Background(), r, map[string]string{"security.protocol": "SSL", "ssl.ca.location": "/nonexistent/ca.pem"}, "https://127.0.0.1:1", "s")
	requireRow(t, r, "s", HTTP, FAIL, "TLS config err")
}

func TestRunSchemaPresenceCheck(t *testing.T) {
	allowLoopback(t)
	srv := newStatusServer(t, map[string]int{
		"/subjects":                        http.StatusOK,
		"/subjects/orders-value/versions":  http.StatusOK,
		"/subjects/missing-value/versions": http.StatusNotFound,
	})
	r := &Report{}
	rc := runnerConfig{props: map[string]string{"schema.registry.url": srv.URL}}
	res := runRoutineCheck(context.Background(), r, rc, RoutineCheck{
		ID: "sp", Kind: KindSchemaPresence, Tier: TierCore,
		Params: map[string]any{"subjects": []any{"orders-value", "missing-value"}},
	})
	if res.Status != CheckFAIL {
		t.Errorf("status = %s, want FAIL", res.Status)
	}
	requireRow(t, r, srv.URL, HTTP, OK, "GET /subjects OK")
	requireRow(t, r, "orders-value", HTTP, OK, "subject registered")
	requireRow(t, r, "missing-value", HTTP, FAIL, "subject not found (404)")

	r = &Report{}
	runSchemaPresenceCheck(context.Background(), r, runnerConfig{props: map[string]string{}}, []string{"x"})
	requireRow(t, r, "(no schema.registry.url)", HTTP, FAIL, "schema-presence check needs schema.registry.url")
}

func TestRunComponentHealthCheck(t *testing.T) {
	allowLoopback(t)
	srv := newStatusServer(t, map[string]int{"/healthz": http.StatusOK, "/ready": http.StatusServiceUnavailable})
	t.Setenv("KSHARK_TEST_COMPONENT_URL", srv.URL)
	check := RoutineCheck{ID: "ch", Kind: KindComponentHealth, Params: map[string]any{
		"components": []any{"api", "worker", "ui", "orphan"},
		"probes": map[string]any{
			"api":    map[string]any{"url": srv.URL},
			"worker": map[string]any{"url_env": "KSHARK_TEST_COMPONENT_URL", "path": "ready"},
			"ui":     map[string]any{"url_env": "KSHARK_TEST_UNSET_URL"},
		},
	}}
	r := &Report{}
	runComponentHealthCheck(context.Background(), r, check)
	requireRow(t, r, srv.URL+"/healthz", HTTP, OK, "GET /healthz -> 200")
	requireRow(t, r, srv.URL+"/ready", HTTP, FAIL, "GET /ready -> 503 (want 200)")
	rows := rowsOf(r)
	warns := 0
	for _, row := range rows {
		if row.Status == WARN && row.Target == "(no probe target)" {
			warns++
		}
	}
	if warns != 2 {
		t.Errorf("unconfigured components = %d, want 2 in %+v", warns, rows)
	}
}

func TestRunHTTPCheck(t *testing.T) {
	srv := newStatusServer(t, map[string]int{"/api": http.StatusOK})
	t.Setenv("KSHARK_TEST_HTTP_URL", srv.URL)
	tests := []struct {
		name   string
		params map[string]any
		target string
		status CheckStatus
		detail string
	}{
		{"url and path", map[string]any{"url": srv.URL, "path": "/api"}, srv.URL + "/api", OK, "GET /api -> 200"},
		{"url_env", map[string]any{"url_env": "KSHARK_TEST_HTTP_URL", "path": "api"}, srv.URL + "/api", OK, "GET /api -> 200"},
		{"nested http block", map[string]any{"http": map[string]any{"url_env": "KSHARK_TEST_HTTP_URL", "path": "/missing"}}, srv.URL + "/missing", FAIL, "GET /missing -> 404 (want 200)"},
		{"no url", map[string]any{"url_env": "KSHARK_TEST_UNSET_URL"}, "(no url)", FAIL, "http-check has no resolvable url"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowLoopback(t)
			r := &Report{}
			runHTTPCheck(context.Background(), r, RoutineCheck{ID: "hc", Kind: KindHTTPCheck, Params: tt.params})
			requireRow(t, r, tt.target, HTTP, tt.status, tt.detail)
		})
	}
	t.Run("refused connection", func(t *testing.T) {
		allowLoopback(t)
		r := &Report{}
		httpGet(context.Background(), r, "c", "http://127.0.0.1:1", "", 200)
		requireRow(t, r, "http://127.0.0.1:1", HTTP, FAIL, "GET failed")
	})
	t.Run("blocked by default", func(t *testing.T) {
		r := &Report{}
		httpGet(context.Background(), r, "c", srv.URL, "/api", 200)
		requireRow(t, r, srv.URL, HTTP, FAIL, "URL blocked (SSRF protection)")
	})
}

func TestCheckRESTProxy(t *testing.T) {
	tests := []struct {
		name   string
		code   int
		status CheckStatus
		detail string
	}{
		{"ok", http.StatusOK, OK, "GET /topics OK"},
		{"auth", http.StatusUnauthorized, FAIL, "Auth 401"},
		{"other", http.StatusTeapot, WARN, "HTTP 418"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowLoopback(t)
			srv := newStatusServer(t, map[string]int{"/topics": tt.code})
			r := &Report{}
			checkRESTProxy(context.Background(), r, map[string]string{"rest.proxy.url": srv.URL})
			requireRow(t, r, srv.URL, HTTP, tt.status, tt.detail)
		})
	}
	t.Run("not configured", func(t *testing.T) {
		r := &Report{}
		checkRESTProxy(context.Background(), r, map[string]string{})
		if len(rowsOf(r)) != 0 {
			t.Errorf("rows = %+v", rowsOf(r))
		}
	})
	t.Run("cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r := &Report{}
		checkRESTProxy(ctx, r, map[string]string{"rest.proxy.url": "http://127.0.0.1:1"})
		requireRow(t, r, "timeout", DIAG, FAIL, "Global timeout reached before REST proxy check")
	})
	t.Run("loopback blocked", func(t *testing.T) {
		r := &Report{}
		checkRESTProxy(context.Background(), r, map[string]string{"rest.proxy.url": "http://127.0.0.1:1"})
		requireRow(t, r, "http://127.0.0.1:1", DIAG, FAIL, "SSRF check")
	})
	t.Run("refused", func(t *testing.T) {
		allowLoopback(t)
		r := &Report{}
		checkRESTProxy(context.Background(), r, map[string]string{"rest.proxy.url": "http://127.0.0.1:1"})
		requireRow(t, r, "http://127.0.0.1:1", HTTP, FAIL, "GET /topics failed")
	})
}

func aiServer(t *testing.T, handler func(w http.ResponseWriter, req APIRequest)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("Authorization = %q", got)
		}
		var req APIRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		handler(w, req)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func aiChoice(content string) string {
	b, _ := json.Marshal(APIResponse{Choices: []Choice{{Message: Message{Role: "assistant", Content: content}}}})
	return string(b)
}

func TestAnalyzeReport_HTTP(t *testing.T) {
	good := `{"rootCauseAnalysis":"listener mismatch","problemLayer":"L7","confidence":"high","evidence":["e1"],"suggestedFixes":["f1"]}`
	tests := []struct {
		name    string
		code    int
		body    string
		wantErr string
	}{
		{"success", http.StatusOK, aiChoice(good), ""},
		{"http error", http.StatusInternalServerError, "secret upstream detail", "AI API returned HTTP 500"},
		{"bad envelope", http.StatusOK, "not json", "could not decode AI API response"},
		{"no choices", http.StatusOK, `{"choices":[]}`, "AI API returned no choices"},
		{"bad content", http.StatusOK, aiChoice("plain text"), "could not unmarshal AI JSON response"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowLoopback(t)
			srv := aiServer(t, func(w http.ResponseWriter, req APIRequest) {
				if req.Model != "test-model" || len(req.Messages) != 2 || req.Messages[0].Content != "sys" || req.Messages[1].Content != "usr" {
					t.Errorf("request = %+v", req)
				}
				w.WriteHeader(tt.code)
				_, _ = w.Write([]byte(tt.body))
			})
			c := NewAIClient(&AIProviderConfig{APIEndpoint: srv.URL, APIKey: "test-key", Model: "test-model"})
			got, err := c.AnalyzeReport(context.Background(), "sys", "usr")
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				if strings.Contains(err.Error(), "secret upstream detail") {
					t.Errorf("error leaks response body: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("AnalyzeReport: %v", err)
			}
			if got.RootCauseAnalysis != "listener mismatch" || got.ProblemLayer != "L7" || len(got.SuggestedFixes) != 1 {
				t.Errorf("analysis = %+v", got)
			}
		})
	}
}

func TestAnalyzeReport_TransportErrors(t *testing.T) {
	c := NewAIClient(&AIProviderConfig{APIEndpoint: "http://127.0.0.1:1/v1", APIKey: "test-key"})
	if _, err := c.AnalyzeReport(context.Background(), "s", "u"); err == nil || !strings.Contains(err.Error(), "AI endpoint blocked (SSRF protection)") {
		t.Errorf("err = %v, want SSRF block", err)
	}
	allowLoopback(t)
	if _, err := c.AnalyzeReport(context.Background(), "s", "u"); err == nil || !strings.Contains(err.Error(), "failed to send request to AI API") {
		t.Errorf("err = %v, want send failure", err)
	}
	bad := NewAIClient(&AIProviderConfig{APIEndpoint: "http://bad host/", APIKey: "k"})
	if _, err := bad.AnalyzeReport(context.Background(), "s", "u"); err == nil || !strings.Contains(err.Error(), "could not create API request") {
		t.Errorf("err = %v, want request build failure", err)
	}
}
