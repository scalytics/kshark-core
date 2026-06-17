// Copyright 2024-2026 Scalytics GmbH and kshark Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// allowLoopback points the SSRF gate at a no-op so loopback httptest servers
// are reachable in tests. It restores the production gate on cleanup.
func allowLoopback(t *testing.T) {
	t.Helper()
	prev := ssrfCheck
	ssrfCheck = func(string) (string, error) { return "", nil }
	t.Cleanup(func() { ssrfCheck = prev })
}

func TestComputeVerdict(t *testing.T) {
	mk := func(status BVBCheckStatus, skipReason string) BVBCheck {
		return BVBCheck{ID: "c", Status: status, SkipReason: skipReason}
	}

	tests := []struct {
		name   string
		checks []BVBCheck
		want   VerdictStatus
		pass   int
		fail   int
		skip   int
		texcl  int
	}{
		{
			name:   "all pass is green",
			checks: []BVBCheck{mk(CheckPASS, ""), mk(CheckPASS, "")},
			want:   VerdictGreen, pass: 2,
		},
		{
			name:   "any fail is red",
			checks: []BVBCheck{mk(CheckPASS, ""), mk(CheckFAIL, "")},
			want:   VerdictRed, pass: 1, fail: 1,
		},
		{
			name:   "tier-excluded skip does not drag verdict",
			checks: []BVBCheck{mk(CheckPASS, ""), mk(CheckSKIP, "TIER-EXCLUDED: @needs-headroom")},
			want:   VerdictGreen, pass: 1, skip: 1, texcl: 1,
		},
		{
			name:   "plain skip is red (not tier-excluded)",
			checks: []BVBCheck{mk(CheckPASS, ""), mk(CheckSKIP, "no observable signal produced")},
			want:   VerdictRed, pass: 1, skip: 1,
		},
		{
			name:   "pass plus tier-excluded equals total is green",
			checks: []BVBCheck{mk(CheckSKIP, "TIER-EXCLUDED: @needs-headroom")},
			want:   VerdictGreen, skip: 1, texcl: 1,
		},
		{
			name:   "fail beats tier-excluded",
			checks: []BVBCheck{mk(CheckFAIL, ""), mk(CheckSKIP, "TIER-EXCLUDED: @needs-headroom")},
			want:   VerdictRed, fail: 1, skip: 1, texcl: 1,
		},
		{
			name:   "empty is green (vacuously)",
			checks: nil,
			want:   VerdictGreen,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := computeVerdict(tt.checks)
			if v.Status != tt.want {
				t.Errorf("Status = %q, want %q", v.Status, tt.want)
			}
			if v.Pass != tt.pass {
				t.Errorf("Pass = %d, want %d", v.Pass, tt.pass)
			}
			if v.Fail != tt.fail {
				t.Errorf("Fail = %d, want %d", v.Fail, tt.fail)
			}
			if v.Skip != tt.skip {
				t.Errorf("Skip = %d, want %d", v.Skip, tt.skip)
			}
			if v.TierExcluded != tt.texcl {
				t.Errorf("TierExcluded = %d, want %d", v.TierExcluded, tt.texcl)
			}
			if v.Total != len(tt.checks) {
				t.Errorf("Total = %d, want %d", v.Total, len(tt.checks))
			}
		})
	}
}

func TestFoldRows(t *testing.T) {
	tests := []struct {
		name string
		rows []Row
		want BVBCheckStatus
	}{
		{"no rows folds to skip", nil, CheckSKIP},
		{"ok folds to pass", []Row{{Status: OK, Component: "c", Target: "t", Detail: "d", Layer: HTTP}}, CheckPASS},
		{"warn folds to pass", []Row{{Status: WARN, Component: "c", Target: "t", Detail: "d", Layer: HTTP}}, CheckPASS},
		{"any fail folds to fail", []Row{
			{Status: OK, Component: "c", Target: "t", Detail: "ok", Layer: HTTP},
			{Status: FAIL, Component: "c", Target: "t2", Detail: "bad", Layer: HTTP},
		}, CheckFAIL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, evidence, _ := foldRows(tt.rows)
			if got != tt.want {
				t.Errorf("foldRows status = %q, want %q", got, tt.want)
			}
			if len(tt.rows) > 0 && len(evidence) == 0 {
				t.Errorf("foldRows produced no evidence for %d rows", len(tt.rows))
			}
		})
	}
}

// TestRunRoutineCheck_TierExcluded asserts a needs-headroom check is skipped
// with the TIER-EXCLUDED contract when the active tier is core, and that it
// runs no probe (zero rows added).
func TestRunRoutineCheck_TierExcluded(t *testing.T) {
	report := &Report{}
	rc := runnerConfig{activeTier: TierCore}
	check := RoutineCheck{ID: "SCEN-x-04", Kind: KindHTTPCheck, Tier: TierNeedsHeadroom,
		Params: map[string]any{"url": "http://example.invalid"}}

	res := runRoutineCheck(context.Background(), report, rc, check)
	if res.Status != CheckSKIP {
		t.Errorf("Status = %q, want SKIP", res.Status)
	}
	if !res.isTierExcluded() {
		t.Errorf("SkipReason = %q, want TIER-EXCLUDED prefix", res.SkipReason)
	}
	if len(report.Rows) != 0 {
		t.Errorf("expected no rows for a tier-excluded check, got %d", len(report.Rows))
	}
}

// TestRunRoutineCheck_HTTPCheck_OK wires an http-check against a live httptest
// server through runRoutineCheck and asserts PASS with row back-references.
func TestRunRoutineCheck_HTTPCheck_OK(t *testing.T) {
	allowLoopback(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(200)
			return
		}
		w.WriteHeader(404)
	}))
	defer server.Close()

	report := &Report{}
	rc := runnerConfig{activeTier: TierCore}
	check := RoutineCheck{
		ID:    "SCEN-x-03",
		Kind:  KindHTTPCheck,
		Tier:  TierCore,
		Claim: "the terminal is reachable",
		Params: map[string]any{
			"url":  server.URL,
			"path": "/healthz",
		},
	}

	res := runRoutineCheck(context.Background(), report, rc, check)
	if res.Status != CheckPASS {
		t.Fatalf("Status = %q, want PASS; evidence=%v", res.Status, res.Evidence)
	}
	if res.Claim != "the terminal is reachable" {
		t.Errorf("Claim = %q, not carried through", res.Claim)
	}
	if len(res.Rows) == 0 {
		t.Error("expected row back-references on the check")
	}
	if len(res.Evidence) == 0 {
		t.Error("expected evidence on a passing http-check")
	}
}

// TestRunRoutineCheck_HTTPCheck_Fail asserts a non-200 status folds to FAIL.
func TestRunRoutineCheck_HTTPCheck_Fail(t *testing.T) {
	allowLoopback(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
	}))
	defer server.Close()

	report := &Report{}
	rc := runnerConfig{activeTier: TierCore}
	check := RoutineCheck{
		ID:     "SCEN-x-03",
		Kind:   KindHTTPCheck,
		Tier:   TierCore,
		Params: map[string]any{"url": server.URL, "path": "/healthz"},
	}

	res := runRoutineCheck(context.Background(), report, rc, check)
	if res.Status != CheckFAIL {
		t.Errorf("Status = %q, want FAIL for HTTP 503", res.Status)
	}
}

// TestRunRoutineCheck_ComponentHealth wires component-health with per-component
// probe URLs against a live server through the runner.
func TestRunRoutineCheck_ComponentHealth(t *testing.T) {
	allowLoopback(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer server.Close()

	report := &Report{}
	rc := runnerConfig{activeTier: TierCore}
	check := RoutineCheck{
		ID:    "SCEN-x-03",
		Kind:  KindComponentHealth,
		Tier:  TierCore,
		Claim: "the application plane is reachable",
		Params: map[string]any{
			"components": []any{"kafgraph"},
			"probes": map[string]any{
				"kafgraph": map[string]any{"url": server.URL, "path": "/healthz"},
			},
		},
	}

	res := runRoutineCheck(context.Background(), report, rc, check)
	if res.Status != CheckPASS {
		t.Errorf("Status = %q, want PASS; evidence=%v", res.Status, res.Evidence)
	}
}

// TestRunRoutineCheck_HTTPCheck_UrlEnv asserts url_env resolution at run time.
func TestRunRoutineCheck_HTTPCheck_UrlEnv(t *testing.T) {
	allowLoopback(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer server.Close()
	t.Setenv("KSHARK_TEST_TERMINAL_URL", server.URL)

	report := &Report{}
	rc := runnerConfig{activeTier: TierCore}
	check := RoutineCheck{
		ID:   "SCEN-x-04",
		Kind: KindHTTPCheck,
		Tier: TierCore,
		Params: map[string]any{
			"http": map[string]any{"url_env": "KSHARK_TEST_TERMINAL_URL", "path": "/healthz"},
		},
	}

	res := runRoutineCheck(context.Background(), report, rc, check)
	if res.Status != CheckPASS {
		t.Errorf("Status = %q, want PASS via url_env; evidence=%v", res.Status, res.Evidence)
	}
}

func TestLoadImageDigests(t *testing.T) {
	// Empty path yields empty (not nil) list.
	d, err := loadImageDigests("")
	if err != nil {
		t.Fatalf("loadImageDigests(\"\"): %v", err)
	}
	if d == nil || len(d) != 0 {
		t.Errorf("expected empty digest list, got %v", d)
	}
}
