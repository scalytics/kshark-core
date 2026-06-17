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
	"path/filepath"
	"strings"
	"testing"
)

func TestParseRoutine_Valid(t *testing.T) {
	yaml := `
routine_id: bp-006-citizen360
routine_version: 0.1.0
blueprint: bp-006-citizen360
backend:
  family: kafscale
checks:
  - id: SCEN-bp006-01
    kind: topic-presence
    claim: "the five source simulators produce to the five citizen.* topics"
    topics: [citizen.persons, citizen.addresses]
  - id: SCEN-bp006-04
    kind: http-check
    tier: needs-headroom
    claim: "a person-scoped query returns cross-source results"
    http: { url_env: SMOKE_TERMINAL_URL, path: "/api/person?id=P0001" }
`
	r, err := parseRoutine([]byte(yaml))
	if err != nil {
		t.Fatalf("parseRoutine valid: unexpected error: %v", err)
	}
	if r.RoutineID != "bp-006-citizen360" {
		t.Errorf("RoutineID = %q, want bp-006-citizen360", r.RoutineID)
	}
	if r.RoutineVersion != "0.1.0" {
		t.Errorf("RoutineVersion = %q, want 0.1.0", r.RoutineVersion)
	}
	if r.Backend.Family != "kafscale" {
		t.Errorf("Backend.Family = %q, want kafscale", r.Backend.Family)
	}
	if len(r.Checks) != 2 {
		t.Fatalf("len(Checks) = %d, want 2", len(r.Checks))
	}

	c0 := r.Checks[0]
	if c0.Kind != KindTopicPresence {
		t.Errorf("check[0].Kind = %q, want topic-presence", c0.Kind)
	}
	if c0.Tier != TierCore {
		t.Errorf("check[0].Tier = %q, want defaulted to core", c0.Tier)
	}
	topics := stringSliceParam(c0.Params, "topics")
	if len(topics) != 2 || topics[0] != "citizen.persons" {
		t.Errorf("check[0] topics = %v, want [citizen.persons citizen.addresses]", topics)
	}

	c1 := r.Checks[1]
	if c1.Tier != TierNeedsHeadroom {
		t.Errorf("check[1].Tier = %q, want needs-headroom", c1.Tier)
	}
	http, ok := c1.Params["http"].(map[string]any)
	if !ok {
		t.Fatalf("check[1] http param missing or wrong type: %T", c1.Params["http"])
	}
	if mapStringParam(http, "url_env") != "SMOKE_TERMINAL_URL" {
		t.Errorf("check[1] http.url_env = %q, want SMOKE_TERMINAL_URL", mapStringParam(http, "url_env"))
	}
}

func TestParseRoutine_UnknownKind(t *testing.T) {
	yaml := `
routine_id: bp-x
routine_version: 0.1.0
blueprint: bp-x
checks:
  - id: SCEN-x-01
    kind: teleport
    claim: "should never parse"
`
	_, err := parseRoutine([]byte(yaml))
	if err == nil {
		t.Fatal("parseRoutine: expected error for unknown kind, got nil")
	}
	if !strings.Contains(err.Error(), "unknown kind") {
		t.Errorf("error = %q, want it to mention 'unknown kind'", err.Error())
	}
}

func TestParseRoutine_RejectsBadInput(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "missing routine_id",
			yaml: "routine_version: 0.1.0\nblueprint: bp-x\nchecks:\n  - {id: a, kind: connectivity}\n",
			want: "routine_id is required",
		},
		{
			name: "missing routine_version",
			yaml: "routine_id: bp-x\nblueprint: bp-x\nchecks:\n  - {id: a, kind: connectivity}\n",
			want: "routine_version is required",
		},
		{
			name: "missing blueprint",
			yaml: "routine_id: bp-x\nroutine_version: 0.1.0\nchecks:\n  - {id: a, kind: connectivity}\n",
			want: "blueprint is required",
		},
		{
			name: "no checks",
			yaml: "routine_id: bp-x\nroutine_version: 0.1.0\nblueprint: bp-x\nchecks: []\n",
			want: "at least one check",
		},
		{
			name: "duplicate check id",
			yaml: "routine_id: bp-x\nroutine_version: 0.1.0\nblueprint: bp-x\nchecks:\n  - {id: a, kind: connectivity}\n  - {id: a, kind: connectivity}\n",
			want: "duplicate check id",
		},
		{
			name: "invalid tier",
			yaml: "routine_id: bp-x\nroutine_version: 0.1.0\nblueprint: bp-x\nchecks:\n  - {id: a, kind: connectivity, tier: gold}\n",
			want: "invalid tier",
		},
		{
			name: "topic-presence without topics",
			yaml: "routine_id: bp-x\nroutine_version: 0.1.0\nblueprint: bp-x\nchecks:\n  - {id: a, kind: topic-presence}\n",
			want: "needs a non-empty topics list",
		},
		{
			name: "schema-presence without subjects",
			yaml: "routine_id: bp-x\nroutine_version: 0.1.0\nblueprint: bp-x\nchecks:\n  - {id: a, kind: schema-presence}\n",
			want: "needs a non-empty subjects list",
		},
		{
			name: "http-check without url",
			yaml: "routine_id: bp-x\nroutine_version: 0.1.0\nblueprint: bp-x\nchecks:\n  - {id: a, kind: http-check}\n",
			want: "needs a url or url_env",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseRoutine([]byte(tc.yaml))
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), tc.want)
			}
		})
	}
}

// TestLoadRoutine_SampleBP006 loads the shipped sample routine and asserts it
// parses, defaults tiers, and binds claim ids on every check.
func TestLoadRoutine_SampleBP006(t *testing.T) {
	path := filepath.Join("..", "..", "examples", "validation-routine.bp-006-citizen360.yaml")
	r, err := loadRoutine(path)
	if err != nil {
		t.Fatalf("loadRoutine(%s): %v", path, err)
	}
	if r.RoutineID != "bp-006-citizen360" {
		t.Errorf("RoutineID = %q, want bp-006-citizen360", r.RoutineID)
	}
	if len(r.Checks) != 5 {
		t.Fatalf("len(Checks) = %d, want 5 (SCEN-bp006-01..05)", len(r.Checks))
	}
	wantIDs := map[string]bool{
		"SCEN-bp006-01": false, "SCEN-bp006-02": false, "SCEN-bp006-03": false,
		"SCEN-bp006-04": false, "SCEN-bp006-05": false,
	}
	for _, c := range r.Checks {
		if _, ok := wantIDs[c.ID]; !ok {
			t.Errorf("unexpected check id %q", c.ID)
		}
		wantIDs[c.ID] = true
		if c.Claim == "" {
			t.Errorf("check %q has no claim binding", c.ID)
		}
		if c.Tier != TierCore && c.Tier != TierNeedsHeadroom {
			t.Errorf("check %q tier %q not normalized", c.ID, c.Tier)
		}
	}
	for id, seen := range wantIDs {
		if !seen {
			t.Errorf("sample routine missing check %q", id)
		}
	}
	// The person-scope check must be needs-headroom (SPEC-14 section 9).
	for _, c := range r.Checks {
		if c.ID == "SCEN-bp006-04" && c.Tier != TierNeedsHeadroom {
			t.Errorf("SCEN-bp006-04 tier = %q, want needs-headroom", c.Tier)
		}
	}
}
