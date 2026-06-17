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
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// ---------- Blueprint Validation Routine (SPEC-14 section 3) ----------
//
// A validation routine is a declarative, versioned, per-blueprint description
// of the checks a Blueprint-Validation-Bundle (BVB) runs. This file defines
// the on-disk format (validation-routine.yaml) and its loader/validator.
//
// The routine never names a service that is not a manifest.services[].id; that
// discipline is enforced by the meta-repo datasheet/C4 verifiers, not here.
// This loader enforces the structural contract: a known kind, a tier in the
// allowed set, and the presence of the kind-specific parameters each check
// needs.

// CheckKind is one of the section 3.2 check kinds the routine can name. Each
// maps onto an existing kshark check function; the routine does not introduce
// new diagnostic code.
type CheckKind string

const (
	// KindConnectivity formalizes ADR-0002 GATE 2: the proxy is the contacted
	// endpoint and broker discovery runs via cluster metadata.
	KindConnectivity CheckKind = "connectivity"
	// KindProduceConsume produces a probe message and consumes it back.
	KindProduceConsume CheckKind = "produce-consume"
	// KindTopicPresence asserts each named topic is visible in metadata.
	KindTopicPresence CheckKind = "topic-presence"
	// KindSchemaPresence asserts the expected Schema Registry subjects exist.
	KindSchemaPresence CheckKind = "schema-presence"
	// KindComponentHealth asserts each named component probe returns healthy.
	KindComponentHealth CheckKind = "component-health"
	// KindHTTPCheck is a generic GET against a URL with an expected status.
	KindHTTPCheck CheckKind = "http-check"
)

// knownKinds is the closed set of check kinds the loader accepts.
var knownKinds = map[CheckKind]bool{
	KindConnectivity:    true,
	KindProduceConsume:  true,
	KindTopicPresence:   true,
	KindSchemaPresence:  true,
	KindComponentHealth: true,
	KindHTTPCheck:       true,
}

// Routine tiers mirror the ADR-0001 two-tier smoke model. A core check counts
// toward the verdict unconditionally; a needs-headroom check is excluded by
// policy when SMOKE_TIER excludes it, reading as a policy exclusion, not a
// failure (the TIER-EXCLUDED contract, see verdict computation).
const (
	TierCore          = "core"
	TierNeedsHeadroom = "needs-headroom"
)

// validTiers is the closed set of tiers a check may declare. An empty tier
// defaults to core on load.
var validTiers = map[string]bool{
	TierCore:          true,
	TierNeedsHeadroom: true,
}

// RoutineCheck is one check in a validation routine. The kind-specific
// parameters (topics, subjects, components, url, ...) live under Params so the
// format stays open to the section 3.2 kinds without a struct per kind. The
// SPEC-14 section 9 worked example inlines those params at the check level;
// the loader folds them into Params during normalization so both the nested
// `params:` block and the inlined keys are accepted.
type RoutineCheck struct {
	// ID is the CLAIMS_MAP / smoke test id this check is the runtime twin of
	// (COMP-<comp>-NN, SCEN-<bp>-NN, NEG-<comp>-NN). Required.
	ID string `yaml:"id"`
	// Kind is one of the section 3.2 kinds. Required; unknown kinds error.
	Kind CheckKind `yaml:"kind"`
	// Claim is the datasheet claim this check binds to. A check with no claim
	// is a diagnostic, not a validation, and does not affect the verdict.
	Claim string `yaml:"claim"`
	// Tier is core or needs-headroom. Empty defaults to core.
	Tier string `yaml:"tier"`
	// Params carries the kind-specific parameters.
	Params map[string]any `yaml:"params"`

	// The SPEC-14 section 9 example inlines these at the check level. They are
	// captured here and folded into Params on load.
	Topics    []string       `yaml:"topics"`
	Subjects  []string       `yaml:"subjects"`
	Component []string       `yaml:"components"`
	HTTP      map[string]any `yaml:"http"`
}

// Routine is a parsed validation-routine.yaml. Field names follow SPEC-14
// section 9 (snake_case YAML); the JSON tags carry the camelCase names the BVB
// report stamps under the bvb object.
type Routine struct {
	BlueprintID     string         `yaml:"blueprint" json:"blueprintId"`
	ManifestVersion string         `yaml:"manifest_version" json:"manifestVersion"`
	RoutineID       string         `yaml:"routine_id" json:"routineId"`
	RoutineVersion  string         `yaml:"routine_version" json:"routineVersion"`
	Backend         RoutineBackend `yaml:"backend" json:"backend"`
	Checks          []RoutineCheck `yaml:"checks" json:"-"`
}

// RoutineBackend names the backend family the routine targets. It is asserted,
// not guessed, and is overridable at run time via --backend.
type RoutineBackend struct {
	Family  string `yaml:"family" json:"family"`
	Version string `yaml:"version" json:"version,omitempty"`
}

// loadRoutine reads and validates a validation-routine.yaml file.
func loadRoutine(path string) (*Routine, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read routine: %w", err)
	}
	return parseRoutine(data)
}

// parseRoutine unmarshals and validates routine YAML bytes. It is split from
// loadRoutine so tests can exercise the parser without touching the disk.
func parseRoutine(data []byte) (*Routine, error) {
	var r Routine
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&r); err != nil {
		return nil, fmt.Errorf("parse routine yaml: %w", err)
	}
	if err := r.normalizeAndValidate(); err != nil {
		return nil, err
	}
	return &r, nil
}

// normalizeAndValidate folds inlined check params into Params, defaults the
// tier, and enforces the structural contract.
func (r *Routine) normalizeAndValidate() error {
	if r.RoutineID == "" {
		return fmt.Errorf("routine: routine_id is required")
	}
	if r.RoutineVersion == "" {
		return fmt.Errorf("routine %q: routine_version is required", r.RoutineID)
	}
	if r.BlueprintID == "" {
		return fmt.Errorf("routine %q: blueprint is required", r.RoutineID)
	}
	if len(r.Checks) == 0 {
		return fmt.Errorf("routine %q: at least one check is required", r.RoutineID)
	}

	seen := map[string]bool{}
	for i := range r.Checks {
		c := &r.Checks[i]
		if c.ID == "" {
			return fmt.Errorf("routine %q: check[%d] has no id", r.RoutineID, i)
		}
		if seen[c.ID] {
			return fmt.Errorf("routine %q: duplicate check id %q", r.RoutineID, c.ID)
		}
		seen[c.ID] = true

		if c.Kind == "" {
			return fmt.Errorf("routine %q: check %q has no kind", r.RoutineID, c.ID)
		}
		if !knownKinds[c.Kind] {
			return fmt.Errorf("routine %q: check %q has unknown kind %q", r.RoutineID, c.ID, c.Kind)
		}

		if c.Tier == "" {
			c.Tier = TierCore
		}
		if !validTiers[c.Tier] {
			return fmt.Errorf("routine %q: check %q has invalid tier %q (want %q or %q)",
				r.RoutineID, c.ID, c.Tier, TierCore, TierNeedsHeadroom)
		}

		// Fold inlined params into Params so the runner has one lookup path.
		if c.Params == nil {
			c.Params = map[string]any{}
		}
		if len(c.Topics) > 0 {
			c.Params["topics"] = toAnySlice(c.Topics)
		}
		if len(c.Subjects) > 0 {
			c.Params["subjects"] = toAnySlice(c.Subjects)
		}
		if len(c.Component) > 0 {
			c.Params["components"] = toAnySlice(c.Component)
		}
		if c.HTTP != nil {
			c.Params["http"] = c.HTTP
		}

		if err := validateCheckParams(c); err != nil {
			return fmt.Errorf("routine %q: %w", r.RoutineID, err)
		}
	}
	return nil
}

// validateCheckParams enforces the minimal per-kind parameter contract so a
// malformed routine fails on load, not mid-run.
func validateCheckParams(c *RoutineCheck) error {
	switch c.Kind {
	case KindTopicPresence:
		if len(stringSliceParam(c.Params, "topics")) == 0 {
			return fmt.Errorf("check %q (topic-presence): needs a non-empty topics list", c.ID)
		}
	case KindSchemaPresence:
		if len(stringSliceParam(c.Params, "subjects")) == 0 {
			return fmt.Errorf("check %q (schema-presence): needs a non-empty subjects list", c.ID)
		}
	case KindComponentHealth:
		if len(stringSliceParam(c.Params, "components")) == 0 {
			return fmt.Errorf("check %q (component-health): needs a non-empty components list", c.ID)
		}
	case KindProduceConsume:
		if stringParam(c.Params, "topic") == "" {
			return fmt.Errorf("check %q (produce-consume): needs a topic", c.ID)
		}
	case KindHTTPCheck:
		http, _ := c.Params["http"].(map[string]any)
		if stringParam(c.Params, "url") == "" && stringParam(c.Params, "url_env") == "" &&
			(http == nil || (mapStringParam(http, "url") == "" && mapStringParam(http, "url_env") == "")) {
			return fmt.Errorf("check %q (http-check): needs a url or url_env", c.ID)
		}
	case KindConnectivity:
		// No required params: connectivity reaches the bootstrap endpoint from
		// -props and runs broker discovery.
	}
	return nil
}

// ---------- small param helpers ----------

func toAnySlice(in []string) []any {
	out := make([]any, len(in))
	for i, s := range in {
		out[i] = s
	}
	return out
}

// stringSliceParam reads a []string-shaped param, tolerating both []string and
// []any (the shape yaml.v3 produces for inline lists).
func stringSliceParam(params map[string]any, key string) []string {
	if params == nil {
		return nil
	}
	switch v := params[key].(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func stringParam(params map[string]any, key string) string {
	if params == nil {
		return ""
	}
	if s, ok := params[key].(string); ok {
		return s
	}
	return ""
}

func mapStringParam(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	if s, ok := m[key].(string); ok {
		return s
	}
	return ""
}
