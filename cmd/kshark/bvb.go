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

import "time"

// ---------- Blueprint-Validation-Bundle (SPEC-14 section 4) ----------
//
// The BVB object is additive on the existing Report. An existing kshark report
// consumer ignores the bvb field; the raw L3-to-L7 diagnostics stay in the
// top-level rows/summary. bvb.checks is the routine-aware, claim-bound view.

// VerdictStatus is GREEN or RED, mirroring the GATE-1 rule
// PASS + TIER-EXCLUDED == total.
type VerdictStatus string

const (
	VerdictGreen VerdictStatus = "GREEN"
	VerdictRed   VerdictStatus = "RED"
)

// BVBCheckStatus is the routine-check vocabulary: PASS | FAIL | SKIP.
type BVBCheckStatus string

const (
	CheckPASS BVBCheckStatus = "PASS"
	CheckFAIL BVBCheckStatus = "FAIL"
	CheckSKIP BVBCheckStatus = "SKIP"
)

// tierExcludedPrefix is the gate-parser contract: a SKIP whose skip_reason
// carries this prefix is a policy exclusion, not a failure, and does not drag
// the verdict.
const tierExcludedPrefix = "TIER-EXCLUDED:"

// BVB is the blueprint-scoping, claim-binding, digest-pinning, verdict object
// added to a kshark report when run via validate-blueprint.
type BVB struct {
	BlueprintID     string        `json:"blueprintId"`
	ManifestVersion string        `json:"manifestVersion,omitempty"`
	RoutineID       string        `json:"routineId"`
	RoutineVersion  string        `json:"routineVersion"`
	Backend         BVBBackend    `json:"backend"`
	ImageDigests    []ImageDigest `json:"imageDigests"`
	Environment     BVBEnv        `json:"environment"`
	StartedAt       time.Time     `json:"startedAt"`
	FinishedAt      time.Time     `json:"finishedAt"`
	Checks          []BVBCheck    `json:"checks"`
	Verdict         Verdict       `json:"verdict"`
}

// BVBBackend is the asserted backend identity. family in
// {kafscale, confluent, redpanda, msk}; version from broker metadata when
// available.
type BVBBackend struct {
	Family  string `json:"family"`
	Version string `json:"version,omitempty"`
}

// ImageDigest pins a multi-arch manifest-list digest for one image.
type ImageDigest struct {
	Image     string   `json:"image"`
	Digest    string   `json:"digest"`
	Platforms []string `json:"platforms,omitempty"`
}

// BVBEnv captures the run environment. host/arch come from captureSystemContext;
// advertisedEndpoint is the bootstrap the run pointed at; diskFsyncFloorNote is
// the 4k-O_DSYNC etcd-floor note when cheaply available, omitted otherwise.
type BVBEnv struct {
	Host               string `json:"host,omitempty"`
	Arch               string `json:"arch,omitempty"`
	AdvertisedEndpoint string `json:"advertisedEndpoint,omitempty"`
	DiskFsyncFloorNote string `json:"diskFsyncFloorNote,omitempty"`
}

// BVBCheck is one routine-aware, claim-bound check result.
type BVBCheck struct {
	ID         string         `json:"id"`
	Kind       CheckKind      `json:"kind"`
	Claim      string         `json:"claim,omitempty"`
	Tier       string         `json:"tier"`
	Status     BVBCheckStatus `json:"status"`
	SkipReason string         `json:"skipReason,omitempty"`
	Evidence   []string       `json:"evidence,omitempty"`
	DurationMs int64          `json:"durationMs"`
	// Rows back-references into report.rows that produced this check.
	Rows []string `json:"rows,omitempty"`
}

// Verdict aggregates the checks. status is GREEN iff every check is PASS or
// tier-excluded, RED otherwise.
type Verdict struct {
	Status       VerdictStatus `json:"status"`
	Pass         int           `json:"pass"`
	Fail         int           `json:"fail"`
	Skip         int           `json:"skip"`
	TierExcluded int           `json:"tierExcluded"`
	Total        int           `json:"total"`
}

// isTierExcluded reports whether a SKIP carries the TIER-EXCLUDED contract.
func (c BVBCheck) isTierExcluded() bool {
	return c.Status == CheckSKIP && len(c.SkipReason) >= len(tierExcludedPrefix) &&
		c.SkipReason[:len(tierExcludedPrefix)] == tierExcludedPrefix
}

// computeVerdict aggregates check results into a verdict, mirroring the GATE-1
// rule: GREEN iff PASS + TIER-EXCLUDED == total. A FAIL, or a SKIP that is not
// tier-excluded, makes the verdict RED.
func computeVerdict(checks []BVBCheck) Verdict {
	v := Verdict{Status: VerdictGreen, Total: len(checks)}
	for _, c := range checks {
		switch c.Status {
		case CheckPASS:
			v.Pass++
		case CheckFAIL:
			v.Fail++
		case CheckSKIP:
			v.Skip++
			if c.isTierExcluded() {
				v.TierExcluded++
			}
		}
	}
	// GREEN iff every non-tier-excluded check passed.
	if v.Pass+v.TierExcluded != v.Total {
		v.Status = VerdictRed
	}
	return v
}
