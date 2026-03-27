package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ---------- loadReport tests ----------

func TestLoadReport_Valid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.json")

	r := Report{
		StartedAt:  time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		FinishedAt: time.Date(2026, 1, 1, 10, 0, 5, 0, time.UTC),
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ApiVersions OK"},
			{Component: "dns", Target: "broker", Layer: L3, Status: FAIL, Detail: "DNS lookup failed"},
		},
		Summary: map[string]CheckStats{
			string(L7): {OK: 1},
			string(L3): {FAIL: 1},
		},
	}

	data, err := json.Marshal(&r)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	loaded, err := loadReport(path)
	if err != nil {
		t.Fatalf("loadReport() error = %v", err)
	}
	if len(loaded.Rows) != 2 {
		t.Errorf("loaded %d rows, want 2", len(loaded.Rows))
	}
	if loaded.Rows[0].Component != "kafka" {
		t.Errorf("Rows[0].Component = %q, want %q", loaded.Rows[0].Component, "kafka")
	}
	if loaded.Rows[1].Status != FAIL {
		t.Errorf("Rows[1].Status = %q, want %q", loaded.Rows[1].Status, FAIL)
	}
	if loaded.Summary[string(L3)].FAIL != 1 {
		t.Errorf("Summary[L3].FAIL = %d, want 1", loaded.Summary[string(L3)].FAIL)
	}
}

func TestLoadReport_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte(`{not valid json`), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := loadReport(path)
	if err == nil {
		t.Fatal("loadReport() expected error for invalid JSON, got nil")
	}
}

func TestLoadReport_MissingFile(t *testing.T) {
	_, err := loadReport("/nonexistent/path/report.json")
	if err == nil {
		t.Fatal("loadReport() expected error for missing file, got nil")
	}
}

// ---------- classifyChange tests ----------

func TestClassifyChange(t *testing.T) {
	tests := []struct {
		name   string
		before CheckStatus
		after  CheckStatus
		want   string
	}{
		{"FAIL to OK is improved", FAIL, OK, "IMPROVED"},
		{"OK to FAIL is degraded", OK, FAIL, "DEGRADED"},
		{"FAIL to WARN is improved", FAIL, WARN, "IMPROVED"},
		{"WARN to OK is improved", WARN, OK, "IMPROVED"},
		{"OK to WARN is degraded", OK, WARN, "DEGRADED"},
		{"WARN to FAIL is degraded", WARN, FAIL, "DEGRADED"},
		{"SKIP to OK is improved", SKIP, OK, "IMPROVED"},
		{"OK to SKIP is degraded", OK, SKIP, "DEGRADED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyChange(tt.before, tt.after)
			if got != tt.want {
				t.Errorf("classifyChange(%s, %s) = %q, want %q", tt.before, tt.after, got, tt.want)
			}
		})
	}
}

// ---------- diff logic tests ----------
//
// runDiff calls loadReport + prints to stdout with os.Exit on error.
// To test the diff comparison logic in isolation, we replicate the
// core algorithm from runDiff using the same key/result types.

// computeDiff reproduces the comparison logic from runDiff for testability.
func computeDiff(r1, r2 *Report) []diffResult {
	before := map[rowKey]Row{}
	for _, row := range r1.Rows {
		k := rowKey{Component: row.Component, Target: row.Target, Layer: row.Layer}
		before[k] = row
	}

	after := map[rowKey]Row{}
	for _, row := range r2.Rows {
		k := rowKey{Component: row.Component, Target: row.Target, Layer: row.Layer}
		after[k] = row
	}

	var results []diffResult
	seen := map[rowKey]bool{}

	for _, row := range r2.Rows {
		k := rowKey{Component: row.Component, Target: row.Target, Layer: row.Layer}
		if seen[k] {
			continue
		}
		seen[k] = true

		prev, existed := before[k]
		if !existed {
			results = append(results, diffResult{
				Key:      k,
				Before:   "",
				After:    row.Status,
				AfterDet: row.Detail,
				Kind:     "NEW",
			})
			continue
		}
		if prev.Status != row.Status {
			kind := classifyChange(prev.Status, row.Status)
			results = append(results, diffResult{
				Key:       k,
				Before:    prev.Status,
				After:     row.Status,
				BeforeDet: prev.Detail,
				AfterDet:  row.Detail,
				Kind:      kind,
			})
		}
	}

	for _, row := range r1.Rows {
		k := rowKey{Component: row.Component, Target: row.Target, Layer: row.Layer}
		if seen[k] {
			continue
		}
		seen[k] = true

		if _, exists := after[k]; !exists {
			results = append(results, diffResult{
				Key:       k,
				Before:    row.Status,
				After:     "",
				BeforeDet: row.Detail,
				Kind:      "REMOVED",
			})
		}
	}

	return results
}

func countKinds(results []diffResult) (improved, degraded, newCount, removed int) {
	for _, d := range results {
		switch d.Kind {
		case "IMPROVED":
			improved++
		case "DEGRADED":
			degraded++
		case "NEW":
			newCount++
		case "REMOVED":
			removed++
		}
	}
	return
}

func TestDiffReports_NoChanges(t *testing.T) {
	r1 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok"},
			{Component: "dns", Target: "broker", Layer: L3, Status: OK, Detail: "resolved"},
		},
	}
	r2 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok"},
			{Component: "dns", Target: "broker", Layer: L3, Status: OK, Detail: "resolved"},
		},
	}

	results := computeDiff(r1, r2)
	improved, degraded, newCount, removed := countKinds(results)

	if improved != 0 || degraded != 0 || newCount != 0 || removed != 0 {
		t.Errorf("expected 0 changes, got improved=%d degraded=%d new=%d removed=%d",
			improved, degraded, newCount, removed)
	}
}

func TestDiffReports_StatusImproved(t *testing.T) {
	r1 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L4, Status: FAIL, Detail: "TCP timeout"},
		},
	}
	r2 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L4, Status: OK, Detail: "TCP connected"},
		},
	}

	results := computeDiff(r1, r2)
	improved, degraded, newCount, removed := countKinds(results)

	if improved != 1 {
		t.Errorf("improved = %d, want 1", improved)
	}
	if degraded != 0 || newCount != 0 || removed != 0 {
		t.Errorf("unexpected changes: degraded=%d new=%d removed=%d", degraded, newCount, removed)
	}
	if results[0].Before != FAIL {
		t.Errorf("Before = %q, want %q", results[0].Before, FAIL)
	}
	if results[0].After != OK {
		t.Errorf("After = %q, want %q", results[0].After, OK)
	}
}

func TestDiffReports_StatusDegraded(t *testing.T) {
	r1 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L4, Status: OK, Detail: "TCP connected"},
		},
	}
	r2 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L4, Status: FAIL, Detail: "TCP timeout"},
		},
	}

	results := computeDiff(r1, r2)
	improved, degraded, newCount, removed := countKinds(results)

	if degraded != 1 {
		t.Errorf("degraded = %d, want 1", degraded)
	}
	if improved != 0 || newCount != 0 || removed != 0 {
		t.Errorf("unexpected changes: improved=%d new=%d removed=%d", improved, newCount, removed)
	}
}

func TestDiffReports_NewRow(t *testing.T) {
	r1 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok"},
		},
	}
	r2 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok"},
			{Component: "rest", Target: "proxy:8082", Layer: HTTP, Status: OK, Detail: "HTTP OK"},
		},
	}

	results := computeDiff(r1, r2)
	improved, degraded, newCount, removed := countKinds(results)

	if newCount != 1 {
		t.Errorf("new = %d, want 1", newCount)
	}
	if improved != 0 || degraded != 0 || removed != 0 {
		t.Errorf("unexpected changes: improved=%d degraded=%d removed=%d", improved, degraded, removed)
	}

	// Verify the new row details
	for _, d := range results {
		if d.Kind == "NEW" {
			if d.Key.Component != "rest" {
				t.Errorf("NEW row component = %q, want %q", d.Key.Component, "rest")
			}
			if d.Before != "" {
				t.Errorf("NEW row Before = %q, want empty", d.Before)
			}
		}
	}
}

func TestDiffReports_RemovedRow(t *testing.T) {
	r1 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok"},
			{Component: "rest", Target: "proxy:8082", Layer: HTTP, Status: OK, Detail: "HTTP OK"},
		},
	}
	r2 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok"},
		},
	}

	results := computeDiff(r1, r2)
	improved, degraded, newCount, removed := countKinds(results)

	if removed != 1 {
		t.Errorf("removed = %d, want 1", removed)
	}
	if improved != 0 || degraded != 0 || newCount != 0 {
		t.Errorf("unexpected changes: improved=%d degraded=%d new=%d", improved, degraded, newCount)
	}

	// Verify the removed row details
	for _, d := range results {
		if d.Kind == "REMOVED" {
			if d.Key.Component != "rest" {
				t.Errorf("REMOVED row component = %q, want %q", d.Key.Component, "rest")
			}
			if d.After != "" {
				t.Errorf("REMOVED row After = %q, want empty", d.After)
			}
		}
	}
}

func TestDiffReports_MixedChanges(t *testing.T) {
	r1 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L4, Status: FAIL, Detail: "timeout"},      // will improve
			{Component: "dns", Target: "broker", Layer: L3, Status: OK, Detail: "resolved"},               // will degrade
			{Component: "tls", Target: "broker:9092", Layer: L56, Status: OK, Detail: "valid cert"},       // unchanged
			{Component: "diag", Target: "traceroute", Layer: DIAG, Status: WARN, Detail: "high latency"},  // will be removed
		},
	}
	r2 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L4, Status: OK, Detail: "connected"},      // improved
			{Component: "dns", Target: "broker", Layer: L3, Status: FAIL, Detail: "NXDOMAIN"},             // degraded
			{Component: "tls", Target: "broker:9092", Layer: L56, Status: OK, Detail: "valid cert"},       // unchanged
			{Component: "rest", Target: "proxy:8082", Layer: HTTP, Status: OK, Detail: "topics listed"},   // new
		},
	}

	results := computeDiff(r1, r2)
	improved, degraded, newCount, removed := countKinds(results)

	if improved != 1 {
		t.Errorf("improved = %d, want 1", improved)
	}
	if degraded != 1 {
		t.Errorf("degraded = %d, want 1", degraded)
	}
	if newCount != 1 {
		t.Errorf("new = %d, want 1", newCount)
	}
	if removed != 1 {
		t.Errorf("removed = %d, want 1", removed)
	}
	if len(results) != 4 {
		t.Errorf("total results = %d, want 4", len(results))
	}
}

func TestDiffReports_DuplicateRowsInReport(t *testing.T) {
	// Verify that duplicate rows (same key) are deduplicated
	r1 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok"},
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok again"},
		},
	}
	r2 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: FAIL, Detail: "failed"},
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: FAIL, Detail: "failed again"},
		},
	}

	results := computeDiff(r1, r2)
	// Should produce exactly 1 result (deduped by key)
	if len(results) != 1 {
		t.Errorf("expected 1 result for duplicate keys, got %d", len(results))
	}
	if results[0].Kind != "DEGRADED" {
		t.Errorf("Kind = %q, want DEGRADED", results[0].Kind)
	}
}

func TestDiffReports_EmptyReports(t *testing.T) {
	r1 := &Report{}
	r2 := &Report{}

	results := computeDiff(r1, r2)
	if len(results) != 0 {
		t.Errorf("expected 0 results for two empty reports, got %d", len(results))
	}
}

func TestDiffReports_AllNew(t *testing.T) {
	r1 := &Report{}
	r2 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok"},
			{Component: "dns", Target: "broker", Layer: L3, Status: OK, Detail: "resolved"},
		},
	}

	results := computeDiff(r1, r2)
	_, _, newCount, _ := countKinds(results)
	if newCount != 2 {
		t.Errorf("new = %d, want 2", newCount)
	}
}

// ---------- runDiff integration test (happy path, no os.Exit) ----------

func TestRunDiff_IdenticalReports(t *testing.T) {
	dir := t.TempDir()

	r := Report{
		StartedAt:  time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		FinishedAt: time.Date(2026, 1, 1, 10, 0, 5, 0, time.UTC),
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok"},
		},
	}
	data, _ := json.Marshal(&r)
	path1 := filepath.Join(dir, "report1.json")
	path2 := filepath.Join(dir, "report2.json")
	os.WriteFile(path1, data, 0644)
	os.WriteFile(path2, data, 0644)

	// runDiff prints to stdout and returns (no os.Exit for valid files)
	runDiff(path1, path2)
}

func TestRunDiff_WithChanges(t *testing.T) {
	dir := t.TempDir()

	r1 := Report{
		StartedAt:  time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		FinishedAt: time.Date(2026, 1, 1, 10, 0, 5, 0, time.UTC),
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L4, Status: FAIL, Detail: "TCP timeout"},
		},
	}
	r2 := Report{
		StartedAt:  time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC),
		FinishedAt: time.Date(2026, 1, 1, 11, 0, 3, 0, time.UTC),
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L4, Status: OK, Detail: "Connected"},
		},
	}
	d1, _ := json.Marshal(&r1)
	d2, _ := json.Marshal(&r2)
	path1 := filepath.Join(dir, "report1.json")
	path2 := filepath.Join(dir, "report2.json")
	os.WriteFile(path1, d1, 0644)
	os.WriteFile(path2, d2, 0644)

	runDiff(path1, path2)
}

func TestDiffReports_AllRemoved(t *testing.T) {
	r1 := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok"},
			{Component: "dns", Target: "broker", Layer: L3, Status: OK, Detail: "resolved"},
		},
	}
	r2 := &Report{}

	results := computeDiff(r1, r2)
	_, _, _, removed := countKinds(results)
	if removed != 2 {
		t.Errorf("removed = %d, want 2", removed)
	}
}
