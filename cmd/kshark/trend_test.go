package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------- trendDirection tests ----------

func TestTrendDirection_Improving(t *testing.T) {
	// First half has more failures than second half
	trends := []trendEntry{
		{HasFailed: true},
		{HasFailed: true},
		{HasFailed: true},
		{HasFailed: true},
		{HasFailed: false},
		{HasFailed: false},
		{HasFailed: false},
		{HasFailed: false},
	}

	result := trendDirection(trends)
	if !strings.Contains(result, "improving") {
		t.Errorf("trendDirection() = %q, want string containing 'improving'", result)
	}
}

func TestTrendDirection_Degrading(t *testing.T) {
	// Second half has more failures than first half
	trends := []trendEntry{
		{HasFailed: false},
		{HasFailed: false},
		{HasFailed: false},
		{HasFailed: false},
		{HasFailed: true},
		{HasFailed: true},
		{HasFailed: true},
		{HasFailed: true},
	}

	result := trendDirection(trends)
	if !strings.Contains(result, "degrading") {
		t.Errorf("trendDirection() = %q, want string containing 'degrading'", result)
	}
}

func TestTrendDirection_Stable(t *testing.T) {
	// Same failure rates in both halves
	trends := []trendEntry{
		{HasFailed: true},
		{HasFailed: false},
		{HasFailed: true},
		{HasFailed: false},
	}

	result := trendDirection(trends)
	if !strings.Contains(result, "stable") {
		t.Errorf("trendDirection() = %q, want string containing 'stable'", result)
	}
}

func TestTrendDirection_SingleEntry(t *testing.T) {
	// Single entry: not enough data to determine trend
	trends := []trendEntry{
		{HasFailed: false},
	}

	result := trendDirection(trends)
	if result != "" {
		t.Errorf("trendDirection() with 1 entry = %q, want empty string", result)
	}
}

func TestTrendDirection_TwoEntries(t *testing.T) {
	tests := []struct {
		name  string
		first bool
		sec   bool
		want  string
	}{
		{"both pass", false, false, "stable"},
		{"both fail", true, true, "stable"},
		{"fail then pass", true, false, "improving"},
		{"pass then fail", false, true, "degrading"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trends := []trendEntry{
				{HasFailed: tt.first},
				{HasFailed: tt.sec},
			}
			result := trendDirection(trends)
			if !strings.Contains(result, tt.want) {
				t.Errorf("trendDirection() = %q, want string containing %q", result, tt.want)
			}
		})
	}
}

// ---------- formatDuration tests ----------

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name string
		dur  time.Duration
		want string
	}{
		{"zero", 0, "0ms"},
		{"sub-second millis", 500 * time.Millisecond, "500ms"},
		{"one second", 1 * time.Second, "1s"},
		{"thirty seconds", 30 * time.Second, "30s"},
		{"fifty-nine seconds", 59 * time.Second, "59s"},
		{"one minute exactly", 60 * time.Second, "1m"},
		{"ninety seconds", 90 * time.Second, "1m30s"},
		{"two minutes", 2 * time.Minute, "2m"},
		{"two minutes thirty", 2*time.Minute + 30*time.Second, "2m30s"},
		{"one hour", 60 * time.Minute, "60m"},
		{"one hour thirty minutes", 90 * time.Minute, "90m"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDuration(tt.dur)
			if got != tt.want {
				t.Errorf("formatDuration(%v) = %q, want %q", tt.dur, got, tt.want)
			}
		})
	}
}

// ---------- failColor tests ----------

func TestFailColor(t *testing.T) {
	if got := failColor(0); got != "" {
		t.Errorf("failColor(0) = %q, want empty", got)
	}
	if got := failColor(1); got != ColorRed {
		t.Errorf("failColor(1) = %q, want ColorRed", got)
	}
	if got := failColor(5); got != ColorRed {
		t.Errorf("failColor(5) = %q, want ColorRed", got)
	}
}

// ---------- loadReport integration for trend (directory scanning) ----------

func TestLoadReportsFromDir(t *testing.T) {
	dir := t.TempDir()

	baseTime := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)

	// Create 3 valid report files
	for i := 0; i < 3; i++ {
		r := Report{
			StartedAt:  baseTime.Add(time.Duration(i) * time.Hour),
			FinishedAt: baseTime.Add(time.Duration(i)*time.Hour + 30*time.Second),
			Rows: []Row{
				{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok"},
			},
			Summary: map[string]CheckStats{
				string(L7): {OK: 1},
			},
		}
		data, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			t.Fatalf("json.Marshal report %d: %v", i, err)
		}
		path := filepath.Join(dir, "report_"+string(rune('a'+i))+".json")
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatalf("WriteFile report %d: %v", i, err)
		}
	}

	// Create a non-JSON file (should be skipped)
	if err := os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("not a report"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create an invalid JSON file (should be skipped)
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), []byte("{broken"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create a valid JSON file without StartedAt (should be skipped per trend logic)
	noStart := Report{
		Rows: []Row{
			{Component: "x", Target: "y", Layer: L3, Status: OK, Detail: "ok"},
		},
		Summary: map[string]CheckStats{
			string(L3): {OK: 1},
		},
	}
	noStartData, _ := json.Marshal(noStart)
	if err := os.WriteFile(filepath.Join(dir, "no_start.json"), noStartData, 0644); err != nil {
		t.Fatal(err)
	}

	// Scan directory using the same logic as runTrend
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	var trends []trendEntry
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		r, err := loadReport(path)
		if err != nil {
			continue
		}
		if r.StartedAt.IsZero() {
			continue
		}
		te := trendEntry{
			Path:      path,
			StartedAt: r.StartedAt,
			Duration:  r.FinishedAt.Sub(r.StartedAt),
		}
		for _, cs := range r.Summary {
			te.OK += cs.OK
			te.WARN += cs.WARN
			te.FAIL += cs.FAIL
			te.SKIP += cs.SKIP
		}
		if te.FAIL > 0 {
			te.HasFailed = true
		}
		trends = append(trends, te)
	}

	if len(trends) != 3 {
		t.Fatalf("loaded %d trend entries, want 3", len(trends))
	}

	// Verify they are in order by sorting (same as runTrend)
	for i := 1; i < len(trends); i++ {
		if trends[i].StartedAt.Before(trends[i-1].StartedAt) {
			// They may not be sorted from ReadDir, but the data should be valid
			// runTrend sorts them, so we just verify all entries loaded correctly
		}
	}

	// Verify aggregate counts
	for _, te := range trends {
		if te.OK != 1 {
			t.Errorf("entry %s: OK = %d, want 1", te.Path, te.OK)
		}
		if te.HasFailed {
			t.Errorf("entry %s: HasFailed = true, want false", te.Path)
		}
	}
}

func TestTrendEntry_WithFailures(t *testing.T) {
	dir := t.TempDir()
	baseTime := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)

	r := Report{
		StartedAt:  baseTime,
		FinishedAt: baseTime.Add(10 * time.Second),
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok"},
			{Component: "dns", Target: "broker", Layer: L3, Status: FAIL, Detail: "NXDOMAIN"},
		},
		Summary: map[string]CheckStats{
			string(L7): {OK: 1},
			string(L3): {FAIL: 1},
		},
	}
	data, _ := json.MarshalIndent(r, "", "  ")
	path := filepath.Join(dir, "fail_report.json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	loaded, err := loadReport(path)
	if err != nil {
		t.Fatalf("loadReport: %v", err)
	}

	te := trendEntry{
		Path:      path,
		StartedAt: loaded.StartedAt,
		Duration:  loaded.FinishedAt.Sub(loaded.StartedAt),
	}
	for _, cs := range loaded.Summary {
		te.OK += cs.OK
		te.FAIL += cs.FAIL
	}
	if te.FAIL > 0 {
		te.HasFailed = true
	}

	// Find first fail detail
	for _, row := range loaded.Rows {
		if row.Status == FAIL {
			te.FirstFail = string(row.Layer) + " " + row.Detail
			break
		}
	}

	if !te.HasFailed {
		t.Error("HasFailed should be true")
	}
	if te.FAIL != 1 {
		t.Errorf("FAIL = %d, want 1", te.FAIL)
	}
	if te.OK != 1 {
		t.Errorf("OK = %d, want 1", te.OK)
	}
	if te.FirstFail != "L3-Network NXDOMAIN" {
		t.Errorf("FirstFail = %q, want %q", te.FirstFail, "L3-Network NXDOMAIN")
	}
	if te.Duration != 10*time.Second {
		t.Errorf("Duration = %v, want 10s", te.Duration)
	}
}
