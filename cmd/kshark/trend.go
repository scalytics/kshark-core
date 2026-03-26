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
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// trendEntry holds aggregated stats for a single report file.
type trendEntry struct {
	Path      string
	StartedAt time.Time
	Duration  time.Duration
	OK        int
	WARN      int
	FAIL      int
	SKIP      int
	HasFailed bool
	FirstFail string // detail of first FAIL row (for "last failure" display)
}

// runTrend scans the reports/ directory, loads JSON reports, and prints a
// historical trend table.
func runTrend(lastN int) {
	reportsDir := "reports"

	entries, err := os.ReadDir(reportsDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading reports directory: %v\n", err)
		os.Exit(1)
	}

	var trends []trendEntry

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		path := filepath.Join(reportsDir, entry.Name())
		r, err := loadReport(path)
		if err != nil {
			continue // skip non-report JSON files
		}

		// Validate that this looks like a kshark report (has StartedAt)
		if r.StartedAt.IsZero() {
			continue
		}

		te := trendEntry{
			Path:      path,
			StartedAt: r.StartedAt,
			Duration:  r.FinishedAt.Sub(r.StartedAt),
		}

		// Aggregate counts from Summary
		for _, cs := range r.Summary {
			te.OK += cs.OK
			te.WARN += cs.WARN
			te.FAIL += cs.FAIL
			te.SKIP += cs.SKIP
		}

		if te.FAIL > 0 {
			te.HasFailed = true
		}

		// Find first FAIL detail for display
		for _, row := range r.Rows {
			if row.Status == FAIL {
				te.FirstFail = fmt.Sprintf("%s %s", string(row.Layer), row.Detail)
				if len(te.FirstFail) > 50 {
					te.FirstFail = te.FirstFail[:47] + "..."
				}
				break
			}
		}

		trends = append(trends, te)
	}

	if len(trends) == 0 {
		fmt.Println("\nkshark trend — no report files found in reports/")
		fmt.Println("Run a scan with -json to generate reports.")
		return
	}

	// Sort by StartedAt ascending
	sort.Slice(trends, func(i, j int) bool {
		return trends[i].StartedAt.Before(trends[j].StartedAt)
	})

	// Take last N
	if lastN > 0 && len(trends) > lastN {
		trends = trends[len(trends)-lastN:]
	}

	fmt.Printf("\nkshark trend — last %d scans\n\n", len(trends))

	// Header
	fmt.Printf("  %-4s %-22s %-10s %4s %5s %5s  %-8s\n",
		"#", "Date", "Duration", "OK", "WARN", "FAIL", "Status")
	fmt.Printf("  %-4s %-22s %-10s %4s %5s %5s  %-8s\n",
		strings.Repeat("─", 4), strings.Repeat("─", 22), strings.Repeat("─", 10),
		strings.Repeat("─", 4), strings.Repeat("─", 5), strings.Repeat("─", 5),
		strings.Repeat("─", 8))

	passed := 0
	var totalDuration time.Duration
	var lastFailTime time.Time
	var lastFailDetail string

	for i, te := range trends {
		status := ColorGreen + "OK" + ColorReset
		if te.HasFailed {
			status = ColorRed + "FAIL" + ColorReset
			lastFailTime = te.StartedAt
			lastFailDetail = te.FirstFail
		} else {
			passed++
		}

		totalDuration += te.Duration

		durStr := formatDuration(te.Duration)

		fmt.Printf("  %-4d %-22s %-10s %4d %5d %s%5d%s  %s\n",
			i+1,
			te.StartedAt.Format("2006-01-02 15:04"),
			durStr,
			te.OK,
			te.WARN,
			failColor(te.FAIL), te.FAIL, ColorReset,
			status)
	}

	fmt.Println()

	// Summary line
	total := len(trends)
	pct := 0
	if total > 0 {
		pct = (passed * 100) / total
	}
	avgDur := formatDuration(totalDuration / time.Duration(total))

	// Trend direction
	trendDir := trendDirection(trends)

	fmt.Printf("Trend: %d/%d passed (%d%%), avg duration %s %s\n",
		passed, total, pct, avgDur, trendDir)

	if !lastFailTime.IsZero() {
		fmt.Printf("Last failure: %s (%s)\n", lastFailTime.Format("2006-01-02 15:04"), lastFailDetail)
	}
	fmt.Println()
}

// trendDirection analyzes recent results to determine if health is improving or degrading.
func trendDirection(trends []trendEntry) string {
	if len(trends) < 2 {
		return ""
	}

	// Compare first half vs second half failure rates
	mid := len(trends) / 2
	firstHalf := trends[:mid]
	secondHalf := trends[mid:]

	firstFails := 0
	for _, t := range firstHalf {
		if t.HasFailed {
			firstFails++
		}
	}

	secondFails := 0
	for _, t := range secondHalf {
		if t.HasFailed {
			secondFails++
		}
	}

	firstRate := float64(firstFails) / float64(len(firstHalf))
	secondRate := float64(secondFails) / float64(len(secondHalf))

	if secondRate < firstRate {
		return ColorGreen + "↑ improving" + ColorReset
	} else if secondRate > firstRate {
		return ColorRed + "↓ degrading" + ColorReset
	}
	return "→ stable"
}

// formatDuration formats a duration into a human-readable short string.
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	m := int(d.Minutes())
	s := int(d.Seconds()) % 60
	if s == 0 {
		return fmt.Sprintf("%dm", m)
	}
	return fmt.Sprintf("%dm%ds", m, s)
}

// failColor returns red color code if count > 0, empty string otherwise.
func failColor(count int) string {
	if count > 0 {
		return ColorRed
	}
	return ""
}
