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
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// rowKey uniquely identifies a check row for diff comparison.
type rowKey struct {
	Component string
	Target    string
	Layer     Layer
}

// diffResult holds the comparison between two report rows.
type diffResult struct {
	Key       rowKey
	Before    CheckStatus
	After     CheckStatus
	BeforeDet string
	AfterDet  string
	Kind      string // "IMPROVED", "DEGRADED", "NEW", "REMOVED", "UNCHANGED"
}

// runDiff loads two JSON report files and prints the differences.
func runDiff(path1, path2 string) {
	r1, err := loadReport(path1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading %s: %v\n", path1, err)
		os.Exit(1)
	}
	r2, err := loadReport(path2)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading %s: %v\n", path2, err)
		os.Exit(1)
	}

	// Index rows from report1 by key
	before := map[rowKey]Row{}
	for _, row := range r1.Rows {
		k := rowKey{Component: row.Component, Target: row.Target, Layer: row.Layer}
		before[k] = row
	}

	// Index rows from report2 by key
	after := map[rowKey]Row{}
	for _, row := range r2.Rows {
		k := rowKey{Component: row.Component, Target: row.Target, Layer: row.Layer}
		after[k] = row
	}

	var results []diffResult
	seen := map[rowKey]bool{}

	// Check all rows in report2 against report1
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

	// Check for rows removed (in report1 but not report2)
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

	// Print results
	fmt.Printf("\nkshark diff: %s → %s\n\n", path1, path2)

	if len(results) == 0 {
		fmt.Println("  No differences found.")
		fmt.Println()
		return
	}

	// Header
	fmt.Printf("  %-14s %-26s %-12s %-10s %-10s %s\n",
		"Component", "Target", "Layer", "Before", "After", "")
	fmt.Printf("  %-14s %-26s %-12s %-10s %-10s %s\n",
		strings.Repeat("─", 14), strings.Repeat("─", 26), strings.Repeat("─", 12),
		strings.Repeat("─", 10), strings.Repeat("─", 10), strings.Repeat("─", 12))

	improved, degraded, newCount, removed := 0, 0, 0, 0

	for _, d := range results {
		prefix := " "
		var annotation string
		var color string

		switch d.Kind {
		case "IMPROVED":
			improved++
			annotation = "← IMPROVED"
			color = ColorGreen
		case "DEGRADED":
			degraded++
			annotation = "← DEGRADED"
			color = ColorRed
		case "NEW":
			newCount++
			prefix = "+"
			annotation = "← NEW"
			color = ColorYellow
		case "REMOVED":
			removed++
			prefix = "-"
			annotation = "← REMOVED"
			color = ColorYellow
		}

		beforeStr := string(d.Before)
		afterStr := string(d.After)
		if beforeStr == "" {
			beforeStr = "-"
		}
		if afterStr == "" {
			afterStr = "-"
		}

		fmt.Printf("%s%s %-14s %-26s %-12s %-10s %-10s %s%s\n",
			color, prefix,
			d.Key.Component, truncate(d.Key.Target, 26), string(d.Key.Layer),
			beforeStr, afterStr, annotation, ColorReset)
	}

	fmt.Println()
	fmt.Printf("Summary: %s%d improved%s, %s%d degraded%s, %d new, %d removed\n",
		ColorGreen, improved, ColorReset,
		ColorRed, degraded, ColorReset,
		newCount, removed)
	fmt.Println()
}

// classifyChange determines if a status transition is an improvement or degradation.
func classifyChange(before, after CheckStatus) string {
	rank := map[CheckStatus]int{
		FAIL: 0,
		WARN: 1,
		SKIP: 2,
		OK:   3,
	}
	rb := rank[before]
	ra := rank[after]
	if ra > rb {
		return "IMPROVED"
	}
	return "DEGRADED"
}

// loadReport reads a JSON file and unmarshals it into a Report struct.
func loadReport(path string) (*Report, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	var r Report
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}
	return &r, nil
}
