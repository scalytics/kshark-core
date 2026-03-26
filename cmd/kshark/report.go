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
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// ---------- Formatting Constants ----------

const (
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorReset  = "\033[0m"
	IconOK      = "✅"
	IconWarn    = "⚠️"
	IconFail    = "❌"
	IconSkip    = "⚪"
)

// ---------- Models ----------

type CheckStatus string

const (
	OK   CheckStatus = "OK"
	WARN CheckStatus = "WARN"
	FAIL CheckStatus = "FAIL"
	SKIP CheckStatus = "SKIP"
)

type Layer string

const (
	L3   Layer = "L3-Network"
	L4   Layer = "L4-TCP"
	L56  Layer = "L5-6-TLS"
	L7   Layer = "L7-Kafka"
	HTTP Layer = "L7-HTTP"
	DIAG Layer = "Diag"
)

type Row struct {
	Component string      `json:"component"`
	Target    string      `json:"target"`
	Layer     Layer       `json:"layer"`
	Status    CheckStatus `json:"status"`
	Detail    string      `json:"detail"`
	Hint      string      `json:"hint,omitempty"`
}

type Report struct {
	Rows       []Row                 `json:"rows"`
	Summary    map[string]CheckStats `json:"summary"`
	StartedAt  time.Time             `json:"started_at"`
	FinishedAt time.Time             `json:"finished_at"`
	ConfigEcho map[string]string     `json:"config_echo,omitempty"`
	Analysis   *AnalysisMeta         `json:"analysis,omitempty"`
	Run        *RunMeta              `json:"run,omitempty"`
	Artifacts  *ArtifactsMeta        `json:"artifacts,omitempty"`
	HasFailed  bool                  `json:"-"`
}

type CheckStats struct {
	OK   int `json:"ok"`
	WARN int `json:"warn"`
	FAIL int `json:"fail"`
	SKIP int `json:"skip"`
}

type AnalysisMeta struct {
	PromptFile      string `json:"prompt_file,omitempty"`
	PromptSHA256    string `json:"prompt_sha256,omitempty"`
	PromptContent   string `json:"prompt_content,omitempty"`
	ResponseFile    string `json:"response_file,omitempty"`
	ResponseSHA256  string `json:"response_sha256,omitempty"`
	ResponseContent string `json:"response_content,omitempty"`
	ResponseStatus  string `json:"response_status,omitempty"`
	ResponseError   string `json:"response_error,omitempty"`
	Provider        string `json:"provider,omitempty"`
	Model           string `json:"model,omitempty"`
}

type RunMeta struct {
	Args   []string             `json:"args,omitempty"`
	Params map[string]ParamMeta `json:"params,omitempty"`
}

type ParamMeta struct {
	Value     string `json:"value,omitempty"`
	Default   string `json:"default,omitempty"`
	IsDefault bool   `json:"is_default,omitempty"`
	Provided  bool   `json:"provided,omitempty"`
}

type ArtifactsMeta struct {
	LogFile       string `json:"log_file,omitempty"`
	LogSHA256     string `json:"log_sha256,omitempty"`
	LogContent    string `json:"log_content,omitempty"`
	PromptFile    string `json:"prompt_file,omitempty"`
	PromptSHA256  string `json:"prompt_sha256,omitempty"`
	PromptContent string `json:"prompt_content,omitempty"`
	ReportChecksumFile string `json:"report_checksum_file,omitempty"`
	ReportChecksum     string `json:"report_checksum,omitempty"`
}

func addRow(r *Report, row Row) {
	if row.Status == FAIL {
		r.HasFailed = true
	}
	r.Rows = append(r.Rows, row)
	slog.Debug("row added", "component", row.Component, "target", row.Target, "layer", row.Layer, "status", row.Status, "detail", row.Detail, "hint", row.Hint)
}

func summarize(r *Report) {
	r.Summary = map[string]CheckStats{}
	for _, row := range r.Rows {
		cs := r.Summary[string(row.Layer)]
		switch row.Status {
		case OK:
			cs.OK++
		case WARN:
			cs.WARN++
		case FAIL:
			cs.FAIL++
		case SKIP:
			cs.SKIP++
		}
		r.Summary[string(row.Layer)] = cs
	}
}

func printPretty(r *Report) {
	fmt.Printf("\nKafka Wire Health Report  (%s → %s)  on %s\n",
		r.StartedAt.Format(time.RFC3339), r.FinishedAt.Format(time.RFC3339), runtime.GOOS)
	fmt.Println(strings.Repeat("─", 92))
	fmt.Printf("%-4s %-14s %-26s %-12s %s\n", " ", "Component", "Target", "Layer", "Detail")
	fmt.Println(strings.Repeat("─", 92))

	for _, row := range r.Rows {
		var color, icon string
		switch row.Status {
		case OK:
			color = ColorGreen
			icon = IconOK
		case WARN:
			color = ColorYellow
			icon = IconWarn
		case FAIL:
			color = ColorRed
			icon = IconFail
		case SKIP:
			color = "" // No color for skip
			icon = IconSkip
		}

		fmt.Printf("%s%-4s %-14s %-26s %-12s %s%s\n",
			color, icon, row.Component, truncate(row.Target, 26), row.Layer, row.Detail, ColorReset)

		if row.Hint != "" && row.Status != OK {
			fmt.Printf("  %s↳ Hint: %s%s\n", ColorYellow, row.Hint, ColorReset)
		}
	}
	fmt.Println(strings.Repeat("─", 92))
	for layer, s := range r.Summary {
		fmt.Printf("%-12s  %sOK:%d%s  %sWARN:%d%s  %sFAIL:%d%s  SKIP:%d\n",
			layer,
			ColorGreen, s.OK, ColorReset,
			ColorYellow, s.WARN, ColorReset,
			ColorRed, s.FAIL, ColorReset,
			s.SKIP)
	}
	fmt.Println()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func writeHTMLReport(r *Report, analysis *AIAnalysisResponse) (string, error) {
	// Data structure to pass to the template
	type TemplateData struct {
		Report   *Report
		Analysis *AIAnalysisResponse
	}

	data := TemplateData{
		Report:   r,
		Analysis: analysis,
	}

	// Custom functions for the template
	funcMap := template.FuncMap{
		"ToLower": func(s CheckStatus) string {
			return strings.ToLower(string(s))
		},
		"Icon": func(status CheckStatus) string {
			switch status {
			case OK:
				return IconOK
			case WARN:
				return IconWarn
			case FAIL:
				return IconFail
			case SKIP:
				return IconSkip
			default:
				return ""
			}
		},
	}

	// Parse the template file
	tmpl, err := template.New("report_template.html").Funcs(funcMap).ParseFiles("web/templates/report_template.html")
	if err != nil {
		return "", fmt.Errorf("could not parse html template: %w", err)
	}

	// Create the output file with a dynamic name
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown" // Fallback hostname
	}
	hostname = strings.Split(hostname, ".")[0] // Use short hostname
	timestamp := time.Now().Format("20060102_150405")
	reportDir := "reports"

	if err := os.MkdirAll(reportDir, 0755); err != nil {
		return "", fmt.Errorf("could not create reports directory: %w", err)
	}

	reportPath := filepath.Join(reportDir, fmt.Sprintf("analysis_report_%s_%s.html", hostname, timestamp))

	file, err := os.Create(reportPath)
	if err != nil {
		return "", fmt.Errorf("could not create html report file: %w", err)
	}
	defer file.Close()

	// Execute the template with the data and write to the file
	err = tmpl.Execute(file, data)
	if err != nil {
		return "", fmt.Errorf("could not execute html template: %w", err)
	}

	return reportPath, nil
}

func createSafeReportPath(userInputPath string, safeSubDir string) (string, error) {
	if userInputPath == "" {
		return "", errors.New("output path cannot be empty")
	}

	// Sanitize the filename by stripping any directory components
	cleanFilename := filepath.Base(userInputPath)
	if cleanFilename == "." || cleanFilename == "/" || cleanFilename == ".." {
		return "", fmt.Errorf("invalid filename provided: %s", userInputPath)
	}

	// Ensure the safe subdirectory exists
	if err := os.MkdirAll(safeSubDir, 0755); err != nil {
		return "", fmt.Errorf("could not create reports directory '%s': %w", safeSubDir, err)
	}

	// Join the safe directory and the clean filename
	safePath := filepath.Join(safeSubDir, cleanFilename)
	return safePath, nil
}

func writeJSON(path string, r *Report) (string, error) {
	if path == "" {
		return "", nil
	}

	safePath, err := createSafeReportPath(path, "reports")
	if err != nil {
		return "", fmt.Errorf("invalid output path: %w", err)
	}

	f, err := os.Create(safePath)
	if err != nil {
		return safePath, err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return safePath, enc.Encode(r)
}
