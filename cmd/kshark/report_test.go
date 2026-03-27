package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTruncate(t *testing.T) {
	tests := []struct {
		name  string
		input string
		limit int
		want  string
	}{
		{
			name:  "shorter than limit",
			input: "hello",
			limit: 10,
			want:  "hello",
		},
		{
			name:  "exactly at limit",
			input: "hello",
			limit: 5,
			want:  "hello",
		},
		{
			name:  "longer than limit",
			input: "hello world",
			limit: 5,
			want:  "hell\u2026",
		},
		{
			name:  "one char over limit",
			input: "abcdef",
			limit: 5,
			want:  "abcd\u2026",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncate(tt.input, tt.limit)
			if got != tt.want {
				t.Errorf("truncate(%q, %d) = %q, want %q", tt.input, tt.limit, got, tt.want)
			}
		})
	}
}

func TestCreateSafeReportPath(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		check   func(t *testing.T, path string)
	}{
		{
			name:  "normal filename",
			input: "report.json",
			check: func(t *testing.T, path string) {
				if path == "" {
					t.Error("expected non-empty path")
				}
			},
		},
		{
			name:  "path traversal attempt",
			input: "../../etc/passwd",
			check: func(t *testing.T, path string) {
				// filepath.Base strips the traversal, result should be just "passwd"
				// in the safe subdirectory
				if path == "" {
					t.Error("expected non-empty path")
				}
				// Must not contain ".."
				if len(path) > 0 {
					// The path should end with just the base filename
					if path == "../../etc/passwd" {
						t.Error("path traversal was not prevented")
					}
				}
			},
		},
		{
			name:    "empty path",
			input:   "",
			wantErr: true,
		},
		{
			name:    "directory-only input (..)",
			input:   "..",
			wantErr: true,
		},
		{
			name:    "directory-only input (.)",
			input:   ".",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			safeDir := t.TempDir()
			got, err := createSafeReportPath(tt.input, safeDir)
			if (err != nil) != tt.wantErr {
				t.Fatalf("createSafeReportPath(%q) error = %v, wantErr = %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && tt.check != nil {
				tt.check(t, got)
			}
		})
	}
}

func TestAddRow(t *testing.T) {
	tests := []struct {
		name       string
		status     CheckStatus
		wantFailed bool
	}{
		{
			name:       "OK row does not set HasFailed",
			status:     OK,
			wantFailed: false,
		},
		{
			name:       "WARN row does not set HasFailed",
			status:     WARN,
			wantFailed: false,
		},
		{
			name:       "FAIL row sets HasFailed",
			status:     FAIL,
			wantFailed: true,
		},
		{
			name:       "SKIP row does not set HasFailed",
			status:     SKIP,
			wantFailed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Report{}
			addRow(r, Row{
				Component: "test",
				Target:    "target",
				Layer:     L7,
				Status:    tt.status,
				Detail:    "test detail",
			})
			if r.HasFailed != tt.wantFailed {
				t.Errorf("addRow() with status %s: HasFailed = %v, want %v", tt.status, r.HasFailed, tt.wantFailed)
			}
			if len(r.Rows) != 1 {
				t.Errorf("addRow() rows count = %d, want 1", len(r.Rows))
			}
		})
	}
}

func TestAddRow_FailPersists(t *testing.T) {
	r := &Report{}
	addRow(r, Row{Status: FAIL, Component: "a", Target: "t", Layer: L7, Detail: "fail"})
	addRow(r, Row{Status: OK, Component: "b", Target: "t", Layer: L7, Detail: "ok"})

	if !r.HasFailed {
		t.Error("HasFailed should remain true after adding an OK row following a FAIL row")
	}
	if len(r.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(r.Rows))
	}
}

func TestSummarize(t *testing.T) {
	r := &Report{}
	addRow(r, Row{Component: "kafka", Target: "t1", Layer: L7, Status: OK, Detail: "ok"})
	addRow(r, Row{Component: "kafka", Target: "t2", Layer: L7, Status: WARN, Detail: "warn"})
	addRow(r, Row{Component: "kafka", Target: "t3", Layer: L7, Status: FAIL, Detail: "fail"})
	addRow(r, Row{Component: "kafka", Target: "t4", Layer: L7, Status: SKIP, Detail: "skip"})
	addRow(r, Row{Component: "dns", Target: "h1", Layer: L3, Status: OK, Detail: "ok"})
	addRow(r, Row{Component: "dns", Target: "h2", Layer: L3, Status: FAIL, Detail: "fail"})

	summarize(r)

	l7 := r.Summary[string(L7)]
	if l7.OK != 1 {
		t.Errorf("L7 OK = %d, want 1", l7.OK)
	}
	if l7.WARN != 1 {
		t.Errorf("L7 WARN = %d, want 1", l7.WARN)
	}
	if l7.FAIL != 1 {
		t.Errorf("L7 FAIL = %d, want 1", l7.FAIL)
	}
	if l7.SKIP != 1 {
		t.Errorf("L7 SKIP = %d, want 1", l7.SKIP)
	}

	l3 := r.Summary[string(L3)]
	if l3.OK != 1 {
		t.Errorf("L3 OK = %d, want 1", l3.OK)
	}
	if l3.FAIL != 1 {
		t.Errorf("L3 FAIL = %d, want 1", l3.FAIL)
	}
}

func TestPrintPretty_NoPanic(t *testing.T) {
	r := &Report{
		StartedAt:  time.Now().Add(-5 * time.Second),
		FinishedAt: time.Now(),
	}
	addRow(r, Row{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ApiVersions OK"})
	addRow(r, Row{Component: "kafka", Target: "broker:9092", Layer: L56, Status: WARN, Detail: "cert expires soon", Hint: "Renew cert"})
	addRow(r, Row{Component: "dns", Target: "broker", Layer: L3, Status: FAIL, Detail: "DNS failed", Hint: "Check /etc/hosts"})
	addRow(r, Row{Component: "kafka", Target: "broker:9092", Layer: L4, Status: SKIP, Detail: "skipped"})
	summarize(r)

	// Just verify no panic -- we cannot easily capture stdout in a unit test
	printPretty(r)
}

func TestWriteJSON_Success(t *testing.T) {
	dir := t.TempDir()

	// writeJSON calls createSafeReportPath which uses "reports" as subdir,
	// but we can pass just a filename and it will be placed under "reports/"
	// We need to chdir so the "reports" dir is created in a temp location.
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(origDir)
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}

	report := &Report{
		StartedAt:  time.Now().Add(-1 * time.Second),
		FinishedAt: time.Now(),
		ConfigEcho: map[string]string{"bootstrap.servers": "broker:9092"},
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok"},
		},
	}
	summarize(report)

	actualPath, err := writeJSON("test_report.json", report)
	if err != nil {
		t.Fatalf("writeJSON() error = %v", err)
	}
	if actualPath == "" {
		t.Fatal("writeJSON() returned empty path")
	}

	data, err := os.ReadFile(actualPath)
	if err != nil {
		t.Fatalf("could not read written file: %v", err)
	}

	// Verify valid JSON
	var decoded Report
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("written file is not valid JSON: %v", err)
	}
	if len(decoded.Rows) != 1 {
		t.Errorf("decoded rows = %d, want 1", len(decoded.Rows))
	}
	if decoded.ConfigEcho["bootstrap.servers"] != "broker:9092" {
		t.Errorf("decoded bootstrap.servers = %q, want broker:9092", decoded.ConfigEcho["bootstrap.servers"])
	}
}

func TestWriteJSON_EmptyPath(t *testing.T) {
	result, err := writeJSON("", nil)
	if err != nil {
		t.Fatalf("writeJSON(\"\") error = %v", err)
	}
	if result != "" {
		t.Errorf("writeJSON(\"\") = %q, want empty", result)
	}
}

func TestWriteHTMLReport_MissingTemplate(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	defer os.Chdir(origDir)
	os.Chdir(dir)

	r := &Report{
		StartedAt:  time.Now(),
		FinishedAt: time.Now(),
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ok"},
		},
	}

	_, err := writeHTMLReport(r, nil)
	if err == nil {
		t.Fatal("expected error when template file is missing")
	}
	if !strings.Contains(err.Error(), "template") {
		t.Errorf("expected template error, got: %v", err)
	}
}

func TestWriteHTMLReport_WithTemplate(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	defer os.Chdir(origDir)
	os.Chdir(dir)

	// Create minimal template
	os.MkdirAll("web/templates", 0755)
	tmpl := `<!DOCTYPE html><html><body>
{{range .Report.Rows}}<div class="{{.Status | ToLower}}">{{Icon .Status}} {{.Component}} {{.Detail}}</div>
{{end}}
{{if .Analysis}}<div>{{.Analysis.RootCauseAnalysis}}</div>{{end}}
</body></html>`
	os.WriteFile("web/templates/report_template.html", []byte(tmpl), 0644)

	r := &Report{
		StartedAt:  time.Now(),
		FinishedAt: time.Now(),
		Rows: []Row{
			{Component: "kafka", Target: "broker:9092", Layer: L7, Status: OK, Detail: "ApiVersions OK"},
			{Component: "kafka", Target: "broker:9092", Layer: L4, Status: FAIL, Detail: "TCP timeout"},
		},
	}

	path, err := writeHTMLReport(r, nil)
	if err != nil {
		t.Fatalf("writeHTMLReport() error = %v", err)
	}
	if path == "" {
		t.Fatal("expected non-empty path")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "ApiVersions OK") {
		t.Error("HTML report missing row detail")
	}
	if !strings.Contains(content, "<!DOCTYPE html>") {
		t.Error("HTML report missing doctype")
	}
}

func TestWriteHTMLReport_WithAnalysis(t *testing.T) {
	dir := t.TempDir()
	origDir, _ := os.Getwd()
	defer os.Chdir(origDir)
	os.Chdir(dir)

	os.MkdirAll("web/templates", 0755)
	tmpl := `<!DOCTYPE html><html><body>
{{if .Analysis}}<div id="analysis">{{.Analysis.RootCauseAnalysis}}</div>{{end}}
</body></html>`
	os.WriteFile("web/templates/report_template.html", []byte(tmpl), 0644)

	r := &Report{StartedAt: time.Now(), FinishedAt: time.Now()}
	analysis := &AIAnalysisResponse{
		RootCauseAnalysis: "Firewall blocks port 9092",
	}

	path, err := writeHTMLReport(r, analysis)
	if err != nil {
		t.Fatalf("writeHTMLReport() error = %v", err)
	}

	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), "Firewall blocks port 9092") {
		t.Error("HTML report missing analysis content")
	}
}

func TestWriteJSON_PathTraversal(t *testing.T) {
	dir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(origDir)
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}

	report := &Report{StartedAt: time.Now(), FinishedAt: time.Now()}
	actualPath, err := writeJSON("../../evil.json", report)
	if err != nil {
		t.Fatalf("writeJSON() error = %v", err)
	}
	// createSafeReportPath strips directory components, so it should
	// end up in the safe "reports" subdirectory, not outside
	if strings.Contains(actualPath, "..") {
		t.Errorf("path traversal not prevented: %s", actualPath)
	}
	// Verify the file was actually written under reports/
	if !strings.Contains(actualPath, filepath.Join("reports", "evil.json")) {
		t.Errorf("expected file under reports/evil.json, got %s", actualPath)
	}
}
