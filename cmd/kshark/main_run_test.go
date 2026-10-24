package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type exitPanic int

type mainResult struct {
	code   int
	exited bool
	stdout string
	stderr string
}

func capturePipe(t *testing.T) (*os.File, func() string) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan string)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		r.Close()
		done <- buf.String()
	}()
	return w, func() string {
		w.Close()
		return <-done
	}
}

func runMain(t *testing.T, stdin string, args ...string) mainResult {
	t.Helper()
	oldArgs, oldFlags, oldExit := os.Args, flag.CommandLine, exit
	oldStdin, oldStdout, oldStderr := os.Stdin, os.Stdout, os.Stderr
	oldLogger := slog.Default()

	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = inW.Write([]byte(stdin))
		inW.Close()
	}()
	outW, readOut := capturePipe(t)
	errW, readErr := capturePipe(t)

	os.Args = append([]string{"kshark"}, args...)
	flag.CommandLine = flag.NewFlagSet("kshark", flag.ContinueOnError)
	exit = func(code int) { panic(exitPanic(code)) }
	os.Stdin, os.Stdout, os.Stderr = inR, outW, errW

	var res mainResult
	func() {
		defer func() {
			if p := recover(); p != nil {
				code, ok := p.(exitPanic)
				if !ok {
					panic(p)
				}
				res.code, res.exited = int(code), true
			}
		}()
		main()
	}()

	os.Args, flag.CommandLine, exit = oldArgs, oldFlags, oldExit
	os.Stdin, os.Stdout, os.Stderr = oldStdin, oldStdout, oldStderr
	slog.SetDefault(oldLogger)
	inR.Close()
	res.stdout = readOut()
	res.stderr = readErr()
	return res
}

func writeTestFile(t *testing.T, path, content string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func copyReportTemplate(t *testing.T, dir string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "web", "templates", "report_template.html"))
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(dir, "web", "templates", "report_template.html"), string(data))
}

func deadBrokerProps(t *testing.T, dir string) string {
	return writeTestFile(t, filepath.Join(dir, "client.properties"), "bootstrap.servers=127.0.0.1:1\nsecurity.protocol=PLAINTEXT\n")
}

func readReport(t *testing.T, path string) *Report {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	r := &Report{}
	if err := json.Unmarshal(data, r); err != nil {
		t.Fatalf("parse report: %v", err)
	}
	return r
}

func TestMain_UsageAndValidation(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		files    map[string]string
		code     int
		inStdout string
		inStderr string
	}{
		{"version", []string{"-version"}, nil, 0, "kshark version " + version, ""},
		{"bad probe direction", []string{"-probe-direction", "sideways"}, nil, 2, "", "Invalid -probe-direction value \"sideways\""},
		{"no target", nil, nil, 2, "", "Usage: kshark -props client.properties"},
		{"missing props file", []string{"-props", "missing.properties", "-y"}, nil, 1, "", "Failed to load properties"},
		{"props without bootstrap", []string{"-props", "empty.properties", "-y"}, map[string]string{"empty.properties": "security.protocol=PLAINTEXT\n"}, 1, "", "bootstrap.servers missing"},
		{"diff needs two reports", []string{"diff", "a.json"}, nil, 2, "", "Usage: kshark diff"},
		{"diff unreadable report", []string{"diff", "a.json", "b.json"}, nil, 1, "", "Error loading a.json"},
		{"trend without reports dir", []string{"trend"}, nil, 1, "", "Error reading reports directory"},
		{"validate-blueprint usage", []string{"validate-blueprint"}, nil, 2, "", "Usage: kshark validate-blueprint"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Chdir(dir)
			for name, content := range tt.files {
				writeTestFile(t, filepath.Join(dir, name), content)
			}
			res := runMain(t, "", tt.args...)
			if res.code != tt.code {
				t.Errorf("exit code = %d, want %d\nstdout: %s\nstderr: %s", res.code, tt.code, res.stdout, res.stderr)
			}
			if !strings.Contains(res.stdout, tt.inStdout) {
				t.Errorf("stdout missing %q: %s", tt.inStdout, res.stdout)
			}
			if !strings.Contains(res.stderr, tt.inStderr) {
				t.Errorf("stderr missing %q: %s", tt.inStderr, res.stderr)
			}
		})
	}
}

func TestMain_PromptDeclined(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	props := deadBrokerProps(t, dir)
	res := runMain(t, "maybe\nno\n", "-props", props, "-preset", "cc-plain")
	if !res.exited || res.code != 0 {
		t.Fatalf("exit = %v/%d, want exit(0)\nstdout: %s", res.exited, res.code, res.stdout)
	}
	if strings.Count(res.stdout, "Continue with the scan? (y/n): ") != 2 {
		t.Errorf("prompt should repeat for an unknown answer: %s", res.stdout)
	}
	if !strings.Contains(res.stdout, "Scan aborted by user.") {
		t.Errorf("stdout = %s", res.stdout)
	}
}

func TestMain_ScanDeadBroker_ExitCodeAndArtifacts(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	props := deadBrokerProps(t, dir)
	res := runMain(t, "y\n",
		"-props", props, "-diag=false", "-kafka-timeout", "2s", "-start-offset", "middle",
		"-json", "reports/out.json", "-log", "logs/scan.log", "-log-format", "json",
		"-bundle", "bundle.tar.gz", "-tf-state", "missing.tfstate")
	if !res.exited || res.code != 2 {
		t.Fatalf("exit = %v/%d, want L4 exit code 2\nstdout: %s\nstderr: %s", res.exited, res.code, res.stdout, res.stderr)
	}
	if !strings.Contains(res.stderr, "Invalid -start-offset value, defaulting to earliest") {
		t.Errorf("stderr = %s", res.stderr)
	}
	r := readReport(t, filepath.Join(dir, "reports", "out.json"))
	foundL4 := false
	for _, row := range r.Rows {
		if row.Layer == L4 && row.Status == FAIL && row.Target == "127.0.0.1:1" {
			foundL4 = true
		}
	}
	if !foundL4 {
		t.Errorf("report rows lack the L4 failure: %+v", r.Rows)
	}
	if r.Analysis == nil || r.Analysis.ResponseStatus != "skipped" || r.Analysis.ResponseError != "AI analysis disabled" {
		t.Errorf("analysis = %+v", r.Analysis)
	}
	if r.Artifacts == nil || r.Artifacts.LogFile != "logs/scan.log" || r.Artifacts.LogSHA256 == "" || r.Artifacts.PromptFile == "" {
		t.Errorf("artifacts = %+v", r.Artifacts)
	}
	if r.Run == nil || !r.Run.Params["json"].Provided || r.Run.Params["timeout"].Provided {
		t.Errorf("run params = %+v", r.Run)
	}
	if sums, _ := filepath.Glob(filepath.Join(dir, "reports", "report_sha256_*.txt")); len(sums) != 1 {
		t.Errorf("checksum files = %v, want 1", sums)
	}
	if _, err := os.Stat(filepath.Join(dir, "logs", "scan.log")); err != nil {
		t.Errorf("missing log: %v", err)
	}
	if !strings.Contains(res.stdout, "JSON report written to") {
		t.Errorf("stdout = %s", res.stdout)
	}
}

func writeAIConfig(t *testing.T, dir, endpoint, key string) {
	t.Helper()
	cfg := fmt.Sprintf(`{"default_provider":"test","providers":{"test":{"api_endpoint":%q,"api_key":%q,"model":"test-model"}}}`, endpoint, key)
	writeTestFile(t, filepath.Join(dir, "ai_config.json"), cfg)
}

func TestMain_AnalyzeFallbacks(t *testing.T) {
	tests := []struct {
		name     string
		config   func(t *testing.T, dir string)
		extra    []string
		status   string
		errorHas string
	}{
		{"no config", func(*testing.T, string) {}, nil, "skipped", "AI config not available"},
		{"unknown provider", func(t *testing.T, dir string) { writeAIConfig(t, dir, "https://ai.example.com", "k") }, []string{"-provider", "other"}, "skipped", "AI provider 'other' not found"},
		{"placeholder key", func(t *testing.T, dir string) { writeAIConfig(t, dir, "https://ai.example.com", "YOUR_API_KEY") }, nil, "skipped", "API key for provider 'test' is not configured"},
		{"endpoint blocked", func(t *testing.T, dir string) { writeAIConfig(t, dir, "http://127.0.0.1:1", "test-key") }, nil, "error", "AI endpoint blocked (SSRF protection)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Chdir(dir)
			tt.config(t, dir)
			props := deadBrokerProps(t, dir)
			args := append([]string{"-props", props, "-y", "-diag=false", "-kafka-timeout", "2s", "-analyze", "-json", "reports/out.json"}, tt.extra...)
			res := runMain(t, "", args...)
			if res.code != 2 {
				t.Errorf("exit code = %d, want 2", res.code)
			}
			r := readReport(t, filepath.Join(dir, "reports", "out.json"))
			if r.Analysis == nil || r.Analysis.ResponseStatus != tt.status || !strings.Contains(r.Analysis.ResponseError, tt.errorHas) {
				t.Errorf("analysis = %+v, want %s containing %q", r.Analysis, tt.status, tt.errorHas)
			}
		})
	}
}

func TestMain_AnalyzeSuccess(t *testing.T) {
	allowLoopback(t)
	content := `{"rootCauseAnalysis":"broker port closed","problemLayer":"L4","likelyCategory":"network","confidence":"high","severity":"high","explanation":"TCP refused","evidence":["connection refused"],"suggestedFixes":["open port 9092"],"disclaimer":"test"}`
	srv := aiServer(t, func(w http.ResponseWriter, req APIRequest) {
		if len(req.Messages) != 2 || !strings.Contains(req.Messages[1].Content, "127.0.0.1:1") {
			t.Errorf("user prompt does not carry the report: %+v", req.Messages)
		}
		_, _ = w.Write([]byte(aiChoice(content)))
	})
	dir := t.TempDir()
	copyReportTemplate(t, dir)
	t.Chdir(dir)
	writeAIConfig(t, dir, srv.URL, "test-key")
	props := deadBrokerProps(t, dir)
	res := runMain(t, "yes\n", "-props", props, "-diag=false", "-kafka-timeout", "2s", "-analyze", "-json", "reports/out.json")
	if res.code != 2 {
		t.Fatalf("exit code = %d, want 2\nstderr: %s", res.code, res.stderr)
	}
	for _, want := range []string{"Submitting report for AI analysis using provider 'test'", "--- AI Analysis ---", "AI analysis report written to"} {
		if !strings.Contains(res.stdout, want) {
			t.Errorf("stdout missing %q", want)
		}
	}
	r := readReport(t, filepath.Join(dir, "reports", "out.json"))
	if r.Analysis == nil || r.Analysis.ResponseStatus != "ok" || r.Analysis.Provider != "test" || r.Analysis.Model != "test-model" {
		t.Fatalf("analysis = %+v", r.Analysis)
	}
	if r.Analysis.ResponseSHA256 == "" || !strings.Contains(r.Analysis.ResponseContent, "broker port closed") {
		t.Errorf("response artifact = %+v", r.Analysis)
	}
	htmls, _ := filepath.Glob(filepath.Join(dir, "reports", "*.html"))
	if len(htmls) != 1 {
		t.Errorf("html reports = %v, want 1", htmls)
	}
}

func TestMain_ConnectorConfigOnly(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("KSHARK_CONNECT_AUTH", "u:p")
	t.Setenv("KSHARK_CONNECT_TOKEN", "t")
	cfg := writeTestFile(t, filepath.Join(dir, "connector.json"), `{"connector.class":"io.confluent.connect.jdbc.JdbcSinkConnector","connection.url":"jdbc:postgresql://db.invalid:5432/app"}`)
	res := runMain(t, "", "-connector-config", cfg, "-y", "-diag=false", "-json", "reports/out.json")
	r := readReport(t, filepath.Join(dir, "reports", "out.json"))
	if r.ConfigEcho["connector.connector.class"] != "io.confluent.connect.jdbc.JdbcSinkConnector" {
		t.Errorf("config echo = %v, want the connector config", r.ConfigEcho)
	}
	found := false
	for _, row := range r.Rows {
		if strings.Contains(row.Target, "db.invalid") {
			found = true
		}
	}
	if !found {
		t.Errorf("rows do not probe the connector target: %+v", r.Rows)
	}
	if !res.exited || res.code == 0 {
		t.Errorf("exit = %v/%d, want a failing layered code", res.exited, res.code)
	}
	for _, a := range r.Run.Args {
		if a == "u:p" || a == "t" {
			t.Errorf("args leak credentials: %v", r.Run.Args)
		}
	}
}

func TestMain_DiffAndTrendSubcommands(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	props := deadBrokerProps(t, dir)
	for _, name := range []string{"first.json", "second.json"} {
		res := runMain(t, "", "-props", props, "-y", "-diag=false", "-kafka-timeout", "1s", "-json", "reports/"+name)
		if res.code != 2 {
			t.Fatalf("scan for %s exit = %d", name, res.code)
		}
	}
	res := runMain(t, "", "diff", "reports/first.json", "reports/second.json")
	if res.exited {
		t.Errorf("diff exit = %d\nstderr: %s", res.code, res.stderr)
	}
	if !strings.Contains(res.stdout, "kshark diff: reports/first.json → reports/second.json") || !strings.Contains(res.stdout, "No differences found.") {
		t.Errorf("diff output = %s", res.stdout)
	}
	res = runMain(t, "", "trend", "--last", "1")
	if res.exited {
		t.Errorf("trend exit = %d\nstderr: %s", res.code, res.stderr)
	}
	if !strings.Contains(res.stdout, "kshark trend — last 1 scans") || !strings.Contains(res.stdout, "Trend: 0/1 passed (0%)") {
		t.Errorf("trend --last 1 output = %s", res.stdout)
	}
}

func TestMain_Doctor(t *testing.T) {
	t.Chdir(t.TempDir())
	res := runMain(t, "", "doctor")
	if !strings.Contains(res.stdout, "kshark doctor") {
		t.Errorf("stdout = %s", res.stdout)
	}
}
