package main

import (
	"strings"
	"testing"
)

func TestIsValidHostname(t *testing.T) {
	tests := []struct {
		name  string
		host  string
		valid bool
	}{
		{name: "simple hostname", host: "myhost", valid: true},
		{name: "hostname with dots", host: "broker.example.com", valid: true},
		{name: "hostname with hyphens", host: "my-broker.example.com", valid: true},
		{name: "hostname with numbers", host: "broker1.example.com", valid: true},
		{name: "IP address", host: "192.168.1.1", valid: true},
		{name: "semicolon injection", host: ";rm -rf /", valid: false},
		{name: "backtick injection", host: "`whoami`", valid: false},
		{name: "pipe injection", host: "host|cat /etc/passwd", valid: false},
		{name: "dollar injection", host: "host$(id)", valid: false},
		{name: "ampersand injection", host: "host&rm -rf /", valid: false},
		{name: "space in hostname", host: "host name", valid: false},
		{name: "empty string", host: "", valid: false},
		{name: "single letter", host: "a", valid: true},
		{name: "newline injection", host: "host\nwhoami", valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidHostname(tt.host)
			if got != tt.valid {
				t.Errorf("isValidHostname(%q) = %v, want %v", tt.host, got, tt.valid)
			}
		})
	}
}

// ---------- bestEffortTraceroute tests ----------

func TestBestEffortTraceroute_ValidHost(t *testing.T) {
	r := &Report{}
	bestEffortTraceroute(r, "127.0.0.1")
	if len(r.Rows) == 0 {
		t.Error("expected at least 1 row from traceroute")
	}
	// Should be OK (traceroute found) or SKIP (no tool available)
	for _, row := range r.Rows {
		if row.Status == FAIL {
			t.Errorf("unexpected FAIL for valid host: %s", row.Detail)
		}
	}
}

func TestBestEffortTraceroute_InvalidHost(t *testing.T) {
	r := &Report{}
	bestEffortTraceroute(r, ";rm -rf /")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0].Status != FAIL {
		t.Errorf("status = %s, want FAIL for injection attempt", r.Rows[0].Status)
	}
	if !strings.Contains(r.Rows[0].Detail, "Invalid hostname") {
		t.Errorf("detail = %q, expected mention of invalid hostname", r.Rows[0].Detail)
	}
}

// ---------- mtuCheck tests ----------

func TestMtuCheck_ValidHost(t *testing.T) {
	r := &Report{}
	mtuCheck(r, "127.0.0.1")
	if len(r.Rows) == 0 {
		t.Error("expected at least 1 row from MTU check")
	}
}

func TestMtuCheck_InvalidHost(t *testing.T) {
	r := &Report{}
	mtuCheck(r, "host`injection`")
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}
	if r.Rows[0].Status != FAIL {
		t.Errorf("status = %s, want FAIL for injection attempt", r.Rows[0].Status)
	}
}

// ---------- mtuCorrelation tests ----------

func TestMtuCorrelation_BlackHoleDetected(t *testing.T) {
	r := &Report{
		Rows: []Row{
			{Component: "diag", Target: "host", Layer: DIAG, Status: OK, Detail: "MTU ok at payload 1472"},
			{Component: "kafka", Target: "host:9092", Layer: L7, Status: FAIL, Detail: "Produce failed: timeout"},
		},
	}
	mtuCorrelation(r)

	found := false
	for _, row := range r.Rows {
		if strings.Contains(row.Detail, "PMTU black hole") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected PMTU black hole warning when MTU OK but produce times out")
	}
}

func TestMtuCorrelation_NoBlackHole(t *testing.T) {
	r := &Report{
		Rows: []Row{
			{Component: "diag", Target: "host", Layer: DIAG, Status: OK, Detail: "MTU ok at payload 1472"},
			{Component: "kafka", Target: "host:9092", Layer: L7, Status: OK, Detail: "Produce OK"},
		},
	}
	before := len(r.Rows)
	mtuCorrelation(r)

	if len(r.Rows) != before {
		t.Error("expected no new rows when both MTU and produce are OK")
	}
}

func TestMtuCorrelation_GlobalTimeout(t *testing.T) {
	r := &Report{
		Rows: []Row{
			{Component: "diag", Target: "host", Layer: DIAG, Status: OK, Detail: "MTU ok at payload 1472"},
			{Component: "kshark", Target: "timeout", Layer: DIAG, Status: FAIL, Detail: "Global timeout reached before produce/consume"},
		},
	}
	mtuCorrelation(r)

	found := false
	for _, row := range r.Rows {
		if strings.Contains(row.Detail, "PMTU black hole") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected PMTU black hole warning when MTU OK but global timeout on produce")
	}
}

func TestMtuCorrelation_NoMTUData(t *testing.T) {
	r := &Report{
		Rows: []Row{
			{Component: "kafka", Target: "host:9092", Layer: L7, Status: FAIL, Detail: "Produce failed: timeout"},
		},
	}
	before := len(r.Rows)
	mtuCorrelation(r)

	if len(r.Rows) != before {
		t.Error("expected no new rows when there's no MTU data")
	}
}

// ---------- runCmdIfExists tests ----------

func TestRunCmdIfExists_ValidCommand(t *testing.T) {
	out, err := runCmdIfExists("echo", "hello")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "hello") {
		t.Errorf("output = %q, expected to contain 'hello'", out)
	}
}

func TestRunCmdIfExists_NonExistentCommand(t *testing.T) {
	_, err := runCmdIfExists("this_command_does_not_exist_kshark_test")
	if err == nil {
		t.Error("expected error for non-existent command")
	}
}

func TestTrimLines(t *testing.T) {
	tests := []struct {
		name  string
		input string
		limit int
		check func(t *testing.T, result string)
	}{
		{
			name:  "fewer lines than limit",
			input: "line1\nline2\nline3",
			limit: 10,
			check: func(t *testing.T, result string) {
				if result != "line1\nline2\nline3" {
					t.Errorf("expected original string, got %q", result)
				}
			},
		},
		{
			name:  "exactly at limit",
			input: "line1\nline2\nline3",
			limit: 3,
			check: func(t *testing.T, result string) {
				if result != "line1\nline2\nline3" {
					t.Errorf("expected original string, got %q", result)
				}
			},
		},
		{
			name:  "more lines than limit",
			input: "line1\nline2\nline3\nline4\nline5",
			limit: 2,
			check: func(t *testing.T, result string) {
				if !strings.HasPrefix(result, "line1\nline2\n") {
					t.Errorf("expected first 2 lines, got %q", result)
				}
				if !strings.HasSuffix(result, "\u2026") {
					t.Errorf("expected ellipsis suffix, got %q", result)
				}
				// Should not contain line3
				if strings.Contains(result, "line3") {
					t.Errorf("result should not contain line3, got %q", result)
				}
			},
		},
		{
			name:  "single line under limit",
			input: "single",
			limit: 5,
			check: func(t *testing.T, result string) {
				if result != "single" {
					t.Errorf("expected 'single', got %q", result)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := trimLines(tt.input, tt.limit)
			tt.check(t, result)
		})
	}
}
