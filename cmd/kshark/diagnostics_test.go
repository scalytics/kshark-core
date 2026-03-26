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
