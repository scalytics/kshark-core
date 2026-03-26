package main

import (
	"testing"
)

func TestRunDoctor_DoesNotPanic(t *testing.T) {
	// runDoctor prints to stdout and inspects PATH for tools.
	// We just verify it completes without panicking.
	runDoctor()
}

func TestDiagnosticToolsList(t *testing.T) {
	if len(diagnosticTools) == 0 {
		t.Fatal("diagnosticTools slice is empty")
	}

	// Verify that ping is in the list and is required
	foundPing := false
	for _, tool := range diagnosticTools {
		if tool.Name == "ping" {
			foundPing = true
			if !tool.Required {
				t.Error("ping should be marked as required")
			}
			if tool.Purpose == "" {
				t.Error("ping should have a non-empty Purpose")
			}
		}
	}
	if !foundPing {
		t.Error("diagnosticTools does not contain 'ping'")
	}

	// Verify expected tools are present
	expectedTools := []string{"traceroute", "ping", "dig", "openssl", "curl", "nslookup"}
	toolNames := map[string]bool{}
	for _, tool := range diagnosticTools {
		toolNames[tool.Name] = true
	}

	for _, name := range expectedTools {
		if !toolNames[name] {
			t.Errorf("expected tool %q not found in diagnosticTools", name)
		}
	}

	// Verify all entries have non-empty Name and Purpose
	for i, tool := range diagnosticTools {
		if tool.Name == "" {
			t.Errorf("diagnosticTools[%d].Name is empty", i)
		}
		if tool.Purpose == "" {
			t.Errorf("diagnosticTools[%d].Purpose is empty (tool: %s)", i, tool.Name)
		}
	}
}

func TestDiagnosticToolsList_HasRequiredFlag(t *testing.T) {
	// At least one tool should be required, and most should be optional
	requiredCount := 0
	optionalCount := 0
	for _, tool := range diagnosticTools {
		if tool.Required {
			requiredCount++
		} else {
			optionalCount++
		}
	}

	if requiredCount == 0 {
		t.Error("expected at least one required tool in diagnosticTools")
	}
	if optionalCount == 0 {
		t.Error("expected at least one optional tool in diagnosticTools")
	}
}
