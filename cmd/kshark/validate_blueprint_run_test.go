package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateBlueprint_InputErrors(t *testing.T) {
	routineBody := "routine_id: r\nroutine_version: 0.1.0\nblueprint: bp\nchecks:\n  - id: c1\n    kind: connectivity\n"
	tests := []struct {
		name   string
		files  map[string]string
		args   []string
		code   int
		stderr string
	}{
		{"routine missing", map[string]string{"p.properties": "bootstrap.servers=127.0.0.1:1\n"}, []string{"--routine", "nope.yaml", "-props", "p.properties"}, 1, "Error loading routine"},
		{"props missing", map[string]string{"r.yaml": routineBody}, []string{"--routine", "r.yaml", "-props", "nope.properties"}, 1, "Failed to load properties"},
		{"no bootstrap", map[string]string{"r.yaml": routineBody, "p.properties": "security.protocol=PLAINTEXT\n"}, []string{"--routine", "r.yaml", "-props", "p.properties"}, 1, "bootstrap.servers missing in properties"},
		{"bad digests", map[string]string{"r.yaml": routineBody, "p.properties": "bootstrap.servers=127.0.0.1:1\n", "d.json": "{"}, []string{"--routine", "r.yaml", "-props", "p.properties", "--digests", "d.json"}, 1, "Error loading digests"},
		{"dead broker is red", map[string]string{"r.yaml": routineBody, "p.properties": "bootstrap.servers=127.0.0.1:1\n"}, []string{"--routine", "r.yaml", "-props", "p.properties", "--kafka-timeout", "1s", "--log", "bvb.log", "--log-format", "json"}, 1, "HTML render skipped"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Chdir(dir)
			for name, content := range tt.files {
				writeTestFile(t, filepath.Join(dir, name), content)
			}
			res := runMain(t, "", append([]string{"validate-blueprint"}, tt.args...)...)
			if !res.exited || res.code != tt.code {
				t.Errorf("exit = %v/%d, want %d\nstdout: %s\nstderr: %s", res.exited, res.code, tt.code, res.stdout, res.stderr)
			}
			if !strings.Contains(res.stderr, tt.stderr) {
				t.Errorf("stderr missing %q: %s", tt.stderr, res.stderr)
			}
		})
	}
}
