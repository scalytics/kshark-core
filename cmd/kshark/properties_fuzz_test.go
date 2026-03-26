package main

import (
	"os"
	"path/filepath"
	"testing"
)

func FuzzLoadProperties(f *testing.F) {
	f.Add("key=value\n# comment\nk2: v2\n")
	f.Add("")
	f.Add("===\n:::\n")
	f.Add("bootstrap.servers=broker:9092\nsecurity.protocol=SASL_SSL\n")
	f.Add("key with spaces = value with spaces\n")
	f.Add("no-separator-line\n")
	f.Add("# only comments\n# another\n")
	f.Fuzz(func(t *testing.T, input string) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.properties")
		os.WriteFile(path, []byte(input), 0644)
		loadProperties(path) // must not panic
	})
}
