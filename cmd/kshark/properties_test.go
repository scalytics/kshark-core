package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestLoadProperties(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    map[string]string
	}{
		{
			name:    "normal key=value pairs",
			content: "bootstrap.servers=localhost:9092\nsasl.mechanism=PLAIN\n",
			want:    map[string]string{"bootstrap.servers": "localhost:9092", "sasl.mechanism": "PLAIN"},
		},
		{
			name:    "hash comment lines ignored",
			content: "# this is a comment\nkey1=val1\n",
			want:    map[string]string{"key1": "val1"},
		},
		{
			name:    "double-slash comment lines ignored",
			content: "// this is a comment\nkey1=val1\n",
			want:    map[string]string{"key1": "val1"},
		},
		{
			name:    "empty lines ignored",
			content: "\n\nkey1=val1\n\nkey2=val2\n\n",
			want:    map[string]string{"key1": "val1", "key2": "val2"},
		},
		{
			name:    "colon separator when no equals",
			content: "key1: val1\nkey2: val2\n",
			want:    map[string]string{"key1": "val1", "key2": "val2"},
		},
		{
			name:    "equals takes precedence over colon",
			content: "key1=val1:extra\n",
			want:    map[string]string{"key1": "val1:extra"},
		},
		{
			name:    "whitespace trimming",
			content: "  key1  =  val1  \n  key2  =  val2  \n",
			want:    map[string]string{"key1": "val1", "key2": "val2"},
		},
		{
			name:    "empty file",
			content: "",
			want:    map[string]string{},
		},
		{
			name:    "lines without separator are skipped",
			content: "nodelimiter\nkey1=val1\n",
			want:    map[string]string{"key1": "val1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "test.properties")
			if err := os.WriteFile(path, []byte(tt.content), 0644); err != nil {
				t.Fatalf("failed to write test file: %v", err)
			}

			got, err := loadProperties(path)
			if err != nil {
				t.Fatalf("loadProperties() error = %v", err)
			}

			if len(got) != len(tt.want) {
				t.Fatalf("loadProperties() returned %d entries, want %d\ngot:  %v\nwant: %v", len(got), len(tt.want), got, tt.want)
			}
			for k, wantV := range tt.want {
				if gotV, ok := got[k]; !ok {
					t.Errorf("missing key %q", k)
				} else if gotV != wantV {
					t.Errorf("key %q = %q, want %q", k, gotV, wantV)
				}
			}
		})
	}
}

func TestLoadProperties_FileNotFound(t *testing.T) {
	_, err := loadProperties("/nonexistent/path/file.properties")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestApplyPreset(t *testing.T) {
	tests := []struct {
		name   string
		preset string
		input  map[string]string
		want   map[string]string
	}{
		{
			name:   "cc-plain sets SASL_SSL and PLAIN",
			preset: "cc-plain",
			input:  map[string]string{},
			want:   map[string]string{"security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN"},
		},
		{
			name:   "self-scram sets SASL_SSL and SCRAM-SHA-512",
			preset: "self-scram",
			input:  map[string]string{},
			want:   map[string]string{"security.protocol": "SASL_SSL", "sasl.mechanism": "SCRAM-SHA-512"},
		},
		{
			name:   "unknown preset does nothing",
			preset: "unknown-preset",
			input:  map[string]string{"key": "val"},
			want:   map[string]string{"key": "val"},
		},
		{
			name:   "preset does not override existing values",
			preset: "cc-plain",
			input:  map[string]string{"security.protocol": "SSL", "sasl.mechanism": "SCRAM-SHA-256"},
			want:   map[string]string{"security.protocol": "SSL", "sasl.mechanism": "SCRAM-SHA-256"},
		},
		{
			name:   "empty preset does nothing",
			preset: "",
			input:  map[string]string{},
			want:   map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			applyPreset(tt.preset, tt.input)
			if len(tt.input) != len(tt.want) {
				t.Fatalf("applyPreset() result has %d entries, want %d\ngot:  %v\nwant: %v", len(tt.input), len(tt.want), tt.input, tt.want)
			}
			for k, wantV := range tt.want {
				if gotV := tt.input[k]; gotV != wantV {
					t.Errorf("key %q = %q, want %q", k, gotV, wantV)
				}
			}
		})
	}
}

func TestRedactProps(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]string
		check func(t *testing.T, out map[string]string)
	}{
		{
			name: "password fields redacted",
			input: map[string]string{
				"sasl.password":      "secret123",
				"bootstrap.servers":  "localhost:9092",
			},
			check: func(t *testing.T, out map[string]string) {
				if out["sasl.password"] != "[REDACTED]" {
					t.Errorf("sasl.password = %q, want [REDACTED]", out["sasl.password"])
				}
				if out["bootstrap.servers"] != "localhost:9092" {
					t.Errorf("bootstrap.servers = %q, want localhost:9092", out["bootstrap.servers"])
				}
			},
		},
		{
			name: "secret fields redacted",
			input: map[string]string{
				"ssl.key.secret": "mysecret",
			},
			check: func(t *testing.T, out map[string]string) {
				if out["ssl.key.secret"] != "[REDACTED]" {
					t.Errorf("ssl.key.secret = %q, want [REDACTED]", out["ssl.key.secret"])
				}
			},
		},
		{
			name: "key fields redacted",
			input: map[string]string{
				"ssl.key.password": "keypass",
			},
			check: func(t *testing.T, out map[string]string) {
				if out["ssl.key.password"] != "[REDACTED]" {
					t.Errorf("ssl.key.password = %q, want [REDACTED]", out["ssl.key.password"])
				}
			},
		},
		{
			name: "token fields redacted",
			input: map[string]string{
				"sasl.oauthbearer.token": "tok123",
			},
			check: func(t *testing.T, out map[string]string) {
				if out["sasl.oauthbearer.token"] != "[REDACTED]" {
					t.Errorf("sasl.oauthbearer.token = %q, want [REDACTED]", out["sasl.oauthbearer.token"])
				}
			},
		},
		{
			name: "sasl.jaas.config redacted",
			input: map[string]string{
				"sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username='user' password='pass';",
			},
			check: func(t *testing.T, out map[string]string) {
				if out["sasl.jaas.config"] != "[REDACTED]" {
					t.Errorf("sasl.jaas.config = %q, want [REDACTED]", out["sasl.jaas.config"])
				}
			},
		},
		{
			name: "basic.auth.user.info redacted",
			input: map[string]string{
				"basic.auth.user.info": "apikey:secret",
			},
			check: func(t *testing.T, out map[string]string) {
				if out["basic.auth.user.info"] != "[REDACTED]" {
					t.Errorf("basic.auth.user.info = %q, want [REDACTED]", out["basic.auth.user.info"])
				}
			},
		},
		{
			name: "normal fields preserved",
			input: map[string]string{
				"bootstrap.servers":  "broker1:9092",
				"security.protocol":  "SASL_SSL",
				"sasl.mechanism":     "PLAIN",
			},
			check: func(t *testing.T, out map[string]string) {
				if out["bootstrap.servers"] != "broker1:9092" {
					t.Errorf("bootstrap.servers = %q, want broker1:9092", out["bootstrap.servers"])
				}
				if out["security.protocol"] != "SASL_SSL" {
					t.Errorf("security.protocol = %q, want SASL_SSL", out["security.protocol"])
				}
				if out["sasl.mechanism"] != "PLAIN" {
					t.Errorf("sasl.mechanism = %q, want PLAIN", out["sasl.mechanism"])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := redactProps(tt.input)
			tt.check(t, out)
		})
	}
}

func TestLoadProperties_EnvExpansion(t *testing.T) {
	t.Setenv("KSHARK_TEST_SECRET", "expanded-value")

	dir := t.TempDir()
	path := filepath.Join(dir, "test.properties")
	os.WriteFile(path, []byte("sasl.password=${KSHARK_TEST_SECRET}\nplain.key=no-expansion\n"), 0644)

	props, err := loadProperties(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if props["sasl.password"] != "expanded-value" {
		t.Errorf("expected expanded-value, got %q", props["sasl.password"])
	}
	if props["plain.key"] != "no-expansion" {
		t.Errorf("expected no-expansion, got %q", props["plain.key"])
	}
}

func TestWarnInsecurePermissions_NoError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission test not applicable on Windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "test.properties")
	os.WriteFile(path, []byte("key=value"), 0644)

	// Should not panic; we just verify it runs
	warnInsecurePermissions(path)
}

func TestWarnInsecurePermissions_SecureFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission test not applicable on Windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "secure.properties")
	os.WriteFile(path, []byte("key=value"), 0600)

	// Should not warn for 0600 permissions
	warnInsecurePermissions(path)
}

func TestWarnInsecurePermissions_MissingFile(t *testing.T) {
	// Should not panic on missing file
	warnInsecurePermissions("/nonexistent/path/file.properties")
}
