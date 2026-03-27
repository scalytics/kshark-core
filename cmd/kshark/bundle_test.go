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
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRedactTerraformState_Basic(t *testing.T) {
	state := `{
		"resources": [{
			"type": "confluent_api_key",
			"instances": [{
				"attributes": {
					"id": "ABC123",
					"secret": "super-secret-value",
					"display_name": "my-key"
				}
			}]
		}]
	}`

	redacted, err := redactTerraformState([]byte(state))
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(redacted), "super-secret-value") {
		t.Error("secret value was not redacted")
	}
	if !strings.Contains(string(redacted), "[REDACTED]") {
		t.Error("expected [REDACTED] placeholder")
	}
	if !strings.Contains(string(redacted), "ABC123") {
		t.Error("non-sensitive 'id' should be preserved")
	}
	if !strings.Contains(string(redacted), "my-key") {
		t.Error("non-sensitive 'display_name' should be preserved")
	}
}

func TestRedactTerraformState_DeepNested(t *testing.T) {
	state := `{
		"a": {"b": {"c": {"password": "deep-secret"}}}
	}`
	redacted, err := redactTerraformState([]byte(state))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(redacted), "deep-secret") {
		t.Error("deeply nested password was not redacted")
	}
}

func TestRedactTerraformState_NonStringValues(t *testing.T) {
	state := `{
		"api_key": 12345,
		"secret": true,
		"token": 3.14
	}`
	redacted, err := redactTerraformState([]byte(state))
	if err != nil {
		t.Fatal(err)
	}
	// Non-string scalar values should also be redacted
	if strings.Contains(string(redacted), "12345") {
		t.Error("numeric api_key was not redacted")
	}
}

func TestRedactTerraformState_ConfluentPatterns(t *testing.T) {
	state := `{
		"kafka_api_key": "MYKEY123",
		"kafka_api_secret": "MYSECRET",
		"schema_registry_api_key": "SRKEY",
		"schema_registry_api_secret": "SRSECRET",
		"cloud_api_key": "CLOUDKEY",
		"cloud_api_secret": "CLOUDSECRET"
	}`
	redacted, err := redactTerraformState([]byte(state))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"MYKEY123", "MYSECRET", "SRKEY", "SRSECRET", "CLOUDKEY", "CLOUDSECRET"} {
		if strings.Contains(string(redacted), secret) {
			t.Errorf("Confluent secret %q was not redacted", secret)
		}
	}
}

func TestRedactTerraformState_InvalidJSON(t *testing.T) {
	_, err := redactTerraformState([]byte("not json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestRedactTerraformState_PreservesStructure(t *testing.T) {
	state := `{"resources": [{"type": "aws_instance", "name": "web"}]}`
	redacted, err := redactTerraformState([]byte(state))
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal(redacted, &parsed); err != nil {
		t.Fatalf("redacted output is not valid JSON: %v", err)
	}
	resources, ok := parsed["resources"].([]interface{})
	if !ok || len(resources) != 1 {
		t.Error("structure not preserved")
	}
}

func TestIsSensitiveKey(t *testing.T) {
	tests := []struct {
		key  string
		want bool
	}{
		{"password", true},
		{"PASSWORD", true},
		{"db_password", true},
		{"api_key", true},
		{"api_secret", true},
		{"private_key", true},
		{"connection_string", true},
		{"kafka_api_secret", true},
		{"hostname", false},
		{"port", false},
		{"database_name", false},
		{"region", false},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			got := isSensitiveKey(tt.key)
			if got != tt.want {
				t.Errorf("isSensitiveKey(%q) = %v, want %v", tt.key, got, tt.want)
			}
		})
	}
}

func TestDetectEnvironment(t *testing.T) {
	// In test environment, should return "vm" (not docker or k8s)
	env := detectEnvironment()
	if env != "vm" && env != "docker" && env != "kubernetes" {
		t.Errorf("unexpected environment: %s", env)
	}
}

func TestCaptureSystemContext(t *testing.T) {
	ctx := captureSystemContext()
	if ctx["os"] == "" {
		t.Error("os should not be empty")
	}
	if ctx["arch"] == "" {
		t.Error("arch should not be empty")
	}
	if ctx["go"] == "" {
		t.Error("go version should not be empty")
	}
}

func TestRedactPlanText(t *testing.T) {
	plan := `
+ resource "confluent_api_key" "app" {
    id          = (known after apply)
    secret      = "my-api-secret"
    password    = "hunter2"
    region      = "us-east-1"
  }
`
	redacted := redactPlanText(plan)
	if strings.Contains(redacted, "my-api-secret") {
		t.Error("secret not redacted in plan text")
	}
	if strings.Contains(redacted, "hunter2") {
		t.Error("password not redacted in plan text")
	}
	if !strings.Contains(redacted, "us-east-1") {
		t.Error("non-sensitive region should be preserved")
	}
}

func TestCreateBundle(t *testing.T) {
	// Create a temp dir for the test
	tmpDir := t.TempDir()

	report := &Report{
		StartedAt:  time.Now(),
		FinishedAt: time.Now(),
		Rows: []Row{
			{"kafka", "test:9092", L4, OK, "Connected", ""},
		},
		ConfigEcho: map[string]string{
			"bootstrap.servers": "test:9092",
			"sasl.password":     "[REDACTED]",
		},
	}

	bundlePath := filepath.Join(tmpDir, "test-bundle.tar.gz")
	result, err := createBundle(report, "", "", "", "", bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	if result != bundlePath {
		t.Errorf("expected %s, got %s", bundlePath, result)
	}

	// Verify the archive exists and is valid
	f, err := os.Open(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	gzr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	var files []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, filepath.Base(hdr.Name))
	}

	// Check that key files are present
	hasReport := false
	hasManifest := false
	hasConfig := false
	for _, f := range files {
		switch f {
		case "report.json":
			hasReport = true
		case "MANIFEST.md":
			hasManifest = true
		case "client.properties.redacted":
			hasConfig = true
		}
	}
	if !hasReport {
		t.Error("bundle missing report.json")
	}
	if !hasManifest {
		t.Error("bundle missing MANIFEST.md")
	}
	if !hasConfig {
		t.Error("bundle missing client.properties.redacted")
	}
}

func TestCreateBundle_WithTerraformState(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a mock terraform state file
	tfState := `{"resources": [{"type": "confluent_api_key", "instances": [{"attributes": {"secret": "LEAKED"}}]}]}`
	tfPath := filepath.Join(tmpDir, "terraform.tfstate")
	if err := os.WriteFile(tfPath, []byte(tfState), 0644); err != nil {
		t.Fatal(err)
	}

	report := &Report{StartedAt: time.Now(), FinishedAt: time.Now()}
	bundlePath := filepath.Join(tmpDir, "test-bundle.tar.gz")

	_, err := createBundle(report, "", "", tfPath, "", bundlePath)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the terraform state is included and redacted
	f, _ := os.Open(bundlePath)
	defer f.Close()
	gzr, _ := gzip.NewReader(f)
	defer gzr.Close()
	tr := tar.NewReader(gzr)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(hdr.Name, "terraform.tfstate.redacted") {
			data, _ := io.ReadAll(tr)
			if strings.Contains(string(data), "LEAKED") {
				t.Error("terraform state contains unredacted secret")
			}
			if !strings.Contains(string(data), "[REDACTED]") {
				t.Error("terraform state should contain [REDACTED]")
			}
			return
		}
	}
	t.Error("terraform state file not found in bundle")
}

func TestCreateBundle_PathTraversal(t *testing.T) {
	report := &Report{StartedAt: time.Now(), FinishedAt: time.Now()}
	_, err := createBundle(report, "", "", "../../../etc/passwd", "", "")
	if err == nil {
		t.Error("expected error for path traversal in tf-state")
	}
}

func TestPrintExportCommands_NoPanic(t *testing.T) {
	// printExportCommands writes to stdout — just verify it doesn't panic
	dir := t.TempDir()
	bundlePath := filepath.Join(dir, "test-bundle.tar.gz")
	os.WriteFile(bundlePath, []byte("test"), 0644)

	// Capture stdout would require os.Pipe, but for coverage a no-panic test suffices
	printExportCommands(bundlePath)
}

func TestPrintExportCommands_RelativePath(t *testing.T) {
	printExportCommands("my-bundle.tar.gz")
}

func TestDetectEnvironment_Default(t *testing.T) {
	env := detectEnvironment()
	// On a standard dev machine, should be "vm" or "unknown"
	if env != "vm" && env != "unknown" && env != "docker" && env != "kubernetes" {
		t.Errorf("unexpected environment: %s", env)
	}
}
