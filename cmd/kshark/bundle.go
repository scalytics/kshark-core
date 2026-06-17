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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// ---------- Diagnostics Bundle ----------

const (
	maxTFStateSize  = 200 * 1024 * 1024 // 200 MB hard limit
	warnTFStateSize = 50 * 1024 * 1024  // 50 MB warning threshold
)

// tfRedactPatterns lists key name substrings that indicate sensitive values.
var tfRedactPatterns = []string{
	"password", "secret", "api_key", "api_secret", "access_key",
	"secret_key", "private_key", "private_key_pem", "token",
	"bearer_token", "certificate_body", "sasl.jaas.config",
	"connection_string", "credentials", "auth_token",
	"client_secret", "signing_key", "encryption_key",
	"ssh_key", "tls_key", "bootstrap_token",
	// Confluent Cloud specific
	"kafka_api_key", "kafka_api_secret", "schema_registry_api_key",
	"schema_registry_api_secret", "cloud_api_key", "cloud_api_secret",
	"rest_endpoint_credentials",
}

// redactTerraformState parses a Terraform state JSON and redacts sensitive values.
func redactTerraformState(stateJSON []byte) ([]byte, error) {
	var state interface{}
	if err := json.Unmarshal(stateJSON, &state); err != nil {
		return nil, fmt.Errorf("parse terraform state: %w", err)
	}
	redactJSONTree(state)
	return json.MarshalIndent(state, "", "  ")
}

// redactJSONTree walks a JSON tree and redacts values for keys matching sensitive patterns.
func redactJSONTree(v interface{}) {
	switch val := v.(type) {
	case map[string]interface{}:
		for k, child := range val {
			if isSensitiveKey(k) {
				// Redact any scalar value (string, number, bool) for sensitive keys
				switch child.(type) {
				case map[string]interface{}, []interface{}:
					// Recurse into nested structures even for sensitive keys
					redactJSONTree(child)
				default:
					val[k] = "[REDACTED]"
				}
			} else {
				redactJSONTree(child)
			}
		}
	case []interface{}:
		for _, item := range val {
			redactJSONTree(item)
		}
	}
}

// isSensitiveKey checks if a JSON key name matches any redaction pattern.
func isSensitiveKey(key string) bool {
	lower := strings.ToLower(key)
	for _, pattern := range tfRedactPatterns {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}

// captureSystemContext collects system information for the diagnostics bundle.
func captureSystemContext() map[string]string {
	ctx := map[string]string{
		"os":   runtime.GOOS,
		"arch": runtime.GOARCH,
		"go":   runtime.Version(),
	}

	if hostname, err := os.Hostname(); err == nil {
		ctx["hostname"] = hostname
	}

	// /etc/resolv.conf
	if data, err := os.ReadFile("/etc/resolv.conf"); err == nil {
		ctx["resolv.conf"] = string(data)
	}

	// Network interfaces
	if out, err := runCmdIfExists("ip", "addr"); err == nil {
		ctx["interfaces"] = out
	} else if out, err := runCmdIfExists("ifconfig"); err == nil {
		ctx["interfaces"] = out
	}

	// Routes
	if out, err := runCmdIfExists("ip", "route"); err == nil {
		ctx["routes"] = out
	} else if out, err := runCmdIfExists("netstat", "-rn"); err == nil {
		ctx["routes"] = out
	}

	return ctx
}

// detectEnvironment determines if kshark is running in Docker, Kubernetes, or a VM/bare-metal.
func detectEnvironment() string {
	// Check for Kubernetes
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		return "kubernetes"
	}
	// Check for Docker
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return "docker"
	}
	return "vm"
}

// printExportCommands prints ready-to-use commands to copy the bundle off the current host.
func printExportCommands(bundlePath string) {
	env := detectEnvironment()
	absPath, _ := filepath.Abs(bundlePath)
	basename := filepath.Base(bundlePath)
	hostname, _ := os.Hostname()

	fmt.Printf("\nDiagnostics bundle created: %s\n\n", absPath)
	fmt.Println("Export commands:")

	// SCP (always shown)
	fmt.Printf("  # SCP to local machine:\n")
	fmt.Printf("  scp %s@%s:%s .\n\n", os.Getenv("USER"), hostname, absPath)

	// Docker
	if env == "docker" {
		fmt.Printf("  # Docker copy from container (you are inside a container):\n")
	} else {
		fmt.Printf("  # Docker copy from container:\n")
	}
	fmt.Printf("  docker cp <container>:%s ./%s\n\n", absPath, basename)

	// Kubernetes
	if env == "kubernetes" {
		ns := os.Getenv("POD_NAMESPACE")
		if ns == "" {
			ns = "default"
		}
		fmt.Printf("  # Kubectl copy from pod (you are inside a pod):\n")
		fmt.Printf("  kubectl cp %s/%s:%s ./%s\n\n", ns, hostname, absPath, basename)
	} else {
		fmt.Printf("  # Kubectl copy from pod:\n")
		fmt.Printf("  kubectl cp <namespace>/<pod>:%s ./%s\n\n", absPath, basename)
	}
}

// createBundle packages all diagnostic artifacts into a .tar.gz archive.
func createBundle(report *Report, logPath, jsonReportPath, tfStatePath, tfPlanPath, bundleOutput string) (string, error) {
	hostname, _ := os.Hostname()
	timestamp := time.Now().Format("20060102-150405")
	bundleName := fmt.Sprintf("kshark-diag-%s-%s", hostname, timestamp)

	if bundleOutput == "" {
		bundleOutput = bundleName + ".tar.gz"
	}

	// Validate paths
	if tfStatePath != "" {
		if strings.Contains(tfStatePath, "..") {
			return "", fmt.Errorf("terraform state path must not contain '..': %s", tfStatePath)
		}
		info, err := os.Stat(tfStatePath)
		if err != nil {
			return "", fmt.Errorf("terraform state file: %w", err)
		}
		if info.Size() > maxTFStateSize {
			return "", fmt.Errorf("terraform state file too large (%d MB, max %d MB)", info.Size()/(1024*1024), maxTFStateSize/(1024*1024))
		}
		if info.Size() > warnTFStateSize {
			fmt.Fprintf(os.Stderr, "Warning: terraform state file is %d MB (large)\n", info.Size()/(1024*1024))
		}
	}
	if tfPlanPath != "" && strings.Contains(tfPlanPath, "..") {
		return "", fmt.Errorf("terraform plan path must not contain '..': %s", tfPlanPath)
	}

	// Create tar.gz
	outFile, err := os.Create(bundleOutput)
	if err != nil {
		return "", fmt.Errorf("create bundle file: %w", err)
	}
	defer outFile.Close()

	gzWriter := gzip.NewWriter(outFile)
	defer gzWriter.Close()

	tw := tar.NewWriter(gzWriter)
	defer tw.Close()

	manifest := &strings.Builder{}
	manifest.WriteString("# kshark Diagnostics Bundle Manifest\n\n")
	manifest.WriteString(fmt.Sprintf("Created: %s\n", time.Now().Format(time.RFC3339)))
	manifest.WriteString(fmt.Sprintf("Hostname: %s\n", hostname))
	manifest.WriteString(fmt.Sprintf("Environment: %s\n\n", detectEnvironment()))
	manifest.WriteString("## Files\n\n")
	manifest.WriteString("| File | SHA256 |\n|---|---|\n")

	// Helper to add a file to the tar
	addFile := func(name string, data []byte) error {
		header := &tar.Header{
			Name:    filepath.Join(bundleName, name),
			Size:    int64(len(data)),
			Mode:    0644,
			ModTime: time.Now(),
		}
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if _, err := tw.Write(data); err != nil {
			return err
		}
		hash := sha256.Sum256(data)
		manifest.WriteString(fmt.Sprintf("| %s | `%x` |\n", name, hash))
		return nil
	}

	// 1. Report JSON
	if reportJSON, err := json.MarshalIndent(report, "", "  "); err == nil {
		if err := addFile("report.json", reportJSON); err != nil {
			slog.Debug("bundle add report.json", "err", err)
		}
	}

	// 2. Log file
	if logPath != "" {
		if logData, err := os.ReadFile(logPath); err == nil {
			if err := addFile("kshark.log", logData); err != nil {
				slog.Debug("bundle add kshark.log", "err", err)
			}
		}
	}

	// 3. JSON report file (if separate from our in-memory report)
	if jsonReportPath != "" {
		if data, err := os.ReadFile(jsonReportPath); err == nil {
			if err := addFile("report-original.json", data); err != nil {
				slog.Debug("bundle add report-original.json", "err", err)
			}
		}
	}

	// 4. Terraform state (redacted)
	if tfStatePath != "" {
		if data, err := os.ReadFile(tfStatePath); err == nil {
			redacted, err := redactTerraformState(data)
			if err != nil {
				slog.Debug("bundle redact terraform state", "err", err)
				redacted = []byte(fmt.Sprintf(`{"error": "failed to redact: %s"}`, err))
			}
			if err := addFile("terraform/terraform.tfstate.redacted", redacted); err != nil {
				slog.Debug("bundle add terraform.tfstate.redacted", "err", err)
			}
		} else {
			slog.Debug("bundle read terraform state", "err", err)
		}
	}

	// 5. Terraform plan (redacted)
	if tfPlanPath != "" {
		if data, err := os.ReadFile(tfPlanPath); err == nil {
			// Text-based redaction for plan output
			redacted := redactPlanText(string(data))
			if err := addFile("terraform/terraform-plan.txt", []byte(redacted)); err != nil {
				slog.Debug("bundle add terraform-plan.txt", "err", err)
			}
		}
	}

	// 6. System context
	sysCtx := captureSystemContext()
	for name, content := range sysCtx {
		if content == "" {
			continue
		}
		if err := addFile("context/"+name+".txt", []byte(content)); err != nil {
			slog.Debug("bundle add context", "file", name, "err", err)
		}
	}

	// 7. Config echo (already redacted in report)
	if report.ConfigEcho != nil {
		configLines := &strings.Builder{}
		for k, v := range report.ConfigEcho {
			configLines.WriteString(fmt.Sprintf("%s=%s\n", k, v))
		}
		if err := addFile("config/client.properties.redacted", []byte(configLines.String())); err != nil {
			slog.Debug("bundle add client.properties.redacted", "err", err)
		}
	}

	// 8. Write manifest
	if err := addFile("MANIFEST.md", []byte(manifest.String())); err != nil {
		slog.Debug("bundle add MANIFEST.md", "err", err)
	}

	return bundleOutput, nil
}

// redactPlanText does simple text-based redaction of Terraform plan output.
func redactPlanText(text string) string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lower := strings.ToLower(line)
		for _, pattern := range tfRedactPatterns {
			if strings.Contains(lower, pattern) && strings.Contains(line, "=") {
				// Redact the value portion after '='
				parts := strings.SplitN(line, "=", 2)
				if len(parts) == 2 {
					lines[i] = parts[0] + "= [REDACTED]"
				}
				break
			}
		}
	}
	return strings.Join(lines, "\n")
}
