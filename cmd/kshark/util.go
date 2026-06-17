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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// ---------- Utilities ----------

// initScanLog sets up structured logging to a file.
// If logFormat is "json", uses JSON handler; otherwise text.
func initScanLog(path string, logFormat string) (*os.File, error) {
	if path == "" {
		// No file logging requested; set a discard handler
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
		return nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: slog.LevelDebug}
	if logFormat == "json" {
		handler = slog.NewJSONHandler(f, opts)
	} else {
		handler = slog.NewTextHandler(f, opts)
	}
	slog.SetDefault(slog.New(handler))
	return f, nil
}

// redactArgs removes sensitive values from CLI arguments to prevent
// credential leakage in reports. Redacts values following known secret flags.
func redactArgs(args []string) []string {
	secretFlags := map[string]bool{
		"-connect-basic-auth":   true,
		"-connect-bearer-token": true,
	}
	out := make([]string, len(args))
	copy(out, args)
	for i := 0; i < len(out)-1; i++ {
		if secretFlags[out[i]] {
			out[i+1] = "[REDACTED]"
		}
	}
	return out
}

func paramMeta(value, def string, provided bool) ParamMeta {
	return ParamMeta{
		Value:     value,
		Default:   def,
		IsDefault: !provided,
		Provided:  provided,
	}
}

func fileSHA256(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func readFileContent(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// writeReportChecksum writes a SHA256 checksum file for the given report.
func writeReportChecksum(path string) (string, string, error) {
	if path == "" {
		return "", "", fmt.Errorf("empty report path")
	}
	hash, err := fileSHA256(path)
	if err != nil {
		return "", "", err
	}
	timestamp := time.Now().Format("20060102_150405")
	dir := filepath.Dir(path)
	checksumPath := filepath.Join(dir, fmt.Sprintf("report_sha256_%s.txt", timestamp))
	content := fmt.Sprintf("%s\t%s\t%s\n", timestamp, path, hash)
	if err := os.WriteFile(checksumPath, []byte(content), 0644); err != nil {
		return "", "", err
	}
	return checksumPath, hash, nil
}
