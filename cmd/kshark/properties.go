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
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"
)

// ---------- Properties parsing & presets ----------

func loadProperties(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	props := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}
		sep := "="
		if !strings.Contains(line, "=") && strings.Contains(line, ":") {
			sep = ":"
		}
		parts := strings.SplitN(line, sep, 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		val = os.ExpandEnv(val)
		props[key] = val
	}
	return props, sc.Err()
}

func applyPreset(preset string, p map[string]string) {
	switch preset {
	case "cc-plain":
		// Confluent Cloud (classic API key/secret)
		setDefault(p, "security.protocol", "SASL_SSL")
		setDefault(p, "sasl.mechanism", "PLAIN")
		// expects sasl.username / sasl.password to be present
	case "self-scram":
		setDefault(p, "security.protocol", "SASL_SSL")
		setDefault(p, "sasl.mechanism", "SCRAM-SHA-512")
	}
}
func setDefault(p map[string]string, k, v string) {
	if _, ok := p[k]; !ok {
		p[k] = v
	}
}

func redactProps(p map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range p {
		lk := strings.ToLower(k)
		switch {
		case strings.Contains(lk, "password"),
			strings.Contains(lk, "secret"),
			strings.Contains(lk, "key"),
			k == "sasl.oauthbearer.token",
			k == "basic.auth.user.info",
			k == "sasl.jaas.config",
			strings.Contains(lk, "bearer"),
			strings.Contains(lk, "token"),
			strings.Contains(lk, "credential"):
			out[k] = "[REDACTED]"
		default:
			out[k] = v
		}
	}
	return out
}

// warnInsecurePermissions logs a warning if a file is readable by group or others.
func warnInsecurePermissions(path string) {
	if runtime.GOOS == "windows" {
		return // Windows permissions work differently
	}
	info, err := os.Stat(path)
	if err != nil {
		return
	}
	mode := info.Mode().Perm()
	if mode&0077 != 0 {
		slog.Warn("properties file has insecure permissions",
			"path", path,
			"permissions", fmt.Sprintf("%04o", mode),
			"recommended", "0600",
		)
		fmt.Fprintf(os.Stderr, "Warning: %s has permissions %04o (recommend 0600). Run: chmod 600 %s\n",
			path, mode, path)
	}
}
