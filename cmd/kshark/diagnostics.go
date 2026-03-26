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
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// ---------- Diagnostics (traceroute / MTU) ----------

func runCmdIfExists(name string, args ...string) (string, error) {
	path, err := exec.LookPath(name)
	if err != nil {
		return "", err
	}
	cmd := exec.Command(path, args...)
	cmd.Env = os.Environ()
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err = cmd.Run()
	return buf.String(), err
}

// isValidHostname checks if the hostname is valid and doesn't contain malicious characters.
func isValidHostname(host string) bool {
	// Regex to match a valid hostname (alphanumeric, hyphens, periods).
	// It explicitly disallows characters like ';', '&', '|', '`', '$', '(', ')', etc.
	re := regexp.MustCompile(`^[a-zA-Z0-9\.\-]+$`)
	return re.MatchString(host)
}

func bestEffortTraceroute(r *Report, host string) {
	if !isValidHostname(host) {
		addRow(r, Row{"diag", host, DIAG, FAIL, "Invalid hostname provided", "Skipping traceroute to prevent command injection."})
		return
	}
	// Linux: traceroute or tracepath; macOS: traceroute; Windows: tracert
	start := time.Now()
	if out, err := runCmdIfExists("traceroute", "-n", "-w", "2", "-q", "1", host); err == nil {
		slog.Debug("traceroute ok", "host", host, "dur", time.Since(start).Truncate(time.Millisecond))
		addRow(r, Row{"diag", host, DIAG, OK, "traceroute OK (see JSON)", ""})
		addRow(r, Row{"diag", host, DIAG, SKIP, trimLines(out, 15), "Full output in JSON if -json used."})
		return
	}
	slog.Debug("traceroute not found", "host", host, "dur", time.Since(start).Truncate(time.Millisecond))
	start = time.Now()
	if out, err := runCmdIfExists("tracepath", host); err == nil {
		slog.Debug("tracepath ok", "host", host, "dur", time.Since(start).Truncate(time.Millisecond))
		addRow(r, Row{"diag", host, DIAG, OK, "tracepath OK (see JSON)", ""})
		addRow(r, Row{"diag", host, DIAG, SKIP, trimLines(out, 15), "Full output in JSON if -json used."})
		return
	}
	slog.Debug("tracepath not found", "host", host, "dur", time.Since(start).Truncate(time.Millisecond))
	start = time.Now()
	if out, err := runCmdIfExists("tracert", "-d", "-w", "2000", host); err == nil {
		slog.Debug("tracert ok", "host", host, "dur", time.Since(start).Truncate(time.Millisecond))
		addRow(r, Row{"diag", host, DIAG, OK, "tracert OK (see JSON)", ""})
		addRow(r, Row{"diag", host, DIAG, SKIP, trimLines(out, 15), "Full output in JSON if -json used."})
		return
	}
	slog.Debug("tracert not found", "host", host, "dur", time.Since(start).Truncate(time.Millisecond))
	addRow(r, Row{"diag", host, DIAG, SKIP, "No traceroute tool found", "Install traceroute/tracepath (Linux), or use tracert (Windows)."})
}

func mtuCheck(r *Report, host string) {
	if !isValidHostname(host) {
		addRow(r, Row{"diag", host, DIAG, FAIL, "Invalid hostname provided", "Skipping MTU check to prevent command injection."})
		return
	}
	// Linux tracepath usually reports pMTU; otherwise try ping DF
	start := time.Now()
	mtuHint := "MTU probe uses ICMP — results may differ from TCP on port 9092."
	if out, err := runCmdIfExists("tracepath", host); err == nil && strings.Contains(out, "pmtu") {
		slog.Debug("mtu tracepath ok", "host", host, "dur", time.Since(start).Truncate(time.Millisecond))
		addRow(r, Row{"diag", host, DIAG, OK, "pMTU detected via tracepath", mtuHint})
		return
	}
	slog.Debug("mtu tracepath skip", "host", host, "dur", time.Since(start).Truncate(time.Millisecond))
	// Try ping DF with decreasing sizes (Linux/macOS)
	for _, sz := range []int{1472, 1464, 1452, 1400, 1200, 1000} {
		var out string
		var err error
		pingStart := time.Now()
		if runtime.GOOS == "darwin" {
			out, err = runCmdIfExists("ping", "-D", "-s", strconv.Itoa(sz), "-c", "1", host)
		} else {
			out, err = runCmdIfExists("ping", "-M", "do", "-s", strconv.Itoa(sz), "-c", "1", host)
		}
		if err == nil && strings.Contains(strings.ToLower(out), "1 packets transmitted") {
			slog.Debug("mtu ping ok", "host", host, "size", sz, "dur", time.Since(pingStart).Truncate(time.Millisecond))
			addRow(r, Row{"diag", host, DIAG, OK, fmt.Sprintf("MTU ok at payload %d", sz), mtuHint})
			return
		}
		slog.Debug("mtu ping", "host", host, "size", sz, "dur", time.Since(pingStart).Truncate(time.Millisecond), "err", err)
	}
	addRow(r, Row{"diag", host, DIAG, SKIP, "MTU probe inconclusive", "Run tracepath or adjust network MTU if you see fragmentation. " + mtuHint})
}

// mtuCorrelation cross-references MTU OK results with produce/consume timeout/fail
// to detect possible PMTU black holes where ICMP works but large TCP payloads don't.
func mtuCorrelation(r *Report) {
	var mtuOK bool
	var produceTimeout bool

	// Safe to read without mutex: called after all goroutines have completed (diagWg.Wait).
	for _, row := range r.Rows {
		if row.Component == "diag" && strings.HasPrefix(row.Detail, "MTU ok") && row.Status == OK {
			mtuOK = true
		}
		if row.Component == "kafka" && row.Layer == L7 && row.Status == FAIL {
			detail := strings.ToLower(row.Detail)
			if strings.Contains(detail, "timeout") || strings.Contains(detail, "timed out") {
				produceTimeout = true
			}
		}
		if row.Component == "kshark" && row.Detail == "Global timeout reached before produce/consume" {
			produceTimeout = true
		}
	}

	if mtuOK && produceTimeout {
		addRow(r, Row{
			"diag", "pmtu-correlation", DIAG, WARN,
			"PMTU black hole suspected: ICMP MTU probe OK, but Kafka produce/consume timed out",
			"ICMP and TCP may follow different filtering rules. Check intermediate firewalls for ICMP 'Fragmentation Needed' blocking.",
		})
	}
}

func trimLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[:n], "\n") + "\n…"
}
