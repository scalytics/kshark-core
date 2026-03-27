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
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/scalytics/kshark-core/internal/connectapi"
)

// scanConfig holds all parameters needed for running a scan.
type scanConfig struct {
	props          map[string]string
	bootstrap      string
	topics         []string
	diag           bool
	group          string
	kafkaTimeout   time.Duration
	produceTimeout time.Duration
	consumeTimeout time.Duration
	balancer       string
	startOffset    int64
	connectURL     string
	connectorName  string
	connectorCfg   string
	connectAuth    connectapi.ConnectAuthOpts
	probeDirection string // "up" (default, fail-fast) or "full" (all layers)
	neighborhood   bool   // run port neighborhood scan on TCP failure
	neighborPorts  string // comma-separated custom port list for neighborhood scan
}

// ---------- Scan Plan ----------

func parseTopics(raw string) []string {
	if raw == "" {
		return nil
	}
	var topics []string
	for _, t := range strings.Split(raw, ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			topics = append(topics, t)
		}
	}
	return topics
}

func printScanPlan(props map[string]string, topics []string, diag bool, analyze bool, jsonOut string, aiProvider string, aiModel string) {
	fmt.Println("\n--- Scan Plan ---")
	fmt.Printf("Target Kafka Cluster: %s\n", props["bootstrap.servers"])
	if len(topics) > 0 {
		fmt.Printf("Target Topics: %s\n", strings.Join(topics, ", "))
	} else {
		fmt.Println("Target Topics: (none, metadata checks only)")
	}

	fmt.Printf("\nEdition: Open Source (non-commercial)\n")
	fmt.Printf("Version: %s\n", version)

	fmt.Println("\nChecks to be performed:")
	fmt.Println("  - Connectivity Checks (DNS, TCP, TLS) for each broker.")
	fmt.Println("  - Kafka Protocol Checks (ApiVersions, Topic Metadata).")
	if len(topics) > 0 {
		fmt.Println("  - Produce & Consume Probe.")
	}
	if props["schema.registry.url"] != "" {
		fmt.Printf("  - Schema Registry Check: %s\n", props["schema.registry.url"])
	}
	if props["rest.proxy.url"] != "" {
		fmt.Printf("  - REST Proxy Check: %s\n", props["rest.proxy.url"])
	}
	if diag {
		fmt.Println("  - Network Diagnostics (Traceroute, MTU).")
	}

	if analyze {
		if aiProvider != "" && aiModel != "" {
			fmt.Printf("\nAI Analysis: enabled (provider: %s, model: %s)\n", aiProvider, aiModel)
		} else {
			fmt.Println("\nAI Analysis: enabled (no AI provider configured, will generate prompt file)")
		}
	}

	if jsonOut != "" {
		safePath, _ := createSafeReportPath(jsonOut, "reports")
		absPath, _ := filepath.Abs(safePath)
		fmt.Printf("JSON Report: %s\n", absPath)
	}

	reportsDir, _ := filepath.Abs("reports")
	fmt.Printf("Reports & Prompts: %s\n", reportsDir)
	fmt.Println("-------------------")
}

// ---------- Scan ----------

// runScan executes all connectivity checks: broker probes, produce/consume,
// schema registry, connector probe, and REST proxy. Returns early on timeout.
func runScan(ctx context.Context, report *Report, cfg scanConfig) {
	fullMode := cfg.probeDirection == "full"

	// Collect hosts for parallel diagnostics
	var diagHosts []string

	// Per-broker checks (skip if no bootstrap.servers, i.e. connector-only mode)
	brokers := strings.Split(cfg.bootstrap, ",")
	if cfg.bootstrap == "" {
		brokers = nil
	}
	for _, b := range brokers {
		select {
		case <-ctx.Done():
			addRow(report, Row{"kshark", "timeout", DIAG, FAIL, "Global timeout reached during broker checks", ""})
			return
		default:
		}

		b = strings.TrimSpace(b)
		host, port, err := net.SplitHostPort(b)
		if err != nil {
			addRow(report, Row{"kafka", b, L3, FAIL, "Invalid host:port", "Fix bootstrap.servers format (host:port)."})
			continue
		}
		slog.Debug("broker check start", "host", host, "port", port)

		if cfg.diag {
			diagHosts = append(diagHosts, host)
		}

		checkDNS(ctx, report, host, "kafka")
		addr := net.JoinHostPort(host, port)
		conn := checkTCP(ctx, report, addr, "kafka", 8*time.Second)
		if conn == nil {
			slog.Debug("broker check failed tcp", "addr", addr)
			// Trigger neighborhood scan on TCP failure
			if cfg.diag || cfg.neighborhood {
				portInt, _ := strconv.Atoi(port)
				runNeighborhoodScan(report, host, portInt, cfg.neighborPorts, 5*time.Second)
			}
			if !fullMode {
				continue
			}
			// In full mode, skip TLS/Kafka (no socket) but continue to next broker
			continue
		}
		tlsConf, _, err := tlsConfigFromProps(cfg.props, host)
		if err != nil {
			addRow(report, Row{"kafka", addr, L56, FAIL, fmt.Sprintf("TLS config err: %v", err), ""})
			_ = conn.Close()
			if !fullMode {
				continue
			}
			// In full mode: TLS failed, but still try L7 topic checks (they'll likely fail too,
			// but the error messages are diagnostic)
		}
		var secured net.Conn = conn
		var tlsOK bool
		if err == nil {
			if tlsConf != nil {
				secured = wrapTLS(ctx, report, conn, tlsConf, "kafka", addr)
				if secured == nil {
					slog.Debug("broker check failed tls", "addr", addr)
					if !fullMode {
						continue
					}
					// In full mode: TLS failed, still attempt L7 topic checks below
				} else {
					tlsOK = true
				}
			} else {
				addRow(report, Row{"kafka", addr, L56, SKIP, "PLAINTEXT (no TLS)", "Prefer SSL/SASL_SSL."})
				tlsOK = true
			}
		}
		if secured != nil {
			_ = secured.Close()
		} else if conn != nil {
			_ = conn.Close()
		}

		if tlsOK || fullMode {
			slog.Debug("broker check proceeding to L7", "addr", addr, "tlsOK", tlsOK, "fullMode", fullMode)
			for _, t := range cfg.topics {
				slog.Debug("topic metadata check", "topic", t, "broker", addr)
				checkTopic(report, cfg.props, addr, t, cfg.kafkaTimeout)
			}
		}
	}

	// Run diagnostics in parallel (non-blocking)
	var diagWg sync.WaitGroup
	if len(diagHosts) > 0 {
		runDiagnosticsParallel(ctx, report, diagHosts, &diagWg)
	}

	// Produce/Consume
	for _, t := range cfg.topics {
		select {
		case <-ctx.Done():
			addRow(report, Row{"kshark", "timeout", DIAG, FAIL, "Global timeout reached before produce/consume", ""})
			diagWg.Wait()
			return
		default:
			slog.Debug("produce/consume start", "topic", t, "group", cfg.group)
			probeProduceConsume(ctx, report, cfg.props, cfg.bootstrap, t, cfg.group,
				cfg.produceTimeout, cfg.consumeTimeout, cfg.balancer, cfg.kafkaTimeout, cfg.startOffset)
		}
	}

	// Schema Registry
	select {
	case <-ctx.Done():
		addRow(report, Row{"kshark", "timeout", DIAG, FAIL, "Global timeout reached before schema registry check", ""})
		diagWg.Wait()
		return
	default:
		slog.Debug("schema registry check start")
		checkSchemaRegistry(ctx, report, cfg.props)
	}

	// Connector Probe
	select {
	case <-ctx.Done():
		addRow(report, Row{"kshark", "timeout", DIAG, FAIL, "Global timeout reached before connector probe", ""})
		diagWg.Wait()
		return
	default:
		runConnectorProbe(ctx, report, cfg.connectURL, cfg.connectorName, cfg.connectorCfg, cfg.connectAuth)
	}

	// REST Proxy
	checkRESTProxy(ctx, report, cfg.props)

	// Wait for parallel diagnostics to finish
	diagWg.Wait()

	// Cross-reference MTU results with produce/consume results
	mtuCorrelation(report)

	// Broker discovery scan: probe all advertised listeners not in bootstrap
	if cfg.diag && len(brokers) > 0 {
		brokerDiscoveryScan(report, cfg.props, brokers, cfg.kafkaTimeout)
	}

	// Run forced neighborhood scan if --neighborhood flag is set (even on success)
	if cfg.neighborhood {
		for _, b := range brokers {
			b = strings.TrimSpace(b)
			host, port, err := net.SplitHostPort(b)
			if err != nil {
				continue
			}
			portInt, _ := strconv.Atoi(port)
			// Only scan if we haven't already (triggered on failure above)
			if !hasNeighborhoodRows(report, host) {
				runNeighborhoodScan(report, host, portInt, cfg.neighborPorts, 5*time.Second)
			}
		}
	}
}

// runDiagnosticsParallel runs traceroute and MTU checks concurrently for all hosts.
func runDiagnosticsParallel(ctx context.Context, report *Report, hosts []string, wg *sync.WaitGroup) {
	for _, host := range hosts {
		h := host
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			default:
			}
			bestEffortTraceroute(report, h)
			mtuCheck(report, h)
		}()
	}
}

// checkRESTProxy validates and probes a REST Proxy endpoint if configured.
func checkRESTProxy(ctx context.Context, report *Report, props map[string]string) {
	rest := strings.TrimSpace(props["rest.proxy.url"])
	if rest == "" {
		return
	}

	select {
	case <-ctx.Done():
		addRow(report, Row{"kshark", "timeout", DIAG, FAIL, "Global timeout reached before REST proxy check", ""})
		return
	default:
	}

	slog.Debug("rest proxy check start", "url", rest)

	if warning, ssrfErr := isAllowedURL(rest); ssrfErr != nil {
		addRow(report, Row{"rest-proxy", rest, DIAG, FAIL, fmt.Sprintf("SSRF check: %v", ssrfErr), "Use a valid, non-loopback URL."})
		return
	} else if warning != "" {
		slog.Warn("rest proxy private address", "warning", warning)
	}

	checkDNS(ctx, report, extractHost(rest), "rest-proxy")
	tlsConf, _, err := tlsConfigFromProps(props, extractHost(rest))
	if err != nil {
		addRow(report, Row{"rest-proxy", rest, HTTP, FAIL, fmt.Sprintf("TLS config err: %v", err), ""})
		return
	}

	client := httpClientFromTLS(tlsConf, 8*time.Second)
	req, _ := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(rest, "/")+"/topics", nil)
	httpStart := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		slog.Debug("rest proxy http", "url", rest, "dur", time.Since(httpStart).Truncate(time.Millisecond), "err", err)
		addRow(report, Row{"rest-proxy", rest, HTTP, FAIL, fmt.Sprintf("GET /topics failed: %v", err), "Check listener/auth."})
		return
	}
	slog.Debug("rest proxy http ok", "url", rest, "dur", time.Since(httpStart).Truncate(time.Millisecond), "status", resp.StatusCode)
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()

	switch {
	case resp.StatusCode == 200:
		addRow(report, Row{"rest-proxy", rest, HTTP, OK, "GET /topics OK", ""})
	case resp.StatusCode == 401 || resp.StatusCode == 403:
		addRow(report, Row{"rest-proxy", rest, HTTP, FAIL, fmt.Sprintf("Auth %d", resp.StatusCode), "Check credentials or mTLS mapping."})
	default:
		addRow(report, Row{"rest-proxy", rest, HTTP, WARN, fmt.Sprintf("HTTP %d", resp.StatusCode), ""})
	}
}
