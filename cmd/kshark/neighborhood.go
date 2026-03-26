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
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Compile-time check that removed imports don't break anything.
// broker_discovery.go now owns: context, strconv for broker discovery.

// ---------- Neighborhood Scan ----------

// PortProbeResult captures the result of probing a single TCP port.
type PortProbeResult struct {
	Port    int           `json:"port"`
	Status  CheckStatus   `json:"status"`
	Latency time.Duration `json:"latency_ms"`
	Detail  string        `json:"detail"`
	Service string        `json:"service"`
}

// RestrictionDiagnosis summarizes the restriction pattern detected by a neighborhood scan.
type RestrictionDiagnosis struct {
	Classification  string   `json:"classification"`
	Confidence      string   `json:"confidence"`
	Evidence        []string `json:"evidence"`
	SuggestedAction string   `json:"suggested_action"`
}

// defaultNeighborhoodPorts are the ports probed by default during a neighborhood scan.
var defaultNeighborhoodPorts = []int{80, 443, 9092, 9093, 9094, 8081, 8083}

// portServiceMap maps well-known ports to their service names.
var portServiceMap = map[int]string{
	80:   "HTTP",
	443:  "HTTPS",
	9092: "Kafka",
	9093: "Kafka SSL",
	9094: "Kafka SASL_SSL",
	8081: "Schema Registry",
	8083: "Kafka Connect",
}

// parseNeighborhoodPorts parses a comma-separated port list or returns defaults.
func parseNeighborhoodPorts(raw string) []int {
	if raw == "" {
		return defaultNeighborhoodPorts
	}
	var ports []int
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(s)
		if p, err := strconv.Atoi(s); err == nil && p > 0 && p <= 65535 {
			ports = append(ports, p)
		}
	}
	if len(ports) == 0 {
		return defaultNeighborhoodPorts
	}
	return ports
}

// portNeighborhoodScan probes a set of TCP ports on the given host concurrently.
func portNeighborhoodScan(host string, _ int, ports []int, totalTimeout time.Duration) []PortProbeResult {
	ctx, cancel := context.WithTimeout(context.Background(), totalTimeout)
	defer cancel()

	var mu sync.Mutex
	var wg sync.WaitGroup
	results := make([]PortProbeResult, 0, len(ports))

	for _, port := range ports {
		p := port
		wg.Add(1)
		go func() {
			defer wg.Done()

			select {
			case <-ctx.Done():
				mu.Lock()
				results = append(results, PortProbeResult{
					Port:    p,
					Status:  FAIL,
					Detail:  "scan timeout",
					Service: portServiceMap[p],
				})
				mu.Unlock()
				return
			default:
			}

			addr := net.JoinHostPort(host, strconv.Itoa(p))
			start := time.Now()
			conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
			elapsed := time.Since(start)

			r := PortProbeResult{
				Port:    p,
				Latency: elapsed,
				Service: portServiceMap[p],
			}
			if err != nil {
				r.Status = FAIL
				if strings.Contains(err.Error(), "refused") {
					r.Detail = "refused"
				} else {
					r.Detail = "timeout"
				}
			} else {
				r.Status = OK
				r.Detail = "connected"
				conn.Close()
			}

			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		}()
	}

	wg.Wait()
	return results
}

// icmpReachable runs a single ping to check ICMP reachability.
func icmpReachable(host string) (bool, time.Duration) {
	if !isValidHostname(host) {
		return false, 0
	}
	start := time.Now()
	out, err := runCmdIfExists("ping", "-c", "1", "-W", "3", host)
	elapsed := time.Since(start)
	if err != nil {
		return false, elapsed
	}
	if strings.Contains(strings.ToLower(out), "1 packets transmitted") &&
		(strings.Contains(out, "1 received") || strings.Contains(out, "1 packets received")) {
		return true, elapsed
	}
	return false, elapsed
}

// classifyRestriction analyzes port scan results and ICMP reachability to diagnose network restrictions.
func classifyRestriction(results []PortProbeResult, icmpOK bool) RestrictionDiagnosis {
	var openPorts, blockedPorts, refusedPorts []int
	var kafkaBlocked bool

	for _, r := range results {
		switch {
		case r.Status == OK:
			openPorts = append(openPorts, r.Port)
		case r.Detail == "refused":
			refusedPorts = append(refusedPorts, r.Port)
		default:
			blockedPorts = append(blockedPorts, r.Port)
			if r.Port >= 9092 && r.Port <= 9094 {
				kafkaBlocked = true
			}
		}
	}

	d := RestrictionDiagnosis{}

	switch {
	// All ports open — not a network restriction
	case len(blockedPorts) == 0 && len(refusedPorts) == 0:
		d.Classification = "no_network_restriction"
		d.Confidence = "high"
		d.Evidence = []string{fmt.Sprintf("All %d probed ports are open", len(results))}
		d.SuggestedAction = "Network path is clear. Check application-layer configuration (auth, ACLs, listener settings)."

	// All ports blocked, ICMP blocked — host unreachable
	case len(openPorts) == 0 && !icmpOK:
		d.Classification = "host_unreachable"
		d.Confidence = "high"
		d.Evidence = []string{
			fmt.Sprintf("All %d probed TCP ports are blocked", len(blockedPorts)+len(refusedPorts)),
			"ICMP ping also fails",
		}
		d.SuggestedAction = "Host appears completely unreachable. Check routing, DNS resolution, and VPC/subnet connectivity."

	// All ports blocked, ICMP open — all TCP blocked
	case len(openPorts) == 0 && icmpOK:
		d.Classification = "all_tcp_blocked"
		d.Confidence = "high"
		d.Evidence = []string{
			fmt.Sprintf("All %d probed TCP ports are blocked", len(blockedPorts)+len(refusedPorts)),
			"ICMP ping succeeds — host is reachable at L3",
		}
		d.SuggestedAction = "All TCP traffic is blocked. Check NSG/Security Group rules for inbound TCP on required ports."

	// Kafka ports blocked, others open — selective port filtering
	case kafkaBlocked && len(openPorts) > 0:
		d.Classification = "selective_port_filtering"
		d.Confidence = "high"
		d.Evidence = []string{
			fmt.Sprintf("Kafka ports blocked: %v", blockedPorts),
			fmt.Sprintf("Open ports: %v", openPorts),
		}
		d.SuggestedAction = "Selective port filtering detected. Open TCP ports 9092-9094 in firewall/NSG/Security Group."

	// Some ports refused — service not listening
	case len(refusedPorts) > 0 && len(blockedPorts) == 0:
		d.Classification = "service_not_listening"
		d.Confidence = "high"
		d.Evidence = []string{
			fmt.Sprintf("Ports refused (service not running): %v", refusedPorts),
			fmt.Sprintf("Open ports: %v", openPorts),
		}
		d.SuggestedAction = "Connection refused — the target service is not listening on those ports. Check broker/service configuration."

	// Mixed results
	default:
		d.Classification = "mixed_filtering"
		d.Confidence = "medium"
		var evidence []string
		if len(openPorts) > 0 {
			evidence = append(evidence, fmt.Sprintf("Open: %v", openPorts))
		}
		if len(blockedPorts) > 0 {
			evidence = append(evidence, fmt.Sprintf("Blocked: %v", blockedPorts))
		}
		if len(refusedPorts) > 0 {
			evidence = append(evidence, fmt.Sprintf("Refused: %v", refusedPorts))
		}
		if icmpOK {
			evidence = append(evidence, "ICMP: open")
		} else {
			evidence = append(evidence, "ICMP: blocked")
		}
		d.Evidence = evidence
		d.SuggestedAction = "Mixed port filtering detected. Review firewall/NSG rules for each port individually."
	}

	return d
}

// runNeighborhoodScan runs a port neighborhood scan and adds results to the report.
func runNeighborhoodScan(r *Report, host string, failedPort int, customPorts string, timeout time.Duration) {
	if !isValidHostname(host) {
		return
	}

	slog.Debug("neighborhood scan start", "host", host, "failedPort", failedPort)

	ports := parseNeighborhoodPorts(customPorts)
	results := portNeighborhoodScan(host, failedPort, ports, timeout)

	// ICMP check
	icmpOK, icmpLatency := icmpReachable(host)

	// Classify restriction pattern
	diagnosis := classifyRestriction(results, icmpOK)

	// Build port summary string
	var parts []string
	for _, r := range results {
		status := "OPEN"
		if r.Status == FAIL {
			if r.Detail == "refused" {
				status = "REFUSED"
			} else {
				status = "BLOCKED"
			}
		}
		parts = append(parts, fmt.Sprintf("%d=%s", r.Port, status))
	}
	portSummary := strings.Join(parts, ", ")

	// Add summary row
	detail := fmt.Sprintf("Port scan: %s", portSummary)
	if icmpOK {
		detail += fmt.Sprintf(" | ICMP: OPEN (%dms)", icmpLatency.Milliseconds())
	} else {
		detail += " | ICMP: BLOCKED"
	}

	hint := fmt.Sprintf("%s [confidence: %s] %s", diagnosis.Classification, diagnosis.Confidence, diagnosis.SuggestedAction)
	addRow(r, Row{"neighborhood", host, DIAG, WARN, detail, hint})

	slog.Debug("neighborhood scan done", "host", host, "classification", diagnosis.Classification, "confidence", diagnosis.Confidence)
}

// hasNeighborhoodRows checks if neighborhood scan rows already exist for a given host.
func hasNeighborhoodRows(r *Report, host string) bool {
	for _, row := range r.Rows {
		if row.Component == "neighborhood" && row.Target == host {
			return true
		}
	}
	return false
}

// ---------- Connector Neighborhood Ports ----------

// connectorNeighborhoodPortsMap maps connector types to their default neighborhood ports.
var connectorNeighborhoodPortsMap = map[string][]int{
	"mongodb":    {27017, 27018, 27019, 443},
	"postgresql": {5432, 5433, 443},
	"db2":        {50000, 50001, 446, 443},
	"mysql":      {3306, 3307, 443},
	"sqlserver":  {1433, 1434, 443},
	"oracle":     {1521, 1522, 443},
	"redis":      {6379, 6380, 443},
	"elasticsearch": {9200, 9300, 443},
}

// connectorNeighborhoodPorts returns the default neighborhood ports for a given connector type.
func connectorNeighborhoodPorts(connectorType string) []int {
	if ports, ok := connectorNeighborhoodPortsMap[connectorType]; ok {
		return ports
	}
	return defaultNeighborhoodPorts
}
