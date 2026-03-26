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
	"time"
)

// ---------- Broker Discovery Scan ----------

// maxDiscoveredBrokers limits the number of non-bootstrap brokers to probe,
// preventing a probe storm against large clusters.
const maxDiscoveredBrokers = 10

// brokerDiscoveryScan discovers all brokers from the Kafka Metadata response
// and probes any that are NOT in the bootstrap list. This detects advertised
// listener mismatches — the most common Kafka networking problem.
func brokerDiscoveryScan(r *Report, props map[string]string, bootstrapAddrs []string, kafkaTimeout time.Duration) {
	if len(bootstrapAddrs) == 0 {
		return
	}

	// Build set of known bootstrap hosts for comparison
	bootstrapSet := make(map[string]bool)
	for _, addr := range bootstrapAddrs {
		addr = strings.TrimSpace(addr)
		bootstrapSet[addr] = true
		if h, _, err := net.SplitHostPort(addr); err == nil {
			bootstrapSet[h] = true
		}
	}

	// Connect to first reachable bootstrap broker and get metadata
	for _, addr := range bootstrapAddrs {
		addr = strings.TrimSpace(addr)
		host, _, _ := net.SplitHostPort(addr)
		dialer, _, err := dialerFromProps(props, host)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), kafkaTimeout)
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		cancel()
		if err != nil {
			continue
		}

		parts, err := conn.ReadPartitions()
		conn.Close()
		if err != nil {
			slog.Debug("broker discovery: ReadPartitions failed", "addr", addr, "err", err)
			continue
		}

		// Extract unique broker addresses from partition leaders/replicas
		discoveredBrokers := make(map[string]bool)
		for _, p := range parts {
			if p.Leader.Host != "" {
				brokerAddr := net.JoinHostPort(p.Leader.Host, strconv.Itoa(p.Leader.Port))
				if !bootstrapSet[brokerAddr] && !bootstrapSet[p.Leader.Host] {
					discoveredBrokers[brokerAddr] = true
				}
			}
			for _, replica := range p.Replicas {
				if replica.Host != "" {
					brokerAddr := net.JoinHostPort(replica.Host, strconv.Itoa(replica.Port))
					if !bootstrapSet[brokerAddr] && !bootstrapSet[replica.Host] {
						discoveredBrokers[brokerAddr] = true
					}
				}
			}
		}

		if len(discoveredBrokers) == 0 {
			slog.Debug("broker discovery: no additional brokers found beyond bootstrap")
			return
		}

		slog.Debug("broker discovery: found non-bootstrap brokers", "count", len(discoveredBrokers))

		// Probe each discovered broker with DNS + TCP (capped)
		probed := 0
		for brokerAddr := range discoveredBrokers {
			if probed >= maxDiscoveredBrokers {
				addRow(r, Row{
					"neighborhood", "discovery", DIAG, WARN,
					fmt.Sprintf("Broker discovery capped at %d (cluster has %d non-bootstrap brokers)", maxDiscoveredBrokers, len(discoveredBrokers)),
					"Large cluster detected. Only first 10 discovered brokers were probed.",
				})
				break
			}

			host, _, err := net.SplitHostPort(brokerAddr)
			if err != nil {
				continue
			}

			addrs, dnsErr := net.LookupHost(host)
			if dnsErr != nil {
				addRow(r, Row{
					"neighborhood", brokerAddr, L3, FAIL,
					fmt.Sprintf("Discovered broker DNS failed: %v", dnsErr),
					"Cluster advertises " + host + " but it is not resolvable from this network. Check advertised.listeners or DNS setup.",
				})
				probed++
				continue
			}
			slog.Debug("broker discovery DNS ok", "host", host, "addrs", addrs)

			tcpConn, tcpErr := net.DialTimeout("tcp", brokerAddr, 5*time.Second)
			if tcpErr != nil {
				addRow(r, Row{
					"neighborhood", brokerAddr, L4, FAIL,
					fmt.Sprintf("Discovered broker unreachable: %v", tcpErr),
					"Cluster advertises " + brokerAddr + " but TCP connect fails. Check advertised.listeners, firewall rules, or PrivateLink config.",
				})
				probed++
				continue
			}
			tcpConn.Close()

			addRow(r, Row{
				"neighborhood", brokerAddr, DIAG, OK,
				fmt.Sprintf("Discovered broker reachable (resolved to %s)", strings.Join(addrs, ",")),
				"",
			})
			probed++
		}
		return // only need one successful bootstrap connection
	}
}
