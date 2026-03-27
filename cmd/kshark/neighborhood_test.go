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
	"fmt"
	"net"
	"testing"
	"time"
)

func TestParseNeighborhoodPorts_Default(t *testing.T) {
	ports := parseNeighborhoodPorts("")
	if len(ports) != len(defaultNeighborhoodPorts) {
		t.Errorf("expected %d default ports, got %d", len(defaultNeighborhoodPorts), len(ports))
	}
}

func TestParseNeighborhoodPorts_Custom(t *testing.T) {
	ports := parseNeighborhoodPorts("443,9092,5432")
	if len(ports) != 3 {
		t.Fatalf("expected 3 ports, got %d", len(ports))
	}
	if ports[0] != 443 || ports[1] != 9092 || ports[2] != 5432 {
		t.Errorf("unexpected ports: %v", ports)
	}
}

func TestParseNeighborhoodPorts_Invalid(t *testing.T) {
	ports := parseNeighborhoodPorts("abc,def")
	if len(ports) != len(defaultNeighborhoodPorts) {
		t.Errorf("expected defaults on invalid input, got %d ports", len(ports))
	}
}

func TestParseNeighborhoodPorts_OutOfRange(t *testing.T) {
	ports := parseNeighborhoodPorts("0,99999,443")
	if len(ports) != 1 || ports[0] != 443 {
		t.Errorf("expected only [443], got %v", ports)
	}
}

func TestPortNeighborhoodScan_OpenPort(t *testing.T) {
	// Start a mock server
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	port := ln.Addr().(*net.TCPAddr).Port
	results := portNeighborhoodScan("127.0.0.1", 0, []int{port}, 3*time.Second)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Status != OK {
		t.Errorf("expected OK for open port, got %s: %s", results[0].Status, results[0].Detail)
	}
}

func TestPortNeighborhoodScan_ClosedPort(t *testing.T) {
	// Find a port that's definitely not listening
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close() // close it so it's not listening

	results := portNeighborhoodScan("127.0.0.1", 0, []int{port}, 3*time.Second)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Status != FAIL {
		t.Errorf("expected FAIL for closed port, got %s", results[0].Status)
	}
}

func TestClassifyRestriction_AllOpen(t *testing.T) {
	results := []PortProbeResult{
		{Port: 443, Status: OK},
		{Port: 9092, Status: OK},
	}
	d := classifyRestriction(results, true)
	if d.Classification != "no_network_restriction" {
		t.Errorf("expected no_network_restriction, got %s", d.Classification)
	}
	if d.Confidence != "high" {
		t.Errorf("expected high confidence, got %s", d.Confidence)
	}
}

func TestClassifyRestriction_HostUnreachable(t *testing.T) {
	results := []PortProbeResult{
		{Port: 443, Status: FAIL, Detail: "timeout"},
		{Port: 9092, Status: FAIL, Detail: "timeout"},
	}
	d := classifyRestriction(results, false)
	if d.Classification != "host_unreachable" {
		t.Errorf("expected host_unreachable, got %s", d.Classification)
	}
}

func TestClassifyRestriction_AllTCPBlocked(t *testing.T) {
	results := []PortProbeResult{
		{Port: 443, Status: FAIL, Detail: "timeout"},
		{Port: 9092, Status: FAIL, Detail: "timeout"},
	}
	d := classifyRestriction(results, true) // ICMP works
	if d.Classification != "all_tcp_blocked" {
		t.Errorf("expected all_tcp_blocked, got %s", d.Classification)
	}
}

func TestClassifyRestriction_SelectiveFiltering(t *testing.T) {
	results := []PortProbeResult{
		{Port: 443, Status: OK},
		{Port: 9092, Status: FAIL, Detail: "timeout"},
		{Port: 9093, Status: FAIL, Detail: "timeout"},
	}
	d := classifyRestriction(results, true)
	if d.Classification != "selective_port_filtering" {
		t.Errorf("expected selective_port_filtering, got %s", d.Classification)
	}
}

func TestClassifyRestriction_ServiceNotListening(t *testing.T) {
	results := []PortProbeResult{
		{Port: 443, Status: OK},
		{Port: 9092, Status: FAIL, Detail: "refused"},
	}
	d := classifyRestriction(results, true)
	if d.Classification != "service_not_listening" {
		t.Errorf("expected service_not_listening, got %s", d.Classification)
	}
}

func TestConnectorNeighborhoodPorts(t *testing.T) {
	tests := []struct {
		connType string
		wantLen  int
		wantPort int
	}{
		{"mongodb", 4, 27017},
		{"postgresql", 3, 5432},
		{"db2", 4, 50000},
		{"mysql", 3, 3306},
		{"sqlserver", 3, 1433},
		{"oracle", 3, 1521},
		{"redis", 3, 6379},
		{"elasticsearch", 3, 9200},
		{"unknown", len(defaultNeighborhoodPorts), 80},
	}

	for _, tt := range tests {
		t.Run(tt.connType, func(t *testing.T) {
			ports := connectorNeighborhoodPorts(tt.connType)
			if len(ports) != tt.wantLen {
				t.Errorf("expected %d ports for %s, got %d: %v", tt.wantLen, tt.connType, len(ports), ports)
			}
			if ports[0] != tt.wantPort {
				t.Errorf("expected first port %d for %s, got %d", tt.wantPort, tt.connType, ports[0])
			}
		})
	}
}

func TestPortNeighborhoodScan_MultiplePortsMixed(t *testing.T) {
	// Listen on two ports, leave one closed
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln1.Close()
	go func() {
		for {
			conn, err := ln1.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln2.Close()
	go func() {
		for {
			conn, err := ln2.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	// Get a closed port
	lnClosed, _ := net.Listen("tcp", "127.0.0.1:0")
	closedPort := lnClosed.Addr().(*net.TCPAddr).Port
	lnClosed.Close()

	port1 := ln1.Addr().(*net.TCPAddr).Port
	port2 := ln2.Addr().(*net.TCPAddr).Port

	results := portNeighborhoodScan("127.0.0.1", 0, []int{port1, port2, closedPort}, 5*time.Second)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	openCount := 0
	failCount := 0
	for _, r := range results {
		if r.Status == OK {
			openCount++
		} else {
			failCount++
		}
	}
	if openCount != 2 {
		t.Errorf("expected 2 open ports, got %d", openCount)
	}
	if failCount != 1 {
		t.Errorf("expected 1 failed port, got %d", failCount)
	}
}

func TestPortNeighborhoodScan_CompletesWithinTimeout(t *testing.T) {
	start := time.Now()
	// Scan ports on a non-routable address; should respect the 2s timeout
	results := portNeighborhoodScan("192.0.2.1", 0, []int{80, 443, 9092}, 2*time.Second)
	elapsed := time.Since(start)

	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
	if elapsed > 5*time.Second {
		t.Errorf("scan took %v, expected completion within ~2s timeout", elapsed)
	}
}

func TestClassifyRestriction_MixedFiltering(t *testing.T) {
	results := []PortProbeResult{
		{Port: 80, Status: OK},
		{Port: 443, Status: FAIL, Detail: "timeout"},
		{Port: 9092, Status: FAIL, Detail: "refused"},
	}
	d := classifyRestriction(results, true)
	if d.Classification != "mixed_filtering" {
		t.Errorf("expected mixed_filtering, got %s", d.Classification)
	}
	if d.Confidence != "medium" {
		t.Errorf("expected medium confidence, got %s", d.Confidence)
	}
}

func TestRunNeighborhoodScan_InvalidHost(t *testing.T) {
	r := &Report{}
	runNeighborhoodScan(r, ";inject", 9092, "", 3*time.Second)
	// Should not add any rows (hostname rejected)
	if len(r.Rows) != 0 {
		t.Errorf("expected 0 rows for invalid hostname, got %d", len(r.Rows))
	}
}

func TestRunNeighborhoodScan_WithOpenPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()
	port := ln.Addr().(*net.TCPAddr).Port

	r := &Report{}
	runNeighborhoodScan(r, "127.0.0.1", port, fmt.Sprintf("%d", port), 5*time.Second)

	if len(r.Rows) == 0 {
		t.Fatal("expected at least 1 row from neighborhood scan")
	}
	if r.Rows[0].Component != "neighborhood" {
		t.Errorf("component = %q, want 'neighborhood'", r.Rows[0].Component)
	}
}

func TestHasNeighborhoodRows(t *testing.T) {
	r := &Report{}
	if hasNeighborhoodRows(r, "host1") {
		t.Error("expected false for empty report")
	}

	addRow(r, Row{"neighborhood", "host1", DIAG, WARN, "test", ""})
	if !hasNeighborhoodRows(r, "host1") {
		t.Error("expected true after adding neighborhood row")
	}
	if hasNeighborhoodRows(r, "host2") {
		t.Error("expected false for different host")
	}
}
