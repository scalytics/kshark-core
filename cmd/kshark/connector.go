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
	"strings"
	"time"

	"github.com/scalytics/kshark-core/internal/connectapi"
	"github.com/scalytics/kshark-core/internal/probe"
)

// ---------- Connector Probe ----------

// runConnectorProbe fetches connector config and probes the target database.
func runConnectorProbe(ctx context.Context, r *Report, connectURL, connectorName, connectorConfigPath string, auth connectapi.ConnectAuthOpts) {
	if connectURL == "" && connectorName == "" && connectorConfigPath == "" {
		return
	}

	slog.Debug("connector probe start", "connect_url", connectURL, "connector_name", connectorName, "config_path", connectorConfigPath)

	// Step 1: Obtain connector configuration
	var cfg map[string]string
	var name string
	var err error

	switch {
	case connectURL != "" && connectorName != "":
		client, clientErr := connectapi.NewConnectClient(connectURL, auth)
		if clientErr != nil {
			if connectorConfigPath != "" {
				addRow(r, Row{"connect-api", connectURL, L4, WARN, fmt.Sprintf("Connect client error (falling back to local config): %v", clientErr), ""})
				slog.Debug("Connect API client error, falling back to local config file")
				cfg, name, err = connectapi.LoadConnectorConfigFile(connectorConfigPath)
			} else {
				addRow(r, Row{"connect-api", connectURL, L4, FAIL, fmt.Sprintf("Connect client error: %v", clientErr), ""})
				return
			}
		} else {
			cfg, err = client.GetConnectorConfig(ctx, connectorName)
			name = connectorName
			if err != nil {
				if connectorConfigPath != "" {
					addRow(r, Row{"connect-api", connectURL, HTTP, WARN, fmt.Sprintf("Connect API failed (falling back to local config): %v", err), ""})
					slog.Debug("Connect API failed, falling back to local config", "config_path", connectorConfigPath)
					cfg, name, err = connectapi.LoadConnectorConfigFile(connectorConfigPath)
				} else {
					addRow(r, Row{"connect-api", connectURL, HTTP, FAIL, fmt.Sprintf("Connect API: %v", err), ""})
					return
				}
			}
		}
	case connectorConfigPath != "":
		cfg, name, err = connectapi.LoadConnectorConfigFile(connectorConfigPath)
	}

	if err != nil {
		addRow(r, Row{"connector", connectorConfigPath, DIAG, FAIL, fmt.Sprintf("Config load error: %v", err), ""})
		return
	}
	if cfg == nil {
		return
	}

	// Step 2: Parse connector config
	parsed, parseErr := connectapi.ParseConnectorConfig(name, cfg)
	if parseErr != nil {
		addRow(r, Row{"connector", name, DIAG, FAIL, fmt.Sprintf("Config parse error: %v", parseErr), ""})
		return
	}

	if parsed.Type == connectapi.TypeUnknown {
		addRow(r, Row{"connector", name, DIAG, WARN,
			fmt.Sprintf("Connector class '%s' not supported for probing. Supported: MongoDB, JDBC (DB2, PostgreSQL, MySQL, SQL Server, Oracle), Redis, Elasticsearch", parsed.Class), ""})
		return
	}

	slog.Debug("connector probe", "type", parsed.Type, "host", parsed.Target.Host, "port", parsed.Target.Port, "db", parsed.Target.Database, "tls", parsed.Target.TLS)

	// Print connector probe header
	fmt.Printf("\n=== Connector Probe: %s ===\n", name)
	fmt.Printf("  Type: %s\n", parsed.Class)
	fmt.Printf("  Target: %s:%d\n", parsed.Target.Host, parsed.Target.Port)
	if parsed.Target.Database != "" {
		fmt.Printf("  Database: %s", parsed.Target.Database)
		if parsed.Target.Collection != "" {
			fmt.Printf(" | Collection: %s", parsed.Target.Collection)
		}
		fmt.Println()
	}
	fmt.Println()

	// Step 3: Run the appropriate prober
	var prober probe.Prober
	switch parsed.Type {
	case connectapi.TypeMongoDB:
		prober = probe.NewMongoProber()
	case connectapi.TypeDB2:
		prober = probe.NewDB2Prober()
	case connectapi.TypePostgreSQL:
		prober = probe.NewPostgresProber()
	case connectapi.TypeMySQL:
		prober = probe.NewMySQLProber()
	case connectapi.TypeSQLServer:
		prober = probe.NewSQLServerProber()
	case connectapi.TypeOracle:
		prober = probe.NewOracleProber()
	case connectapi.TypeRedis:
		prober = probe.NewRedisProber()
	case connectapi.TypeElasticsearch:
		prober = probe.NewElasticsearchProber()
	}

	steps := prober.Probe(ctx, parsed.Target)

	// Step 4: Convert ProbeSteps to Report Rows and detect L4 failure for neighborhood scan
	component := fmt.Sprintf("connector-%s", parsed.Type)
	var tcpFailed bool
	for _, step := range steps {
		addRow(r, Row{
			Component: component,
			Target:    fmt.Sprintf("%s:%d", parsed.Target.Host, parsed.Target.Port),
			Layer:     Layer(step.Layer),
			Status:    CheckStatus(step.Status),
			Detail:    step.Detail,
			Hint:      step.Hint,
		})
		if step.Layer == "L4-TCP" && step.Status == probe.StatusFAIL {
			tcpFailed = true
		}
	}

	// Step 4b: Run connector-specific neighborhood scan on TCP failure
	if tcpFailed && parsed.Target.Host != "" {
		ports := connectorNeighborhoodPorts(string(parsed.Type))
		portNeighborhoodResults := portNeighborhoodScan(parsed.Target.Host, parsed.Target.Port, ports, 5*time.Second)
		icmpOK, _ := icmpReachable(parsed.Target.Host)
		diagnosis := classifyRestriction(portNeighborhoodResults, icmpOK)
		var parts []string
		for _, pr := range portNeighborhoodResults {
			status := "OPEN"
			if pr.Status == FAIL {
				status = "BLOCKED"
				if pr.Detail == "refused" {
					status = "REFUSED"
				}
			}
			parts = append(parts, fmt.Sprintf("%d=%s", pr.Port, status))
		}
		hint := fmt.Sprintf("%s [confidence: %s] %s", diagnosis.Classification, diagnosis.Confidence, diagnosis.SuggestedAction)
		addRow(r, Row{"neighborhood", parsed.Target.Host, DIAG, WARN,
			fmt.Sprintf("Connector port scan: %s", strings.Join(parts, ", ")), hint})
	}

	// Step 5: Add redacted connector config to config echo
	redacted := connectapi.RedactConnectorConfig(cfg)
	if r.ConfigEcho == nil {
		r.ConfigEcho = make(map[string]string)
	}
	for k, v := range redacted {
		r.ConfigEcho["connector."+k] = v
	}

	// Clear password from memory (best effort)
	parsed.Target.Password = ""
}
