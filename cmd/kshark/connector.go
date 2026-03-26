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
			fmt.Sprintf("Connector class '%s' not supported for probing. Supported: MongoDB, JDBC (DB2, PostgreSQL)", parsed.Class), ""})
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
	}

	steps := prober.Probe(ctx, parsed.Target)

	// Step 4: Convert ProbeSteps to Report Rows
	component := fmt.Sprintf("connector-%s", parsed.Type)
	for _, step := range steps {
		addRow(r, Row{
			Component: component,
			Target:    fmt.Sprintf("%s:%d", parsed.Target.Host, parsed.Target.Port),
			Layer:     Layer(step.Layer),
			Status:    CheckStatus(step.Status),
			Detail:    step.Detail,
			Hint:      step.Hint,
		})
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
