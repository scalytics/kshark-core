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
	"os/exec"
	"runtime"
	"strings"
)

// ---------- Doctor Subcommand ----------

type toolCheck struct {
	Name     string
	Purpose  string
	Required bool
}

var diagnosticTools = []toolCheck{
	{"traceroute", "Network path analysis", false},
	{"tracepath", "Path MTU discovery (Linux)", false},
	{"tracert", "Network path analysis (Windows)", false},
	{"ping", "ICMP reachability + MTU probing", true},
	{"dig", "DNS diagnostics", false},
	{"nslookup", "DNS resolution", false},
	{"openssl", "TLS certificate inspection", false},
	{"curl", "HTTP endpoint testing", false},
}

// runDoctor checks if diagnostic tools are available and reports their status.
func runDoctor() {
	fmt.Println("\nkshark doctor — Diagnostic Tool Check")
	fmt.Println(strings.Repeat("─", 55))
	fmt.Printf("%-15s %-8s %s\n", "Tool", "Status", "Purpose")
	fmt.Println(strings.Repeat("─", 55))

	missing := 0
	missingRequired := 0

	for _, tool := range diagnosticTools {
		path, err := exec.LookPath(tool.Name)
		status := ColorGreen + "OK" + ColorReset
		if err != nil {
			if tool.Required {
				status = ColorRed + "MISSING" + ColorReset
				missingRequired++
			} else {
				status = ColorYellow + "not found" + ColorReset
			}
			missing++
			path = ""
		}

		label := tool.Name
		if tool.Required {
			label += " *"
		}
		fmt.Printf("%-15s %-18s %s\n", label, status, tool.Purpose)
		if path != "" {
			fmt.Printf("%-15s %s\n", "", path)
		}
	}

	fmt.Println(strings.Repeat("─", 55))
	fmt.Printf("Platform: %s/%s  |  Go: %s\n", runtime.GOOS, runtime.GOARCH, runtime.Version())
	fmt.Printf("kshark version: %s (commit %s)\n", version, commit)

	if missing == 0 {
		fmt.Println("\n" + ColorGreen + "All diagnostic tools available." + ColorReset)
	} else {
		fmt.Printf("\n%d tool(s) not found.", missing)
		if missingRequired > 0 {
			fmt.Printf(" %s%d required tool(s) missing!%s", ColorRed, missingRequired, ColorReset)
		}
		fmt.Println()
		fmt.Println("Install missing tools for full diagnostic capability.")
	}
	fmt.Println("* = required for core functionality")
}
