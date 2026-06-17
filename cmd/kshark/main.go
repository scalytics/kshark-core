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
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/scalytics/kshark-core/internal/connectapi"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// isTTY checks if the given file descriptor is a terminal.
func isTTY(file *os.File) bool {
	fileInfo, err := file.Stat()
	if err != nil {
		return false // Default to false on error
	}
	return (fileInfo.Mode() & os.ModeCharDevice) != 0
}

func animateSharkFin(start <-chan bool, done chan bool) {
	// Wait for the start signal
	<-start

	width := 40
	pos := 0
	direction := 1 // 1 for right, -1 for left

	for {
		select {
		case <-done:
			// Clear the line completely before exiting
			fmt.Print("\r" + strings.Repeat(" ", width+20) + "\r")
			return
		default:
			var builder strings.Builder
			builder.WriteString("\r[")
			for i := 0; i < width; i++ {
				if i == pos {
					builder.WriteString("^") // The shark fin
				} else {
					builder.WriteString("~") // The water
				}
			}
			builder.WriteString("] Scanning...")

			fmt.Print(builder.String())

			// Update position and direction
			pos += direction
			if pos >= width-1 || pos <= 0 {
				direction *= -1 // Reverse direction
			}

			time.Sleep(80 * time.Millisecond)
		}
	}
}

// ---------- Main ----------

func main() {
	fmt.Print(`
 __      _________.__                  __
|  | __ /   _____/|  |__ _____ _______|  | __
|  |/ / \_____  \ |  |  \\__  \\_  __ \  |/ /
|    <  /        \|   Y  \/ __ \|  | \/    <
|__|_ \/_______  /|___|  (____  /__|  |__|_ \
     \/        \/      \/     \/           \/
`)
	// Handle subcommands before flag parsing
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "doctor":
			runDoctor()
			return
		case "diff":
			if len(os.Args) < 4 {
				fmt.Fprintln(os.Stderr, "Usage: kshark diff <report1.json> <report2.json>")
				os.Exit(2)
			}
			runDiff(os.Args[2], os.Args[3])
			return
		case "trend":
			lastN := 10
			if len(os.Args) > 2 {
				if os.Args[2] == "--last" && len(os.Args) > 3 {
					lastN, _ = strconv.Atoi(os.Args[3])
				}
			}
			runTrend(lastN)
			return
		case "validate-blueprint":
			runValidateBlueprint(os.Args[2:])
			return
		}
	}

	propsPath := flag.String("props", "", "Path to client .properties")
	topic := flag.String("topic", "", "Comma-separated list of topics to test (optional for metadata-only)")
	group := flag.String("group", "", "Consumer group for probe (ephemeral by default)")
	jsonOut := flag.String("json", "", "Write JSON report to file")
	analyze := flag.Bool("analyze", false, "Analyze report with AI")
	noAI := flag.Bool("no-ai", false, "Skip AI analysis")
	provider := flag.String("provider", "", "Select AI provider from ai_config.json (e.g., openai, scalytics-connect)")
	timeout := flag.Duration("timeout", 60*time.Second, "Global timeout for the entire scan")
	kafkaTimeout := flag.Duration("kafka-timeout", 10*time.Second, "Timeout for Kafka metadata/dial operations")
	opTimeout := flag.Duration("op-timeout", 10*time.Second, "Timeout for Kafka produce/consume operations")
	produceTimeout := flag.Duration("produce-timeout", 0, "Timeout for Kafka produce operations (overrides -op-timeout)")
	consumeTimeout := flag.Duration("consume-timeout", 0, "Timeout for Kafka consume operations (overrides -op-timeout)")
	balancer := flag.String("balancer", "least", "Partition balancer for probes: least|rr|random")
	startOffset := flag.String("start-offset", "earliest", "Probe read start offset: earliest|latest")
	preset := flag.String("preset", "", "Preset: cc-plain|self-scram")
	diag := flag.Bool("diag", true, "Run traceroute/MTU diagnostics if tools are available")
	probeDirection := flag.String("probe-direction", "up", "Probe direction: up (bottom-up, fail-fast) | full (all layers)")
	neighborhood := flag.Bool("neighborhood", false, "Run port neighborhood scan on TCP failures (auto-enabled on failure when -diag is true)")
	neighborPorts := flag.String("neighborhood-ports", "", "Comma-separated list of ports for neighborhood scan (default: 80,443,9092,9093,9094,8081,8083)")
	logPath := flag.String("log", "", "Write detailed scan log to file (default: reports/kshark-<timestamp>.log)")
	logFormat := flag.String("log-format", "text", "Log output format: text|json")
	yes := flag.Bool("y", false, "Skip interactive confirmation and proceed with the scan")
	showVersion := flag.Bool("version", false, "Show version information and exit")
	// Connector probe flags
	connectURL := flag.String("connect-url", "", "Kafka Connect REST API URL (e.g., https://connect:8083)")
	connectorName := flag.String("connector-name", "", "Connector name to probe via Connect REST API")
	connectorConfig := flag.String("connector-config", "", "Path to local connector config JSON file (fallback)")
	connectAuth := flag.String("connect-basic-auth", "", "user:pass for Connect REST API basic auth (or set KSHARK_CONNECT_AUTH env var)")
	connectBearer := flag.String("connect-bearer-token", "", "Bearer token for Connect REST API auth (or set KSHARK_CONNECT_TOKEN env var)")
	connectCACert := flag.String("connect-ca-cert", "", "CA cert PEM for Connect REST API TLS")
	// Diagnostics bundle flags
	bundle := flag.String("bundle", "", "Create diagnostics bundle (.tar.gz). Optional: specify output path.")
	tfState := flag.String("tf-state", "", "Path to Terraform state file to include in bundle (redacted)")
	tfPlan := flag.String("tf-plan", "", "Path to Terraform plan output to include in bundle (redacted)")
	watch := flag.Duration("watch", 0, "Re-run scan at this interval (e.g., 30s, 5m). 0 = single run.")
	flag.Parse()

	// Environment variable fallback for credentials (avoids credentials in shell history)
	if *connectAuth == "" {
		if envAuth := os.Getenv("KSHARK_CONNECT_AUTH"); envAuth != "" {
			*connectAuth = envAuth
		}
	}
	if *connectBearer == "" {
		if envToken := os.Getenv("KSHARK_CONNECT_TOKEN"); envToken != "" {
			*connectBearer = envToken
		}
	}
	providedFlags := map[string]bool{}
	flag.CommandLine.Visit(func(f *flag.Flag) {
		providedFlags[f.Name] = true
	})

	switch *probeDirection {
	case "up", "full":
		// valid
	default:
		fmt.Fprintf(os.Stderr, "Invalid -probe-direction value %q: must be 'up' or 'full'\n", *probeDirection)
		os.Exit(2)
	}

	if *showVersion {
		fmt.Printf("kshark version %s (commit %s, built %s)\n", version, commit, date)
		return
	}

	if *propsPath == "" && *connectURL == "" && *connectorConfig == "" {
		fmt.Fprintln(os.Stderr, "Usage: kshark -props client.properties [...] OR kshark -connect-url URL -connector-name NAME [...] OR kshark -connector-config FILE [...]")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// Graceful shutdown on SIGINT/SIGTERM -- cancels context, triggering
	// early exit through existing ctx.Done() checks in runScan.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		if sig, ok := <-sigCh; ok {
			slog.Warn("received signal, cancelling scan", "signal", sig)
			cancel()
		}
	}()

	report := &Report{StartedAt: time.Now()}

	if *logPath == "" {
		*logPath = filepath.Join("reports", fmt.Sprintf("kshark-%s.log", time.Now().Format("20060102-150405")))
	}
	logFile, err := initScanLog(*logPath, *logFormat)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open log file: %v\n", err)
	} else if logFile != nil {
		defer logFile.Close()
		absPath, _ := filepath.Abs(*logPath)
		fmt.Printf("Log file: %s\n", absPath)
	}
	slog.Debug("scan start", "props", *propsPath, "timeout", timeout.String(), "kafka_timeout", kafkaTimeout.String(), "op_timeout", opTimeout.String(), "produce_timeout", produceTimeout.String(), "consume_timeout", consumeTimeout.String(), "start_offset", *startOffset, "balancer", *balancer, "diag", *diag, "analyze", *analyze, "json", *jsonOut, "topic", *topic, "group", *group, "preset", *preset)

	var props map[string]string
	if *propsPath != "" {
		var err error
		props, err = loadProperties(*propsPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load properties: %v\n", err)
			os.Exit(1)
		}
		warnInsecurePermissions(*propsPath)
		if *preset != "" {
			applyPreset(*preset, props)
		}
		report.ConfigEcho = redactProps(props)
	} else {
		props = map[string]string{}
		report.ConfigEcho = map[string]string{}
	}

	bootstrap := props["bootstrap.servers"]
	if bootstrap == "" && *connectURL == "" && *connectorConfig == "" {
		fmt.Fprintln(os.Stderr, "bootstrap.servers missing")
		os.Exit(1)
	}
	if bootstrap != "" {
		slog.Debug("bootstrap servers", "servers", bootstrap)
	}

	topics := parseTopics(*topic)
	slog.Debug("topics configured", "topics", topics)

	produceTimeoutVal := *produceTimeout
	consumeTimeoutVal := *consumeTimeout
	if produceTimeoutVal <= 0 {
		produceTimeoutVal = *opTimeout
	}
	if consumeTimeoutVal <= 0 {
		consumeTimeoutVal = *opTimeout
	}
	startOffsetVal, startOffsetErr := parseStartOffset(*startOffset)
	if startOffsetErr != nil {
		fmt.Fprintf(os.Stderr, "Invalid -start-offset value, defaulting to earliest: %v\n", startOffsetErr)
		slog.Debug("start-offset invalid", "value", *startOffset, "err", startOffsetErr)
	}

	report.Run = &RunMeta{
		Args: redactArgs(os.Args),
		Params: map[string]ParamMeta{
			"props":           paramMeta(*propsPath, "", providedFlags["props"]),
			"topic":           paramMeta(*topic, "", providedFlags["topic"]),
			"group":           paramMeta(*group, "", providedFlags["group"]),
			"json":            paramMeta(*jsonOut, "", providedFlags["json"]),
			"analyze":         paramMeta(strconv.FormatBool(*analyze), "false", providedFlags["analyze"]),
			"no-ai":           paramMeta(strconv.FormatBool(*noAI), "false", providedFlags["no-ai"]),
			"provider":        paramMeta(*provider, "", providedFlags["provider"]),
			"timeout":         paramMeta(timeout.String(), "1m0s", providedFlags["timeout"]),
			"kafka-timeout":   paramMeta(kafkaTimeout.String(), "10s", providedFlags["kafka-timeout"]),
			"op-timeout":      paramMeta(opTimeout.String(), "10s", providedFlags["op-timeout"]),
			"produce-timeout": paramMeta(produceTimeoutVal.String(), "inherit op-timeout", providedFlags["produce-timeout"]),
			"consume-timeout": paramMeta(consumeTimeoutVal.String(), "inherit op-timeout", providedFlags["consume-timeout"]),
			"start-offset":    paramMeta(*startOffset, "earliest", providedFlags["start-offset"]),
			"balancer":        paramMeta(*balancer, "least", providedFlags["balancer"]),
			"preset":          paramMeta(*preset, "", providedFlags["preset"]),
			"diag":            paramMeta(strconv.FormatBool(*diag), "true", providedFlags["diag"]),
			"probe-direction": paramMeta(*probeDirection, "up", providedFlags["probe-direction"]),
			"neighborhood":    paramMeta(strconv.FormatBool(*neighborhood), "false", providedFlags["neighborhood"]),
			"log":             paramMeta(*logPath, "", providedFlags["log"]),
			"log-format":      paramMeta(*logFormat, "text", providedFlags["log-format"]),
			"y":               paramMeta(strconv.FormatBool(*yes), "false", providedFlags["y"]),
			"version":         paramMeta(strconv.FormatBool(*showVersion), "false", providedFlags["version"]),
			"watch":           paramMeta(watch.String(), "0s", providedFlags["watch"]),
		},
	}

	// Peek at AI config for scan plan display
	var aiProviderName, aiModel string
	if *analyze && !*noAI {
		if aiCfg, err := loadAIConfig(); err == nil {
			pName := aiCfg.DefaultProvider
			if *provider != "" {
				pName = *provider
			}
			if pCfg, ok := aiCfg.Providers[pName]; ok {
				if !strings.HasPrefix(pCfg.APIKey, "YOUR_") && pCfg.APIKey != "" {
					aiProviderName = pName
					aiModel = pCfg.Model
				}
			}
		}
	}

	// Print the plan and wait for confirmation if not running in non-interactive mode
	printScanPlan(props, topics, *diag, *analyze, *jsonOut, aiProviderName, aiModel)

	if !*yes && *watch <= 0 {
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("\nContinue with the scan? (y/n): ")
			input, _ := reader.ReadString('\n')
			input = strings.ToLower(strings.TrimSpace(input))
			if input == "y" || input == "yes" {
				break
			}
			if input == "n" || input == "no" {
				fmt.Println("Scan aborted by user.")
				os.Exit(0)
			}
		}
	}

	// Build the scanConfig once; reused in watch mode
	cfg := scanConfig{
		props:          props,
		bootstrap:      bootstrap,
		topics:         topics,
		diag:           *diag,
		group:          *group,
		kafkaTimeout:   *kafkaTimeout,
		produceTimeout: produceTimeoutVal,
		consumeTimeout: consumeTimeoutVal,
		balancer:       *balancer,
		startOffset:    startOffsetVal,
		connectURL:     *connectURL,
		connectorName:  *connectorName,
		connectorCfg:   *connectorConfig,
		connectAuth: connectapi.ConnectAuthOpts{
			BasicAuth:   *connectAuth,
			BearerToken: *connectBearer,
			CACertPath:  *connectCACert,
		},
		probeDirection: *probeDirection,
		neighborhood:   *neighborhood,
		neighborPorts:  *neighborPorts,
	}

	// ---- Watch mode ----
	if *watch > 0 {
		// Set up a dedicated signal channel for watch-mode graceful stop
		watchCtx, watchCancel := context.WithCancel(context.Background())
		defer watchCancel()

		watchSig := make(chan os.Signal, 1)
		signal.Notify(watchSig, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			if sig, ok := <-watchSig; ok {
				slog.Warn("received signal in watch mode, stopping after current scan", "signal", sig)
				watchCancel()
			}
		}()

		iteration := 1
		for {
			// Clear screen
			fmt.Print("\033[2J\033[H")
			nextRun := time.Now().Add(*watch)
			fmt.Printf("kshark watch — iteration %d  |  interval %s  |  next run ~%s\n",
				iteration, watch.String(), nextRun.Format("15:04:05"))
			fmt.Println(strings.Repeat("─", 72))

			// Create a fresh report and per-iteration context
			iterReport := &Report{StartedAt: time.Now()}
			iterReport.ConfigEcho = report.ConfigEcho
			iterReport.Run = report.Run

			iterCtx, iterCancel := context.WithTimeout(watchCtx, *timeout)

			// Prepare and start scan animation
			var startAnim, doneAnim chan bool
			if isTTY(os.Stdout) {
				startAnim = make(chan bool)
				doneAnim = make(chan bool)
				go animateSharkFin(startAnim, doneAnim)
				startAnim <- true
			}

			scanStart := time.Now()
			runScan(iterCtx, iterReport, cfg)
			iterCancel()

			if doneAnim != nil {
				doneAnim <- true
			}

			iterReport.FinishedAt = time.Now()
			summarize(iterReport)
			printPretty(iterReport)

			// Save JSON with incrementing suffix if -json is set
			if *jsonOut != "" {
				ext := filepath.Ext(*jsonOut)
				base := strings.TrimSuffix(*jsonOut, ext)
				watchFile := fmt.Sprintf("%s_%03d%s", base, iteration, ext)
				actualPath, writeErr := writeJSON(watchFile, iterReport)
				if writeErr != nil {
					fmt.Fprintf(os.Stderr, "Error writing JSON report: %v\n", writeErr)
				} else {
					absPath, _ := filepath.Abs(actualPath)
					fmt.Printf("JSON report written to %s\n", absPath)
				}
			}

			// Check if context was cancelled (SIGINT received)
			select {
			case <-watchCtx.Done():
				fmt.Printf("\nWatch mode stopped after iteration %d.\n", iteration)
				if iterReport.HasFailed {
					os.Exit(layeredExitCode(iterReport))
				}
				return
			default:
			}

			// Sleep for remaining interval (skip if scan took longer)
			elapsed := time.Since(scanStart)
			remaining := *watch - elapsed
			if remaining > 0 {
				fmt.Printf("\nNext scan in %s (at %s) — press Ctrl+C to stop.\n",
					formatDuration(remaining), time.Now().Add(remaining).Format("15:04:05"))
				select {
				case <-time.After(remaining):
				case <-watchCtx.Done():
					fmt.Printf("\nWatch mode stopped after iteration %d.\n", iteration)
					if iterReport.HasFailed {
						os.Exit(layeredExitCode(iterReport))
					}
					return
				}
			} else {
				fmt.Println("\nScan took longer than interval, starting next iteration immediately.")
			}

			iteration++
		}
	}

	// ---- Single-run mode (original behavior) ----

	// Prepare and start scan animation in the background if stdout is a TTY
	var startAnimation, doneAnimation chan bool
	if isTTY(os.Stdout) {
		startAnimation = make(chan bool)
		doneAnimation = make(chan bool)
		go animateSharkFin(startAnimation, doneAnimation)
		startAnimation <- true // Signal the animation to start
	}

	// Run the scan
	runScan(ctx, report, cfg)

	// Stop scan animation if it was started
	if doneAnimation != nil {
		doneAnimation <- true
	}

	report.FinishedAt = time.Now()
	slog.Debug("scan finished", "duration", report.FinishedAt.Sub(report.StartedAt), "failed", report.HasFailed)
	summarize(report)
	printPretty(report)

	// Determine the reports directory (same location as JSON report if specified)
	reportsDir := "reports"
	if *jsonOut != "" {
		reportsDir = filepath.Dir(*jsonOut)
	}

	analysisMeta := &AnalysisMeta{}
	systemPrompt, userPrompt, err := buildAnalysisPrompt(report)
	if err != nil {
		analysisMeta.ResponseStatus = "error"
		analysisMeta.ResponseError = fmt.Sprintf("build prompt: %v", err)
	} else {
		mdPath, err := writeAnalysisPromptMD(systemPrompt, userPrompt, reportsDir)
		if err != nil {
			analysisMeta.ResponseStatus = "error"
			analysisMeta.ResponseError = fmt.Sprintf("write prompt: %v", err)
		} else {
			analysisMeta.PromptFile = mdPath
			if h, err := fileSHA256(mdPath); err == nil {
				analysisMeta.PromptSHA256 = h
			}
			if c, err := readFileContent(mdPath); err == nil {
				analysisMeta.PromptContent = c
			}
			absPath, _ := filepath.Abs(mdPath)
			fmt.Printf("\nAnalysis prompt saved to: %s\n", absPath)
			slog.Debug("analysis prompt", "path", mdPath, "sha256", analysisMeta.PromptSHA256)
		}

		if *analyze && !*noAI {
			aiConfig, err := loadAIConfig()
			needsFallback := false
			var fallbackReason string

			if err != nil {
				needsFallback = true
				fallbackReason = fmt.Sprintf("AI config not available: %v", err)
			}

			var providerName string
			var providerConfig AIProviderConfig
			if !needsFallback {
				providerName = aiConfig.DefaultProvider
				if *provider != "" {
					providerName = *provider
				}

				var ok bool
				providerConfig, ok = aiConfig.Providers[providerName]
				if !ok {
					needsFallback = true
					fallbackReason = fmt.Sprintf("AI provider '%s' not found in ai_config.json", providerName)
				} else if strings.HasPrefix(providerConfig.APIKey, "YOUR_") || providerConfig.APIKey == "" {
					needsFallback = true
					fallbackReason = fmt.Sprintf("API key for provider '%s' is not configured", providerName)
				}
			}

			if needsFallback {
				analysisMeta.ResponseStatus = "skipped"
				analysisMeta.ResponseError = fallbackReason
				fmt.Fprintf(os.Stderr, "\nNote: %s\n", fallbackReason)
			} else {
				analysisMeta.Provider = providerName
				analysisMeta.Model = providerConfig.Model
				aiClient := NewAIClient(&providerConfig)
				fmt.Printf("\nSubmitting report for AI analysis using provider '%s'...\n", providerName)
				analysis, err := aiClient.AnalyzeReport(ctx, systemPrompt, userPrompt)
				if err != nil {
					analysisMeta.ResponseStatus = "error"
					analysisMeta.ResponseError = err.Error()
					fmt.Fprintf(os.Stderr, "Error during AI analysis: %v\n", err)
				} else {
					analysisMeta.ResponseStatus = "ok"
					if respPath, err := writeAnalysisResponseJSON(analysis, reportsDir); err == nil {
						analysisMeta.ResponseFile = respPath
						if h, err := fileSHA256(respPath); err == nil {
							analysisMeta.ResponseSHA256 = h
						}
						if c, err := readFileContent(respPath); err == nil {
							analysisMeta.ResponseContent = c
						}
						slog.Debug("analysis response", "path", respPath, "sha256", analysisMeta.ResponseSHA256)
					} else {
						analysisMeta.ResponseError = fmt.Sprintf("write response: %v", err)
					}

					fmt.Println("\n--- AI Analysis ---")
					printIllustrativeAnalysis(analysis)
					fmt.Println("-------------------")

					reportPath, err := writeHTMLReport(report, analysis)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Error writing HTML report: %v\n", err)
					} else {
						absPath, _ := filepath.Abs(reportPath)
						fmt.Printf("\nAI analysis report written to %s\n", absPath)
					}
				}
			}
		} else {
			analysisMeta.ResponseStatus = "skipped"
			analysisMeta.ResponseError = "AI analysis disabled"
		}
	}
	if analysisMeta.PromptFile != "" || analysisMeta.ResponseStatus != "" {
		report.Analysis = analysisMeta
	}

	if report.Artifacts == nil {
		report.Artifacts = &ArtifactsMeta{}
	}
	if *logPath != "" {
		if logFile != nil {
			_ = logFile.Sync()
		}
		report.Artifacts.LogFile = *logPath
		if h, err := fileSHA256(*logPath); err == nil {
			report.Artifacts.LogSHA256 = h
		}
		if c, err := readFileContent(*logPath); err == nil {
			report.Artifacts.LogContent = c
		}
	}
	if analysisMeta.PromptFile != "" {
		report.Artifacts.PromptFile = analysisMeta.PromptFile
		report.Artifacts.PromptSHA256 = analysisMeta.PromptSHA256
		report.Artifacts.PromptContent = analysisMeta.PromptContent
	}

	if *jsonOut != "" {
		actualPath, err := writeJSON(*jsonOut, report)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error writing JSON report: %v\n", err)
			os.Exit(1)
		}
		absPath, _ := filepath.Abs(actualPath)
		fmt.Printf("JSON report written to %s\n", absPath)
		if checksumPath, checksum, err := writeReportChecksum(actualPath); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing report checksum: %v\n", err)
		} else {
			fmt.Printf("JSON report checksum saved to %s\n", checksumPath)
			slog.Debug("report checksum", "path", checksumPath, "sha256", checksum)
		}
	}

	// Create diagnostics bundle if requested
	if *bundle != "" || *tfState != "" {
		bundleOut := *bundle
		if bundleOut == "" {
			bundleOut = "" // createBundle will generate a default name
		}
		bundlePath, err := createBundle(report, *logPath, *jsonOut, *tfState, *tfPlan, bundleOut)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating diagnostics bundle: %v\n", err)
		} else {
			printExportCommands(bundlePath)
		}
	}

	if report.HasFailed {
		os.Exit(layeredExitCode(report))
	}
}

// layeredExitCode returns an exit code encoding the lowest failing layer.
// This enables CI/CD pipelines to branch based on the failure type:
//
//	1 = L3/DNS failure
//	2 = L4/TCP failure
//	3 = L5-6/TLS failure
//	4 = L7/Application failure (Kafka, HTTP, connector)
//	5 = Diagnostic failure only (timeout, MTU, etc.)
//	1 = fallback if no specific layer detected
func layeredExitCode(r *Report) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	lowestLayer := 99
	for _, row := range r.Rows {
		if row.Status != FAIL {
			continue
		}
		code := 5 // default: diagnostic
		switch row.Layer {
		case L3:
			code = 1
		case L4:
			code = 2
		case L56:
			code = 3
		case L7, HTTP:
			code = 4
		case DIAG:
			code = 5
		}
		if code < lowestLayer {
			lowestLayer = code
		}
	}
	if lowestLayer == 99 {
		return 1 // fallback
	}
	return lowestLayer
}
