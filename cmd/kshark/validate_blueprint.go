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
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"
)

// ---------- validate-blueprint subcommand (SPEC-14 section 5.1) ----------
//
// validate-blueprint loads a per-blueprint validation routine, runs each check
// through the existing kshark check engine (the same L3-to-L7 code that scan
// uses), binds each check to its CLAIMS_MAP claim, stamps the bvb object onto
// the report, and emits JSON plus the bundle. It reaches Kafka only via the
// bootstrap endpoint in -props, which is the ADR-0002 proxy entrypoint.

// runValidateBlueprint is the entrypoint dispatched from main for the
// `validate-blueprint` verb. argv is os.Args[2:].
func runValidateBlueprint(argv []string) {
	fs := flag.NewFlagSet("validate-blueprint", flag.ExitOnError)
	bp := fs.String("bp", "", "Blueprint id (e.g. bp-006-citizen360)")
	routinePath := fs.String("routine", "", "Path to validation-routine.yaml")
	propsPath := fs.String("props", "", "Path to client .properties (bootstrap = the kafSCALE proxy)")
	digestsPath := fs.String("digests", "", "Path to a JSON file of frozen image digests (optional)")
	backendID := fs.String("backend", "", "Backend family override: kafscale|confluent|redpanda|msk (optional)")
	outPath := fs.String("out", "", "Write the BVB report JSON to this path (optional)")
	bundlePath := fs.String("bundle", "", "Create the BVB .tar.gz at this path (optional)")
	kafkaTimeout := fs.Duration("kafka-timeout", 10*time.Second, "Timeout for Kafka metadata/dial operations")
	opTimeout := fs.Duration("op-timeout", 10*time.Second, "Timeout for Kafka produce/consume operations")
	tier := fs.String("tier", "", "Active smoke tier; needs-headroom checks excluded when this is 'core' (or SMOKE_TIER)")
	logPath := fs.String("log", "", "Write detailed log to file")
	logFormat := fs.String("log-format", "text", "Log output format: text|json")
	if err := fs.Parse(argv); err != nil {
		os.Exit(2)
	}

	if *routinePath == "" || *propsPath == "" {
		fmt.Fprintln(os.Stderr, "Usage: kshark validate-blueprint --bp <id> --routine <file> -props <client.properties> [--digests <file>] [--backend <id>] [--out <bundle.json>] [--bundle <bvb.tar.gz>]")
		os.Exit(2)
	}

	if *logPath == "" {
		*logPath = fmt.Sprintf("reports/kshark-bvb-%s.log", time.Now().Format("20060102-150405"))
	}
	logFile, err := initScanLog(*logPath, *logFormat)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open log file: %v\n", err)
	} else if logFile != nil {
		defer logFile.Close()
	}

	routine, err := loadRoutine(*routinePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading routine: %v\n", err)
		os.Exit(1)
	}

	props, err := loadProperties(*propsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load properties: %v\n", err)
		os.Exit(1)
	}
	warnInsecurePermissions(*propsPath)
	bootstrap := strings.TrimSpace(props["bootstrap.servers"])
	if bootstrap == "" {
		fmt.Fprintln(os.Stderr, "bootstrap.servers missing in properties")
		os.Exit(1)
	}

	digests, err := loadImageDigests(*digestsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading digests: %v\n", err)
		os.Exit(1)
	}

	// Tier: --tier flag, else SMOKE_TIER env, else core.
	activeTier := *tier
	if activeTier == "" {
		activeTier = os.Getenv("SMOKE_TIER")
	}
	if activeTier == "" {
		activeTier = TierCore
	}

	backendFamily := routine.Backend.Family
	if *backendID != "" {
		backendFamily = *backendID
	}
	if backendFamily == "" {
		backendFamily = "kafscale"
	}

	report := &Report{StartedAt: time.Now()}
	report.ConfigEcho = redactProps(props)

	rc := runnerConfig{
		bootstrap:    bootstrap,
		props:        props,
		kafkaTimeout: *kafkaTimeout,
		opTimeout:    *opTimeout,
		activeTier:   activeTier,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	bvb := &BVB{
		BlueprintID:     firstNonEmpty(*bp, routine.BlueprintID),
		ManifestVersion: routine.ManifestVersion,
		RoutineID:       routine.RoutineID,
		RoutineVersion:  routine.RoutineVersion,
		Backend:         BVBBackend{Family: backendFamily, Version: routine.Backend.Version},
		ImageDigests:    digests,
		Environment:     captureBVBEnv(bootstrap),
		StartedAt:       report.StartedAt,
	}

	for _, check := range routine.Checks {
		result := runRoutineCheck(ctx, report, rc, check)
		bvb.Checks = append(bvb.Checks, result)
	}

	bvb.FinishedAt = time.Now()
	bvb.Verdict = computeVerdict(bvb.Checks)

	report.FinishedAt = bvb.FinishedAt
	report.BVB = bvb
	summarize(report)

	printBVBSummary(bvb)

	// Reuse the existing HTML render (decoupled from the AI path: pass nil
	// analysis so the verdict + checks render unconditionally).
	if reportPath, err := writeHTMLReport(report, nil); err != nil {
		fmt.Fprintf(os.Stderr, "Note: HTML render skipped: %v\n", err)
	} else {
		fmt.Printf("HTML report written to %s\n", reportPath)
	}

	if *outPath != "" {
		if actual, err := writeJSON(*outPath, report); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing BVB JSON: %v\n", err)
			os.Exit(1)
		} else {
			fmt.Printf("BVB report written to %s\n", actual)
			if csPath, cs, err := writeReportChecksum(actual); err == nil {
				fmt.Printf("BVB report checksum saved to %s (%s)\n", csPath, cs)
			}
		}
	}

	if *bundlePath != "" {
		if path, err := createBundle(report, *logPath, *outPath, "", "", *bundlePath); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating BVB bundle: %v\n", err)
		} else {
			printExportCommands(path)
		}
	}

	if bvb.Verdict.Status != VerdictGreen {
		os.Exit(1)
	}
}

// runnerConfig holds the parameters every routine check needs.
type runnerConfig struct {
	bootstrap    string
	props        map[string]string
	kafkaTimeout time.Duration
	opTimeout    time.Duration
	activeTier   string
}

// runRoutineCheck runs one routine check via the existing kshark functions,
// then folds the rows it produced into a claim-bound BVBCheck. Rows are
// captured by snapshotting the report length before and after the check, so
// each BVBCheck back-references exactly the rows that produced it.
func runRoutineCheck(ctx context.Context, report *Report, rc runnerConfig, check RoutineCheck) BVBCheck {
	res := BVBCheck{
		ID:    check.ID,
		Kind:  check.Kind,
		Claim: check.Claim,
		Tier:  check.Tier,
	}

	// A needs-headroom check is excluded when the active tier is core.
	if check.Tier == TierNeedsHeadroom && rc.activeTier == TierCore {
		res.Status = CheckSKIP
		res.SkipReason = tierExcludedPrefix + " @needs-headroom"
		return res
	}

	start := time.Now()
	report.mu.Lock()
	rowsBefore := len(report.Rows)
	report.mu.Unlock()

	switch check.Kind {
	case KindConnectivity:
		runConnectivityCheck(report, rc)
	case KindProduceConsume:
		topic := stringParam(check.Params, "topic")
		group := stringParam(check.Params, "group")
		startOffset, _ := parseStartOffset("earliest")
		probeProduceConsume(ctx, report, rc.props, rc.bootstrap, topic, group,
			rc.opTimeout, rc.opTimeout, "least", rc.kafkaTimeout, startOffset)
	case KindTopicPresence:
		for _, t := range stringSliceParam(check.Params, "topics") {
			checkTopic(report, rc.props, firstBootstrap(rc.bootstrap), t, rc.kafkaTimeout)
		}
	case KindSchemaPresence:
		runSchemaPresenceCheck(ctx, report, rc, stringSliceParam(check.Params, "subjects"))
	case KindComponentHealth:
		runComponentHealthCheck(ctx, report, check)
	case KindHTTPCheck:
		runHTTPCheck(ctx, report, check)
	}

	report.mu.Lock()
	newRows := append([]Row(nil), report.Rows[rowsBefore:]...)
	report.mu.Unlock()

	res.DurationMs = time.Since(start).Milliseconds()
	res.Status, res.Evidence, res.Rows = foldRows(newRows)
	return res
}

// foldRows maps the rows a check produced into a BVB status + evidence list +
// row back-references. OK and WARN fold to PASS (the warning is preserved in
// evidence); any FAIL is FAIL; no rows folds to SKIP.
func foldRows(rows []Row) (BVBCheckStatus, []string, []string) {
	if len(rows) == 0 {
		return CheckSKIP, []string{"no observable signal produced"}, nil
	}
	status := CheckPASS
	var evidence []string
	var refs []string
	for _, row := range rows {
		refs = append(refs, fmt.Sprintf("%s:%s", row.Layer, row.Target))
		switch row.Status {
		case FAIL:
			status = CheckFAIL
			evidence = append(evidence, fmt.Sprintf("%s %s: %s", row.Component, row.Target, row.Detail))
		case WARN:
			evidence = append(evidence, fmt.Sprintf("warn: %s %s: %s", row.Component, row.Target, row.Detail))
		case OK:
			evidence = append(evidence, fmt.Sprintf("%s %s: %s", row.Component, row.Target, row.Detail))
		case SKIP:
			evidence = append(evidence, fmt.Sprintf("skip: %s %s: %s", row.Component, row.Target, row.Detail))
		}
	}
	return status, evidence, refs
}

// runConnectivityCheck formalizes ADR-0002 GATE 2: ApiVersions against the
// proxy bootstrap, then broker discovery via cluster metadata.
func runConnectivityCheck(report *Report, rc runnerConfig) {
	addr := firstBootstrap(rc.bootstrap)
	conn, err := kafkaConn(report, rc.props, addr, rc.kafkaTimeout)
	if err != nil {
		return
	}
	conn.Close()
	brokers := strings.Split(rc.bootstrap, ",")
	brokerDiscoveryScan(report, rc.props, brokers, rc.kafkaTimeout)
}

// runSchemaPresenceCheck asserts each subject exists via GET <sr>/subjects.
// It reuses checkSchemaRegistry's transport conventions through a small probe.
func runSchemaPresenceCheck(ctx context.Context, report *Report, rc runnerConfig, subjects []string) {
	srURL := strings.TrimSpace(rc.props["schema.registry.url"])
	if srURL == "" {
		addRow(report, Row{"schema-reg", "(no schema.registry.url)", HTTP, FAIL,
			"schema-presence check needs schema.registry.url in -props", "Set schema.registry.url."})
		return
	}
	// First, validate the registry is reachable at all (reuses existing check).
	checkSchemaRegistry(ctx, report, rc.props)
	// Then probe each subject explicitly.
	for _, subject := range subjects {
		probeSubject(ctx, report, rc.props, srURL, subject)
	}
}

// probeSubject does a GET <sr>/subjects/<subject>/versions and records the
// result. It honors the same SSRF + TLS conventions as checkSchemaRegistry.
func probeSubject(ctx context.Context, report *Report, props map[string]string, srURL, subject string) {
	if _, err := isAllowedURL(srURL); err != nil {
		addRow(report, Row{"schema-reg", subject, HTTP, FAIL,
			fmt.Sprintf("URL blocked (SSRF protection): %v", err), ""})
		return
	}
	host := extractHost(srURL)
	tlsConf, _, err := tlsConfigFromProps(props, host)
	if err != nil {
		addRow(report, Row{"schema-reg", subject, HTTP, FAIL, fmt.Sprintf("TLS config err: %v", err), ""})
		return
	}
	client := httpClientFromTLS(tlsConf, 8*time.Second)
	url := strings.TrimRight(srURL, "/") + "/subjects/" + subject + "/versions"
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	if info := props["basic.auth.user.info"]; info != "" {
		up := strings.SplitN(info, ":", 2)
		if len(up) == 2 {
			req.SetBasicAuth(up[0], up[1])
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		addRow(report, Row{"schema-reg", subject, HTTP, FAIL, fmt.Sprintf("GET subject failed: %v", err), "TLS/host/network or auth."})
		return
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		addRow(report, Row{"schema-reg", subject, HTTP, OK, "subject registered", ""})
	case 404:
		addRow(report, Row{"schema-reg", subject, HTTP, FAIL, "subject not found (404)", "Register the value schema for this topic."})
	case 401, 403:
		addRow(report, Row{"schema-reg", subject, HTTP, FAIL, fmt.Sprintf("Auth %d", resp.StatusCode), "Check basic.auth.user.info or mTLS mapping."})
	default:
		addRow(report, Row{"schema-reg", subject, HTTP, WARN, fmt.Sprintf("HTTP %d", resp.StatusCode), ""})
	}
}

// runComponentHealthCheck probes each named component's health endpoint. The
// per-component endpoint and probe are resolved from the routine params: a
// `probes` map keyed by component id (each {url|url_env, path}). A component
// without a resolvable probe target is recorded as SKIP-shaped (no row), which
// the verdict reads only when no params resolve.
func runComponentHealthCheck(ctx context.Context, report *Report, check RoutineCheck) {
	probes, _ := check.Params["probes"].(map[string]any)
	for _, comp := range stringSliceParam(check.Params, "components") {
		target := ""
		path := "/healthz"
		if probes != nil {
			if pm, ok := probes[comp].(map[string]any); ok {
				target = resolveURL(pm)
				if p := mapStringParam(pm, "path"); p != "" {
					path = p
				}
			}
		}
		if target == "" {
			addRow(report, Row{comp, "(no probe target)", HTTP, WARN,
				"component-health probe target not configured", "Add a probes entry (url or url_env) for this component."})
			continue
		}
		httpGet(ctx, report, comp, target, path, 200)
	}
}

// runHTTPCheck does a generic GET against the routine-named URL and asserts the
// expected status (default 200).
func runHTTPCheck(ctx context.Context, report *Report, check RoutineCheck) {
	target := stringParam(check.Params, "url")
	path := stringParam(check.Params, "path")
	if target == "" {
		if envName := stringParam(check.Params, "url_env"); envName != "" {
			target = os.Getenv(envName)
		}
	}
	if http, ok := check.Params["http"].(map[string]any); ok && target == "" {
		target = resolveURL(http)
		if p := mapStringParam(http, "path"); p != "" {
			path = p
		}
	}
	if target == "" {
		addRow(report, Row{check.ID, "(no url)", HTTP, FAIL, "http-check has no resolvable url", "Set url, or url_env pointing at a set env var."})
		return
	}
	httpGet(ctx, report, check.ID, target, path, 200)
}

// ssrfCheck is the SSRF allowlist gate used by the generic HTTP probe. It is a
// package var so tests can point it at a loopback httptest server; production
// always uses isAllowedURL, which hard-blocks loopback/link-local/metadata.
var ssrfCheck = isAllowedURL

// httpGet is the shared generic HTTP probe used by component-health and
// http-check. It honors the SSRF allowlist.
func httpGet(ctx context.Context, report *Report, component, baseURL, path string, wantStatus int) {
	if _, err := ssrfCheck(baseURL); err != nil {
		addRow(report, Row{component, baseURL, HTTP, FAIL, fmt.Sprintf("URL blocked (SSRF protection): %v", err), ""})
		return
	}
	url := strings.TrimRight(baseURL, "/")
	if path != "" {
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		url += path
	}
	client := &http.Client{Timeout: 8 * time.Second}
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := client.Do(req)
	if err != nil {
		addRow(report, Row{component, url, HTTP, FAIL, fmt.Sprintf("GET failed: %v", err), "Check the component is reachable and healthy."})
		return
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if resp.StatusCode == wantStatus {
		addRow(report, Row{component, url, HTTP, OK, fmt.Sprintf("GET %s -> %d", path, resp.StatusCode), ""})
		return
	}
	addRow(report, Row{component, url, HTTP, FAIL, fmt.Sprintf("GET %s -> %d (want %d)", path, resp.StatusCode, wantStatus), ""})
}

// resolveURL reads a url, or a url_env naming an env var, from a probe map.
func resolveURL(m map[string]any) string {
	if u := mapStringParam(m, "url"); u != "" {
		return u
	}
	if envName := mapStringParam(m, "url_env"); envName != "" {
		return os.Getenv(envName)
	}
	return ""
}

// captureBVBEnv records the run environment. The disk fsync floor note is
// included only when running on Linux (the etcd floor concern), kept cheap.
func captureBVBEnv(bootstrap string) BVBEnv {
	env := BVBEnv{
		Arch:               runtime.GOARCH,
		AdvertisedEndpoint: bootstrap,
	}
	if host, err := os.Hostname(); err == nil {
		env.Host = host
	}
	if runtime.GOOS == "linux" {
		env.DiskFsyncFloorNote = "etcd fsync floor is 30 MB/s for 4k O_DSYNC writes; below it the broker leadership view lags etcd"
	}
	return env
}

// loadImageDigests reads a JSON file of {image, digest, platforms} entries.
// An empty path yields an empty list (the MVP takes digests via --digests;
// registry auto-capture is deferred).
func loadImageDigests(path string) ([]ImageDigest, error) {
	if path == "" {
		return []ImageDigest{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read digests: %w", err)
	}
	var digests []ImageDigest
	if err := json.Unmarshal(data, &digests); err != nil {
		return nil, fmt.Errorf("parse digests json: %w", err)
	}
	return digests, nil
}

func printBVBSummary(bvb *BVB) {
	color := ColorGreen
	if bvb.Verdict.Status != VerdictGreen {
		color = ColorRed
	}
	fmt.Printf("\nBlueprint-Validation-Bundle: %s (routine %s@%s, backend %s)\n",
		bvb.BlueprintID, bvb.RoutineID, bvb.RoutineVersion, bvb.Backend.Family)
	fmt.Println(strings.Repeat("─", 72))
	for _, c := range bvb.Checks {
		icon := IconOK
		switch c.Status {
		case CheckFAIL:
			icon = IconFail
		case CheckSKIP:
			icon = IconSkip
		}
		line := fmt.Sprintf("%s %-18s %-16s %s", icon, c.ID, c.Kind, c.Status)
		if c.SkipReason != "" {
			line += "  (" + c.SkipReason + ")"
		}
		fmt.Println(line)
	}
	fmt.Println(strings.Repeat("─", 72))
	fmt.Printf("%sVerdict: %s%s  (PASS:%d FAIL:%d SKIP:%d TIER-EXCLUDED:%d of %d)\n",
		color, bvb.Verdict.Status, ColorReset,
		bvb.Verdict.Pass, bvb.Verdict.Fail, bvb.Verdict.Skip, bvb.Verdict.TierExcluded, bvb.Verdict.Total)
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func firstBootstrap(bootstrap string) string {
	return strings.TrimSpace(strings.Split(bootstrap, ",")[0])
}
