// Copyright 2024 Your Name or Company
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
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"regexp"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	// Kerberos/GSSAPI is optional. Enable with:  go build -tags kerberos
	// and ensure the module resolves on your platform. If you don't use it, you can remove the import & code blocks guarded by build tags.
	// _ "github.com/segmentio/kafka-go/sasl/gssapi"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// ---------- AI Analysis ----------

type AIProviderConfig struct {
	APIEndpoint string `json:"api_endpoint"`
	APIKey      string `json:"api_key"`
	Model       string `json:"model"`
}

type AIConfig struct {
	DefaultProvider string                      `json:"default_provider"`
	Providers       map[string]AIProviderConfig `json:"providers"`
}

func loadAIConfig() (*AIConfig, error) {
	f, err := os.Open("ai_config.json")
	if err != nil {
		return nil, fmt.Errorf("could not open ai_config.json: %w. Please ensure it exists", err)
	}
	defer f.Close()

	var config AIConfig
	if err := json.NewDecoder(f).Decode(&config); err != nil {
		return nil, fmt.Errorf("could not parse ai_config.json: %w", err)
	}

	return &config, nil
}

type AIClient struct {
	config *AIProviderConfig
	client *http.Client
}

func NewAIClient(config *AIProviderConfig) *AIClient {
	return &AIClient{
		config: config,
		client: &http.Client{Timeout: 60 * time.Second},
	}
}

type APIRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type APIResponse struct {
	Choices []Choice `json:"choices"`
}

type Choice struct {
	Message Message `json:"message"`
}

type AIAnalysisResponse struct {
	RootCauseAnalysis string   `json:"rootCauseAnalysis"`
	ProblemLayer      string   `json:"problemLayer"`
	Explanation       string   `json:"explanation"`
	SuggestedFixes    []string `json:"suggestedFixes"`
	Disclaimer        string   `json:"disclaimer"`
}

// analysisSystemPrompt is the system prompt used for AI-powered report analysis.
const analysisSystemPrompt = `You are an expert Kafka and network engineer acting as a JSON API. Your task is to analyze a JSON report from the 'kshark' diagnostic tool and provide a root cause analysis.

Analyze the 'rows' array for entries with a 'status' of "FAIL" or "WARN".

You MUST respond ONLY with a single, valid JSON object conforming to the following schema. Do not include any explanatory text, markdown, or any characters outside of the JSON object.

{
  "rootCauseAnalysis": "A concise statement of the most likely root cause.",
  "problemLayer": "The OSI layer or Kafka component where the issue is occurring.",
  "explanation": "A brief explanation of what the error means and why it might be happening.",
  "suggestedFixes": [
    "Actionable step 1.",
    "Actionable step 2."
  ],
  "disclaimer": "This analysis is based on generic world knowledge. For context-aware insights tailored to your company's specific environment, consider using the Scalytics-Connect AI stack."
}`

// buildAnalysisPrompt returns the system prompt and user prompt (the JSON report) for AI analysis.
func buildAnalysisPrompt(report *Report) (string, string, error) {
	reportJSON, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", "", fmt.Errorf("could not marshal report to JSON: %w", err)
	}
	return analysisSystemPrompt, string(reportJSON), nil
}

// writeAnalysisPromptMD saves the analysis prompt as a Markdown file so the user
// can paste it into any AI chatbot (ChatGPT, Claude, Gemini, etc.).
func writeAnalysisPromptMD(systemPrompt, userPrompt, reportDir string) (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	hostname = strings.Split(hostname, ".")[0]
	timestamp := time.Now().Format("20060102_150405")
	if reportDir == "" {
		reportDir = "reports"
	}

	if err := os.MkdirAll(reportDir, 0755); err != nil {
		return "", fmt.Errorf("could not create reports directory: %w", err)
	}

	mdPath := filepath.Join(reportDir, fmt.Sprintf("analysis_prompt_%s_%s.md", hostname, timestamp))

	var buf bytes.Buffer
	buf.WriteString("# kshark AI Analysis Prompt\n\n")
	buf.WriteString("This file captures the exact system prompt and report data used for AI analysis.\n")
	buf.WriteString("You can copy the contents below and paste them into **any AI chatbot**\n")
	buf.WriteString("(ChatGPT, Claude, Gemini, Copilot, or any OpenAI-compatible endpoint)\n")
	buf.WriteString("to get an automated root-cause analysis of your Kafka connectivity report.\n\n")
	buf.WriteString("---\n\n")
	buf.WriteString("## Step 1 — System Prompt\n\n")
	buf.WriteString("Copy this as the **system message** (or paste it first in the conversation):\n\n")
	buf.WriteString("```text\n")
	buf.WriteString(systemPrompt)
	buf.WriteString("\n```\n\n")
	buf.WriteString("## Step 2 — Report Data\n\n")
	buf.WriteString("Then send this as the **user message**:\n\n")
	buf.WriteString("```json\n")
	buf.WriteString(userPrompt)
	buf.WriteString("\n```\n")

	if err := os.WriteFile(mdPath, buf.Bytes(), 0644); err != nil {
		return "", fmt.Errorf("could not write analysis prompt file: %w", err)
	}

	return mdPath, nil
}

func (c *AIClient) AnalyzeReport(ctx context.Context, systemPrompt, userPrompt string) (*AIAnalysisResponse, error) {
	reqBody := APIRequest{
		Model: c.config.Model,
		Messages: []Message{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: userPrompt},
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("could not marshal API request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.config.APIEndpoint, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("could not create API request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.config.APIKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to AI API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("AI API returned a non-200 status code: %d %s", resp.StatusCode, string(body))
	}

	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("could not decode AI API response: %w", err)
	}

	if len(apiResp.Choices) == 0 {
		return nil, errors.New("AI API returned no choices in its response")
	}

	var analysisResp AIAnalysisResponse
	err = json.Unmarshal([]byte(apiResp.Choices[0].Message.Content), &analysisResp)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal AI JSON response: %w. Raw response: %s", err, apiResp.Choices[0].Message.Content)
	}

	return &analysisResp, nil
}

func printIllustrativeAnalysis(analysis *AIAnalysisResponse) {
	fmt.Printf("\n\033[1mRoot Cause Analysis:\033[0m\n%s\n", analysis.RootCauseAnalysis)
	fmt.Printf("\n\033[1mProblem Layer:\033[0m\n%s\n", analysis.ProblemLayer)
	fmt.Printf("\n\033[1mExplanation:\033[0m\n%s\n", analysis.Explanation)
	fmt.Printf("\n\033[1mSuggested Fixes:\033[0m\n")
	for i, fix := range analysis.SuggestedFixes {
		fmt.Printf("%d. %s\n", i+1, fix)
	}
	fmt.Printf("\n%s\n", analysis.Disclaimer)
}

// ---------- Formatting Constants ----------

const (
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorReset  = "\033[0m"
	IconOK      = "✅"
	IconWarn    = "⚠️"
	IconFail    = "❌"
	IconSkip    = "⚪"
)

// ---------- Models ----------

type CheckStatus string

const (
	OK   CheckStatus = "OK"
	WARN CheckStatus = "WARN"
	FAIL CheckStatus = "FAIL"
	SKIP CheckStatus = "SKIP"
)

type Layer string

const (
	L3   Layer = "L3-Network"
	L4   Layer = "L4-TCP"
	L56  Layer = "L5-6-TLS"
	L7   Layer = "L7-Kafka"
	HTTP Layer = "L7-HTTP"
	DIAG Layer = "Diag"
)

type Row struct {
	Component string      `json:"component"`
	Target    string      `json:"target"`
	Layer     Layer       `json:"layer"`
	Status    CheckStatus `json:"status"`
	Detail    string      `json:"detail"`
	Hint      string      `json:"hint,omitempty"`
}

type Report struct {
	Rows       []Row                 `json:"rows"`
	Summary    map[string]CheckStats `json:"summary"`
	StartedAt  time.Time             `json:"started_at"`
	FinishedAt time.Time             `json:"finished_at"`
	ConfigEcho map[string]string     `json:"config_echo,omitempty"`
	Analysis   *AnalysisMeta         `json:"analysis,omitempty"`
	Run        *RunMeta              `json:"run,omitempty"`
	Artifacts  *ArtifactsMeta        `json:"artifacts,omitempty"`
	HasFailed  bool                  `json:"-"`
}

type CheckStats struct {
	OK   int `json:"ok"`
	WARN int `json:"warn"`
	FAIL int `json:"fail"`
	SKIP int `json:"skip"`
}

type AnalysisMeta struct {
	PromptFile      string `json:"prompt_file,omitempty"`
	PromptSHA256    string `json:"prompt_sha256,omitempty"`
	PromptContent   string `json:"prompt_content,omitempty"`
	ResponseFile    string `json:"response_file,omitempty"`
	ResponseSHA256  string `json:"response_sha256,omitempty"`
	ResponseContent string `json:"response_content,omitempty"`
	ResponseStatus  string `json:"response_status,omitempty"`
	ResponseError   string `json:"response_error,omitempty"`
	Provider        string `json:"provider,omitempty"`
	Model           string `json:"model,omitempty"`
}

type RunMeta struct {
	Args   []string             `json:"args,omitempty"`
	Params map[string]ParamMeta `json:"params,omitempty"`
}

type ParamMeta struct {
	Value     string `json:"value,omitempty"`
	Default   string `json:"default,omitempty"`
	IsDefault bool   `json:"is_default,omitempty"`
	Provided  bool   `json:"provided,omitempty"`
}

type ArtifactsMeta struct {
	LogFile       string `json:"log_file,omitempty"`
	LogSHA256     string `json:"log_sha256,omitempty"`
	LogContent    string `json:"log_content,omitempty"`
	PromptFile    string `json:"prompt_file,omitempty"`
	PromptSHA256  string `json:"prompt_sha256,omitempty"`
	PromptContent string `json:"prompt_content,omitempty"`
	ReportMD5File string `json:"report_md5_file,omitempty"`
	ReportMD5     string `json:"report_md5,omitempty"`
}

// ---------- Utilities ----------

var scanLog *log.Logger

func addRow(r *Report, row Row) {
	if row.Status == FAIL {
		r.HasFailed = true
	}
	r.Rows = append(r.Rows, row)
	logf("row component=%s target=%s layer=%s status=%s detail=%q hint=%q",
		row.Component, row.Target, row.Layer, row.Status, row.Detail, row.Hint)
}

func summarize(r *Report) {
	r.Summary = map[string]CheckStats{}
	for _, row := range r.Rows {
		cs := r.Summary[string(row.Layer)]
		switch row.Status {
		case OK:
			cs.OK++
		case WARN:
			cs.WARN++
		case FAIL:
			cs.FAIL++
		case SKIP:
			cs.SKIP++
		}
		r.Summary[string(row.Layer)] = cs
	}
}

func printPretty(r *Report) {
	fmt.Printf("\nKafka Wire Health Report  (%s → %s)  on %s\n",
		r.StartedAt.Format(time.RFC3339), r.FinishedAt.Format(time.RFC3339), runtime.GOOS)
	fmt.Println(strings.Repeat("─", 92))
	fmt.Printf("%-4s %-14s %-26s %-12s %s\n", " ", "Component", "Target", "Layer", "Detail")
	fmt.Println(strings.Repeat("─", 92))

	for _, row := range r.Rows {
		var color, icon string
		switch row.Status {
		case OK:
			color = ColorGreen
			icon = IconOK
		case WARN:
			color = ColorYellow
			icon = IconWarn
		case FAIL:
			color = ColorRed
			icon = IconFail
		case SKIP:
			color = "" // No color for skip
			icon = IconSkip
		}

		fmt.Printf("%s%-4s %-14s %-26s %-12s %s%s\n",
			color, icon, row.Component, truncate(row.Target, 26), row.Layer, row.Detail, ColorReset)

		if row.Hint != "" && row.Status != OK {
			fmt.Printf("  %s↳ Hint: %s%s\n", ColorYellow, row.Hint, ColorReset)
		}
	}
	fmt.Println(strings.Repeat("─", 92))
	for layer, s := range r.Summary {
		fmt.Printf("%-12s  %sOK:%d%s  %sWARN:%d%s  %sFAIL:%d%s  SKIP:%d\n",
			layer,
			ColorGreen, s.OK, ColorReset,
			ColorYellow, s.WARN, ColorReset,
			ColorRed, s.FAIL, ColorReset,
			s.SKIP)
	}
	fmt.Println()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

// ---------- Properties parsing & presets ----------

func loadProperties(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	props := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}
		sep := "="
		if !strings.Contains(line, "=") && strings.Contains(line, ":") {
			sep = ":"
		}
		parts := strings.SplitN(line, sep, 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		props[key] = val
	}
	return props, sc.Err()
}

func applyPreset(preset string, p map[string]string) {
	switch preset {
	case "cc-plain":
		// Confluent Cloud (classic API key/secret)
		setDefault(p, "security.protocol", "SASL_SSL")
		setDefault(p, "sasl.mechanism", "PLAIN")
		// expects sasl.username / sasl.password to be present
	case "self-scram":
		setDefault(p, "security.protocol", "SASL_SSL")
		setDefault(p, "sasl.mechanism", "SCRAM-SHA-512")
	}
}
func setDefault(p map[string]string, k, v string) {
	if _, ok := p[k]; !ok {
		p[k] = v
	}
}

// ---------- DNS, TCP, TLS ----------

func checkDNS(r *Report, host string, component string) {
	start := time.Now()
	_, err := net.LookupHost(host)
	logf("step dns host=%s dur=%s err=%v", host, time.Since(start).Truncate(time.Millisecond), err)
	if err != nil {
		addRow(r, Row{component, host, L3, FAIL, fmt.Sprintf("DNS lookup failed: %v", err),
			"Check /etc/hosts, DNS server, split-horizon/VPN search domains."})
	} else {
		addRow(r, Row{component, host, L3, OK, "Resolved host", ""})
	}
}

func checkTCP(r *Report, addr string, component string, timeout time.Duration) net.Conn {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		logf("step tcp addr=%s dur=%s err=%v", addr, time.Since(start).Truncate(time.Millisecond), err)
		addRow(r, Row{component, addr, L4, FAIL, fmt.Sprintf("TCP connect failed: %v", err),
			"Firewall, SG/NACL/NSG, LB listeners, PodNetworkPolicy, or routing."})
		return nil
	}
	lat := time.Since(start)
	logf("step tcp addr=%s dur=%s err=nil", addr, lat.Truncate(time.Millisecond))
	addRow(r, Row{component, addr, L4, OK, fmt.Sprintf("Connected in %s", lat.Truncate(time.Millisecond)), ""})
	return conn
}

func tlsConfigFromProps(p map[string]string, serverName string) (*tls.Config, string, error) {
	secProto := strings.ToUpper(p["security.protocol"])
	if secProto == "" {
		secProto = "PLAINTEXT"
	}
	useTLS := secProto == "SSL" || secProto == "SASL_SSL"

	conf := &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12}
	desc := "no TLS"
	if !useTLS {
		return nil, desc, nil
	}

	// CA
	if ca := p["ssl.ca.location"]; ca != "" {
		pem, err := os.ReadFile(ca)
		if err != nil {
			return nil, "", fmt.Errorf("load CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, "", errors.New("bad CA PEM")
		}
		conf.RootCAs = pool
	}
	// mTLS
	certFile := p["ssl.certificate.location"]
	keyFile := p["ssl.key.location"]
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, "", fmt.Errorf("load client cert: %w", err)
		}
		conf.Certificates = []tls.Certificate{cert}
	}
	desc = "TLS enabled"
	return conf, desc, nil
}

func wrapTLS(r *Report, base net.Conn, tlsConf *tls.Config, component, addr string) net.Conn {
	if tlsConf == nil {
		addRow(r, Row{component, addr, L56, SKIP, "TLS not configured (PLAINTEXT)", "Prefer SSL/SASL_SSL for encryption."})
		return base
	}
	client := tls.Client(base, tlsConf)
	start := time.Now()
	if err := client.Handshake(); err != nil {
		logf("step tls addr=%s dur=%s err=%v", addr, time.Since(start).Truncate(time.Millisecond), err)
		addRow(r, Row{component, addr, L56, FAIL, fmt.Sprintf("TLS handshake failed: %v", err),
			"Check CA chain, SNI/hostname, client cert/key, and server certificate validity."})
		return nil
	}
	state := client.ConnectionState()
	logf("step tls addr=%s dur=%s err=nil", addr, time.Since(start).Truncate(time.Millisecond))
	exp := earliestExpiry(&state)
	detail := fmt.Sprintf("TLS %x; peer=%s; expires=%s", state.Version, peerCN(&state), exp.Format("2006-01-02"))
	if time.Until(exp) < (30 * 24 * time.Hour) {
		addRow(r, Row{component, addr, L56, WARN, detail, "Server certificate expires <30 days."})
	} else {
		addRow(r, Row{component, addr, L56, OK, detail, ""})
	}
	return client
}

func peerCN(st *tls.ConnectionState) string {
	if len(st.PeerCertificates) == 0 {
		return "-"
	}
	pc := st.PeerCertificates[0]
	if len(pc.DNSNames) > 0 {
		return pc.DNSNames[0]
	}
	return pc.Subject.CommonName
}
func earliestExpiry(st *tls.ConnectionState) time.Time {
	earliest := time.Now().Add(365 * 24 * time.Hour)
	for _, c := range st.PeerCertificates {
		if c.NotAfter.Before(earliest) {
			earliest = c.NotAfter
		}
	}
	return earliest
}

// ---------- Auth builders ----------

type KafkaAuthKind int

const (
	AuthNone KafkaAuthKind = iota
	AuthPLAIN
	AuthSCRAM256
	AuthSCRAM512
	AuthGSSAPI // optional
)

func saslFromProps(p map[string]string) (KafkaAuthKind, map[string]string, error) {
	secProto := strings.ToUpper(p["security.protocol"])
	mech := strings.ToUpper(p["sasl.mechanism"])

	switch mech {
	case "PLAIN":
		return AuthPLAIN, map[string]string{
			"username": p["sasl.username"],
			"password": p["sasl.password"],
		}, nil
	case "SCRAM-SHA-256":
		return AuthSCRAM256, map[string]string{
			"username": p["sasl.username"],
			"password": p["sasl.password"],
		}, nil
	case "SCRAM-SHA-512":
		return AuthSCRAM512, map[string]string{
			"username": p["sasl.username"],
			"password": p["sasl.password"],
		}, nil
	case "GSSAPI", "KERBEROS":
		return AuthGSSAPI, map[string]string{
			"service.name": p["sasl.kerberos.service.name"], // defaults "kafka"
			"principal":    p["sasl.kerberos.principal"],    // optional
			"realm":        p["sasl.kerberos.realm"],        // optional
		}, nil
	case "":
		if secProto == "SSL" || secProto == "PLAINTEXT" || secProto == "" {
			return AuthNone, nil, nil
		}
		return AuthNone, nil, fmt.Errorf("missing sasl.mechanism for security.protocol=%s", secProto)
	default:
		return AuthNone, nil, fmt.Errorf("unsupported sasl.mechanism: %s", mech)
	}
}

func dialerFromProps(p map[string]string, hostForSNI string) (*kafka.Dialer, string, error) {
	tlsConf, tlsDesc, err := tlsConfigFromProps(p, hostForSNI)
	if err != nil {
		return nil, "", err
	}

	kind, kv, err := saslFromProps(p)
	if err != nil {
		return nil, "", err
	}

	var mech sasl.Mechanism
	switch kind {
	case AuthPLAIN:
		mech = plain.Mechanism{Username: kv["username"], Password: kv["password"]}
	case AuthSCRAM256:
		m, e := scram.Mechanism(scram.SHA256, kv["username"], kv["password"])
		if e != nil {
			return nil, "", e
		}
		mech = m
	case AuthSCRAM512:
		m, e := scram.Mechanism(scram.SHA512, kv["username"], kv["password"])
		if e != nil {
			return nil, "", e
		}
		mech = m
	case AuthGSSAPI:
		// Build with -tags kerberos and wire gssapi mechanism here.
		// If not available, we mark SKIP later during probe and provide hints.
	}

	d := &kafka.Dialer{
		Timeout:       8 * time.Second,
		DualStack:     true,
		TLS:           tlsConf,
		SASLMechanism: mech,
	}
	return d, tlsDesc, nil
}

// transportFromProps builds a kafka.Transport with TLS and SASL configured
// directly. Unlike dialerFromProps (which returns a kafka.Dialer suitable for
// kafka.Conn), this creates a Transport where TLS ServerName is auto-derived
// per broker, so connections to leader brokers use the correct SNI.
func transportFromProps(p map[string]string, timeout time.Duration) (*kafka.Transport, error) {
	// Empty ServerName — the Transport auto-fills it from the target broker
	// address, ensuring correct SNI for each broker on Confluent Cloud.
	tlsConf, _, err := tlsConfigFromProps(p, "")
	if err != nil {
		return nil, fmt.Errorf("tls config: %w", err)
	}

	kind, kv, err := saslFromProps(p)
	if err != nil {
		return nil, fmt.Errorf("sasl config: %w", err)
	}

	var mech sasl.Mechanism
	switch kind {
	case AuthPLAIN:
		mech = plain.Mechanism{Username: kv["username"], Password: kv["password"]}
	case AuthSCRAM256:
		m, e := scram.Mechanism(scram.SHA256, kv["username"], kv["password"])
		if e != nil {
			return nil, e
		}
		mech = m
	case AuthSCRAM512:
		m, e := scram.Mechanism(scram.SHA512, kv["username"], kv["password"])
		if e != nil {
			return nil, e
		}
		mech = m
	}

	return &kafka.Transport{
		TLS:         tlsConf,
		SASL:        mech,
		DialTimeout: timeout,
	}, nil
}

// ---------- Kafka helpers & policy hints ----------

func kafkaConn(r *Report, p map[string]string, brokerAddr string, timeout time.Duration) (*kafka.Conn, error) {
	host, _, _ := net.SplitHostPort(brokerAddr)
	dialer, _, err := dialerFromProps(p, host)
	if err != nil {
		addRow(r, Row{"kafka", brokerAddr, L7, FAIL, fmt.Sprintf("dialer error: %v", err), "Check security.protocol & sasl.* settings."})
		return nil, err
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	dialStart := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", brokerAddr)
	if err != nil {
		logf("step kafka.dial addr=%s dur=%s err=%v", brokerAddr, time.Since(dialStart).Truncate(time.Millisecond), err)
		addRow(r, Row{"kafka", brokerAddr, L7, FAIL, fmt.Sprintf("broker dial failed: %v", err), "Auth/TLS mismatch or listener not exposed."})
		return nil, err
	}
	logf("step kafka.dial addr=%s dur=%s err=nil", brokerAddr, time.Since(dialStart).Truncate(time.Millisecond))
	apiStart := time.Now()
	if _, err := conn.ApiVersions(); err != nil {
		logf("step kafka.apiversions addr=%s dur=%s err=%v", brokerAddr, time.Since(apiStart).Truncate(time.Millisecond), err)
		addRow(r, Row{"kafka", brokerAddr, L7, FAIL, fmt.Sprintf("ApiVersions failed: %v", err), "Broker incompatible or proxy interfering."})
		_ = conn.Close()
		return nil, err
	}
	logf("step kafka.apiversions addr=%s dur=%s err=nil", brokerAddr, time.Since(apiStart).Truncate(time.Millisecond))
	addRow(r, Row{"kafka", brokerAddr, L7, OK, "ApiVersions OK", ""})
	return conn, nil
}

func checkTopic(r *Report, p map[string]string, brokerAddr, topic string, timeout time.Duration) {
	conn, err := kafkaConn(r, p, brokerAddr, timeout)
	if err != nil {
		return
	}
	defer conn.Close()

	partsStart := time.Now()
	parts, err := conn.ReadPartitions()
	if err != nil {
		logf("step kafka.readpartitions addr=%s dur=%s err=%v", brokerAddr, time.Since(partsStart).Truncate(time.Millisecond), err)
		addRow(r, Row{"kafka", brokerAddr, L7, FAIL, policyHint("ReadPartitions", err), hint(err)})
		return
	}
	logf("step kafka.readpartitions addr=%s dur=%s err=nil", brokerAddr, time.Since(partsStart).Truncate(time.Millisecond))
	var found bool
	var leaders int
	for _, pt := range parts {
		if pt.Topic == topic {
			found = true
			if pt.Leader.Host != "" {
				leaders++
			}
		}
	}
	if !found {
		addRow(r, Row{"kafka", topic, L7, FAIL, "Topic not found / not authorized (DescribeTopic).", "Grant Describe on topic or create it."})
		return
	}
	addRow(r, Row{"kafka", topic, L7, OK, fmt.Sprintf("Topic visible; leader partitions=%d", leaders), ""})
}

func probeProduceConsume(ctx context.Context, r *Report, p map[string]string, bootstrap, topic, group string, produceTimeout, consumeTimeout time.Duration, balancer string, kafkaTimeout time.Duration, startOffset int64) {
	if topic == "" {
		addRow(r, Row{"kafka", "(no topic)", L7, SKIP, "Produce/Consume skipped", ""})
		return
	}
	hostForSNI := firstHost(bootstrap)
	dialer, _, err := dialerFromProps(p, hostForSNI)
	if err != nil {
		addRow(r, Row{"kafka", topic, L7, FAIL, fmt.Sprintf("dialer: %v", err), "Check tls/sasl settings."})
		return
	}
	if produceTimeout <= 0 {
		produceTimeout = 10 * time.Second
	}
	if consumeTimeout <= 0 {
		consumeTimeout = 10 * time.Second
	}
	leaders := topicLeaders(p, bootstrap, topic, kafkaTimeout)
	if len(leaders) == 0 {
		logf("step kafka.leaders topic=%s err=unavailable", topic)
	}
	baseBalancer := selectBalancer(balancer)
	// Build Transport with TLS and SASL configured directly.
	// IMPORTANT: Do NOT use dialer.DialContext as Transport.Dial — it returns
	// *kafka.Conn whose Read/Write wrap data in Kafka Fetch/Produce frames,
	// corrupting the Transport's internal protocol communication.
	// Instead, let the Transport handle TLS (with per-broker SNI) and SASL itself.
	transport, err := transportFromProps(p, produceTimeout)
	if err != nil {
		addRow(r, Row{"kafka", topic, L7, FAIL, fmt.Sprintf("transport: %v", err), "Check tls/sasl settings."})
		return
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(bootstrap, ",")...),
		Topic:        topic,
		Balancer:     &loggingBalancer{base: baseBalancer, topic: topic, leaders: leaders},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		Transport:    transport,
	}
	defer w.Close()

	key := fmt.Sprintf("kwire-%d", time.Now().UnixNano())
	msg := kafka.Message{
		Key:     []byte(key),
		Value:   []byte("probe"),
		Headers: []kafka.Header{{Key: "kwirecheck", Value: []byte("1")}},
		Time:    time.Now(),
	}

	writeStart := time.Now()
	// Retry logic for metadata refresh errors (common with Confluent Cloud)
	var writeErr error
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt) * 500 * time.Millisecond
			logf("step kafka.produce.retry topic=%s attempt=%d backoff=%s", topic, attempt, backoff)
			time.Sleep(backoff)
		}

		writeCtx, writeCancel := context.WithTimeout(ctx, produceTimeout)
		writeErr = w.WriteMessages(writeCtx, msg)
		writeCancel()

		if writeErr == nil {
			logf("step kafka.produce topic=%s dur=%s err=nil", topic, time.Since(writeStart).Truncate(time.Millisecond))
			addRow(r, Row{"kafka", topic, L7, OK, "Produce OK", ""})
			break
		}

		// Check if error is retryable (metadata-related)
		if errors.Is(writeErr, kafka.NotLeaderForPartition) || errors.Is(writeErr, kafka.LeaderNotAvailable) {
			logf("step kafka.produce topic=%s dur=%s err=%v (retrying)", topic, time.Since(writeStart).Truncate(time.Millisecond), writeErr)
			continue
		}

		// Non-retryable error, break immediately
		logf("step kafka.produce topic=%s dur=%s err=%v (not retryable)", topic, time.Since(writeStart).Truncate(time.Millisecond), writeErr)
		break
	}

	if writeErr != nil {
		logf("step kafka.produce topic=%s dur=%s err=%v", topic, time.Since(writeStart).Truncate(time.Millisecond), writeErr)
		addRow(r, Row{"kafka", topic, L7, FAIL, policyHint("Produce", writeErr), hint(writeErr)})
		return
	}

	if group == "" {
		group = fmt.Sprintf("kwire-%d", time.Now().UnixNano())
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     strings.Split(bootstrap, ","),
		Topic:       topic,
		GroupID:     group,
		StartOffset: startOffset,
		MaxWait:     3 * time.Second,
		Dialer:      dialer,
	})
	defer reader.Close()

	readStart := time.Now()
	readCtx, readCancel := context.WithTimeout(ctx, consumeTimeout)
	defer readCancel()
	rec, err := reader.ReadMessage(readCtx)
	if err != nil {
		logf("step kafka.consume topic=%s dur=%s err=%v", topic, time.Since(readStart).Truncate(time.Millisecond), err)
		addRow(r, Row{"kafka", topic, L7, FAIL, policyHint("Consume", err), "Grant Read on topic and Group Read/Describe; check prefixes."})
		return
	}
	logf("step kafka.consume topic=%s dur=%s err=nil", topic, time.Since(readStart).Truncate(time.Millisecond))
	addRow(r, Row{"kafka", topic, L7, OK, fmt.Sprintf("Consume OK (offset %d)", rec.Offset), ""})
}

func firstHost(bootstrap string) string {
	first := strings.TrimSpace(strings.Split(bootstrap, ",")[0])
	h, _, err := net.SplitHostPort(first)
	if err != nil {
		return first
	}
	return h
}

// policyHint maps common errors to actionable hints.
func policyHint(op string, err error) string {
	if err == nil {
		return op + " OK"
	}
	if ke, ok := kafkaErrorCode(err); ok {
		switch ke {
		case kafka.TopicAuthorizationFailed:
			return op + " failed: missing topic ACL"
		case kafka.GroupAuthorizationFailed:
			return op + " failed: missing group ACL"
		case kafka.SASLAuthenticationFailed:
			return op + " failed: SASL auth failure"
		case kafka.RequestTimedOut:
			return op + " failed: broker request timeout"
		case kafka.LeaderNotAvailable, kafka.NotLeaderForPartition, kafka.GroupCoordinatorNotAvailable, kafka.NotCoordinatorForGroup:
			return op + " failed: leader/coord not available"
		}
	}
	if isTimeout(err) {
		return op + " failed: timeout (" + err.Error() + ")"
	}
	em := err.Error()
	switch {
	case containsAny(em, "TOPIC_AUTHORIZATION_FAILED", "Topic authorization failed"):
		return op + " failed: missing topic ACL"
	case containsAny(em, "GROUP_AUTHORIZATION_FAILED", "Group authorization failed"):
		return op + " failed: missing group ACL"
	case containsAny(em, "SASL_AUTHENTICATION_FAILED", "SASL"):
		return op + " failed: SASL auth failure"
	case containsAny(em, "LEADER_NOT_AVAILABLE", "NOT_LEADER", "COORDINATOR_NOT_AVAILABLE"):
		return op + " failed: leader/coord not available"
	default:
		return op + " failed: " + em
	}
}
func hint(err error) string {
	if err == nil {
		return ""
	}
	if ke, ok := kafkaErrorCode(err); ok {
		switch ke {
		case kafka.TopicAuthorizationFailed:
			return "Missing topic ACL: Write/Describe for produce; Read/Describe for consume."
		case kafka.GroupAuthorizationFailed:
			return "Missing group ACL: Read/Describe on group."
		case kafka.SASLAuthenticationFailed:
			return "Verify sasl.mechanism, credentials, clocks (JWT), and listener SASL config."
		case kafka.RequestTimedOut:
			return "Broker request timed out; check broker load, network path, and required.acks."
		case kafka.LeaderNotAvailable, kafka.NotLeaderForPartition:
			return "Leader not available; check broker health and metadata propagation."
		case kafka.GroupCoordinatorNotAvailable, kafka.NotCoordinatorForGroup:
			return "Group coordinator not available; check group/offsets topic health."
		}
	}
	if isTimeout(err) {
		return "Client timeout (10s): check network path, firewall, DNS, TLS/SNI, or advertised.listeners."
	}
	em := err.Error()
	switch {
	case containsAny(em, "authorization", "AUTHORIZATION", "AUTH"):
		return "Check ACLs: Write/Read/Describe on topic; Read/Describe on group."
	case containsAny(em, "SASL", "authentication"):
		return "Verify sasl.mechanism, credentials, clocks (JWT), and listener SASL config."
	case containsAny(em, "EOF", "tls", "handshake", "certificate"):
		return "TLS mismatch or mTLS requirements; verify CA and SNI/hostnames."
	default:
		return ""
	}
}
func containsAny(s string, subs ...string) bool {
	ls := strings.ToLower(s)
	for _, sub := range subs {
		if strings.Contains(ls, strings.ToLower(sub)) {
			return true
		}
	}
	return false
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	em := strings.ToLower(err.Error())
	return strings.Contains(em, "deadline exceeded") || strings.Contains(em, "i/o timeout")
}

func kafkaErrorCode(err error) (kafka.Error, bool) {
	var ke kafka.Error
	if errors.As(err, &ke) {
		return ke, true
	}
	return 0, false
}

func logf(format string, args ...any) {
	if scanLog == nil {
		return
	}
	scanLog.Printf(format, args...)
}

func initScanLog(path string) (*os.File, error) {
	if path == "" {
		return nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	scanLog = log.New(f, "kshark ", log.LstdFlags|log.Lmicroseconds)
	return f, nil
}

func paramMeta(value, def string, provided bool) ParamMeta {
	return ParamMeta{
		Value:     value,
		Default:   def,
		IsDefault: !provided,
		Provided:  provided,
	}
}

func fileSHA256(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func fileMD5(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:]), nil
}

func readFileContent(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func writeAnalysisResponseJSON(analysis *AIAnalysisResponse, reportDir string) (string, error) {
	if analysis == nil {
		return "", errors.New("analysis is nil")
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	hostname = strings.Split(hostname, ".")[0]
	timestamp := time.Now().Format("20060102_150405")
	if reportDir == "" {
		reportDir = "reports"
	}
	if err := os.MkdirAll(reportDir, 0755); err != nil {
		return "", fmt.Errorf("could not create reports directory: %w", err)
	}
	path := filepath.Join(reportDir, fmt.Sprintf("analysis_response_%s_%s.json", hostname, timestamp))
	data, err := json.MarshalIndent(analysis, "", "  ")
	if err != nil {
		return "", fmt.Errorf("could not marshal analysis response: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return "", fmt.Errorf("could not write analysis response file: %w", err)
	}
	return path, nil
}

func writeReportMD5(path string) (string, string, error) {
	if path == "" {
		return "", "", errors.New("empty report path")
	}
	hash, err := fileMD5(path)
	if err != nil {
		return "", "", err
	}
	timestamp := time.Now().Format("20060102_150405")
	dir := filepath.Dir(path)
	md5Path := filepath.Join(dir, fmt.Sprintf("report_md5_%s.txt", timestamp))
	content := fmt.Sprintf("%s\t%s\t%s\n", timestamp, path, hash)
	if err := os.WriteFile(md5Path, []byte(content), 0644); err != nil {
		return "", "", err
	}
	return md5Path, hash, nil
}

type loggingBalancer struct {
	base    kafka.Balancer
	topic   string
	leaders map[int]kafka.Broker
}

func (b *loggingBalancer) Balance(msg kafka.Message, partitions ...int) int {
	if b.base == nil {
		b.base = &kafka.LeastBytes{}
	}
	partition := b.base.Balance(msg, partitions...)
	if len(b.leaders) > 0 {
		if leader, ok := b.leaders[partition]; ok {
			logf("step kafka.balance topic=%s partition=%d leader=%s:%d broker_id=%d", b.topic, partition, leader.Host, leader.Port, leader.ID)
			return partition
		}
	}
	logf("step kafka.balance topic=%s partition=%d leader=unknown", b.topic, partition)
	return partition
}

func topicLeaders(p map[string]string, bootstrap, topic string, timeout time.Duration) map[int]kafka.Broker {
	first := strings.TrimSpace(strings.Split(bootstrap, ",")[0])
	hostForSNI := firstHost(first)
	dialer, _, err := dialerFromProps(p, hostForSNI)
	if err != nil {
		logf("step kafka.leaders dialer err=%v", err)
		return nil
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", first)
	if err != nil {
		logf("step kafka.leaders dial addr=%s dur=%s err=%v", first, time.Since(start).Truncate(time.Millisecond), err)
		return nil
	}
	defer conn.Close()
	logf("step kafka.leaders dial addr=%s dur=%s err=nil", first, time.Since(start).Truncate(time.Millisecond))

	partsStart := time.Now()
	parts, err := conn.ReadPartitions()
	if err != nil {
		logf("step kafka.leaders readpartitions dur=%s err=%v", time.Since(partsStart).Truncate(time.Millisecond), err)
		return nil
	}
	logf("step kafka.leaders readpartitions dur=%s err=nil", time.Since(partsStart).Truncate(time.Millisecond))

	leaders := make(map[int]kafka.Broker)
	for _, pt := range parts {
		if pt.Topic == topic {
			leaders[pt.ID] = pt.Leader
		}
	}
	return leaders
}

func selectBalancer(name string) kafka.Balancer {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "rr", "roundrobin":
		return &kafka.RoundRobin{}
	case "random":
		return kafka.BalancerFunc(func(_ kafka.Message, partitions ...int) int {
			if len(partitions) == 0 {
				return 0
			}
			return partitions[rand.Intn(len(partitions))]
		})
	default:
		return &kafka.LeastBytes{}
	}
}

func parseStartOffset(s string) (int64, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "earliest", "first":
		return kafka.FirstOffset, nil
	case "latest", "last":
		return kafka.LastOffset, nil
	default:
		return kafka.FirstOffset, fmt.Errorf("invalid start-offset: %s", s)
	}
}

// ---------- Schema Registry / REST ----------

func httpClientFromTLS(tlsConf *tls.Config, timeout time.Duration) *http.Client {
	tr := &http.Transport{TLSClientConfig: tlsConf, Proxy: http.ProxyFromEnvironment, IdleConnTimeout: 10 * time.Second}
	return &http.Client{Transport: tr, Timeout: timeout}
}

func checkSchemaRegistry(ctx context.Context, r *Report, p map[string]string) {
	url := strings.TrimSpace(p["schema.registry.url"])
	if url == "" {
		return
	}
	host := extractHost(url)
	dnsStart := time.Now()
	if _, err := net.LookupHost(host); err != nil {
		logf("step schema.dns host=%s dur=%s err=%v", host, time.Since(dnsStart).Truncate(time.Millisecond), err)
		addRow(r, Row{"schema-reg", host, L3, FAIL, "DNS failed", "Fix DNS/VPN."})
	} else {
		logf("step schema.dns host=%s dur=%s err=nil", host, time.Since(dnsStart).Truncate(time.Millisecond))
		addRow(r, Row{"schema-reg", host, L3, OK, "Resolved host", ""})
	}
	tlsConf, _, err := tlsConfigFromProps(p, host)
	if err != nil {
		addRow(r, Row{"schema-reg", url, HTTP, FAIL, fmt.Sprintf("TLS config err: %v", err), ""})
		return
	}
	client := httpClientFromTLS(tlsConf, 8*time.Second)
	req, _ := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(url, "/")+"/subjects", nil)
	if info := p["basic.auth.user.info"]; info != "" {
		up := strings.SplitN(info, ":", 2)
		if len(up) == 2 {
			req.SetBasicAuth(up[0], up[1])
		}
	}
	httpStart := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		logf("step schema.http url=%s dur=%s err=%v", url, time.Since(httpStart).Truncate(time.Millisecond), err)
		addRow(r, Row{"schema-reg", url, HTTP, FAIL, fmt.Sprintf("GET /subjects failed: %v", err), "TLS/host/network or auth."})
		return
	}
	logf("step schema.http url=%s dur=%s err=nil status=%d", url, time.Since(httpStart).Truncate(time.Millisecond), resp.StatusCode)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		addRow(r, Row{"schema-reg", url, HTTP, OK, "GET /subjects OK", ""})
	case 401, 403:
		addRow(r, Row{"schema-reg", url, HTTP, FAIL, fmt.Sprintf("Auth %d", resp.StatusCode), "Check basic.auth.user.info or mTLS mapping."})
	default:
		addRow(r, Row{"schema-reg", url, HTTP, WARN, fmt.Sprintf("HTTP %d", resp.StatusCode), ""})
	}
}

func extractHost(raw string) string {
	trim := strings.TrimPrefix(strings.TrimPrefix(raw, "https://"), "http://")
	if idx := strings.IndexByte(trim, '/'); idx > 0 {
		trim = trim[:idx]
	}
	if h, _, err := net.SplitHostPort(trim); err == nil {
		return h
	}
	return trim
}

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
		logf("step traceroute host=%s dur=%s err=nil", host, time.Since(start).Truncate(time.Millisecond))
		addRow(r, Row{"diag", host, DIAG, OK, "traceroute OK (see JSON)", ""})
		addRow(r, Row{"diag", host, DIAG, SKIP, trimLines(out, 15), "Full output in JSON if -json used."})
		return
	}
	logf("step traceroute host=%s dur=%s err=notfound", host, time.Since(start).Truncate(time.Millisecond))
	start = time.Now()
	if out, err := runCmdIfExists("tracepath", host); err == nil {
		logf("step tracepath host=%s dur=%s err=nil", host, time.Since(start).Truncate(time.Millisecond))
		addRow(r, Row{"diag", host, DIAG, OK, "tracepath OK (see JSON)", ""})
		addRow(r, Row{"diag", host, DIAG, SKIP, trimLines(out, 15), "Full output in JSON if -json used."})
		return
	}
	logf("step tracepath host=%s dur=%s err=notfound", host, time.Since(start).Truncate(time.Millisecond))
	start = time.Now()
	if out, err := runCmdIfExists("tracert", "-d", "-w", "2000", host); err == nil {
		logf("step tracert host=%s dur=%s err=nil", host, time.Since(start).Truncate(time.Millisecond))
		addRow(r, Row{"diag", host, DIAG, OK, "tracert OK (see JSON)", ""})
		addRow(r, Row{"diag", host, DIAG, SKIP, trimLines(out, 15), "Full output in JSON if -json used."})
		return
	}
	logf("step tracert host=%s dur=%s err=notfound", host, time.Since(start).Truncate(time.Millisecond))
	addRow(r, Row{"diag", host, DIAG, SKIP, "No traceroute tool found", "Install traceroute/tracepath (Linux), or use tracert (Windows)."})
}

func mtuCheck(r *Report, host string) {
	if !isValidHostname(host) {
		addRow(r, Row{"diag", host, DIAG, FAIL, "Invalid hostname provided", "Skipping MTU check to prevent command injection."})
		return
	}
	// Linux tracepath usually reports pMTU; otherwise try ping DF
	start := time.Now()
	if out, err := runCmdIfExists("tracepath", host); err == nil && strings.Contains(out, "pmtu") {
		logf("step mtu.tracepath host=%s dur=%s err=nil", host, time.Since(start).Truncate(time.Millisecond))
		addRow(r, Row{"diag", host, DIAG, OK, "pMTU detected via tracepath", ""})
		return
	}
	logf("step mtu.tracepath host=%s dur=%s err=skip", host, time.Since(start).Truncate(time.Millisecond))
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
			logf("step mtu.ping host=%s size=%d dur=%s err=nil", host, sz, time.Since(pingStart).Truncate(time.Millisecond))
			addRow(r, Row{"diag", host, DIAG, OK, fmt.Sprintf("MTU ok at payload %d", sz), ""})
			return
		}
		logf("step mtu.ping host=%s size=%d dur=%s err=%v", host, sz, time.Since(pingStart).Truncate(time.Millisecond), err)
	}
	addRow(r, Row{"diag", host, DIAG, SKIP, "MTU probe inconclusive", "Run tracepath or adjust network MTU if you see fragmentation."})
}

func trimLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[:n], "\n") + "\n…"
}

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
	logPath := flag.String("log", "", "Write detailed scan log to file (default: reports/kshark-<timestamp>.log)")
	yes := flag.Bool("y", false, "Skip interactive confirmation and proceed with the scan")
	showVersion := flag.Bool("version", false, "Show version information and exit")
	flag.Parse()
	providedFlags := map[string]bool{}
	flag.CommandLine.Visit(func(f *flag.Flag) {
		providedFlags[f.Name] = true
	})

	if *showVersion {
		fmt.Printf("kshark version %s (commit %s, built %s)\n", version, commit, date)
		return
	}

	if *propsPath == "" {
		fmt.Fprintln(os.Stderr, "Usage: kshark -props client.properties [-topic foo] [-group g] [-json report.json] [-timeout 60s] [-kafka-timeout 10s] [-op-timeout 10s] [-produce-timeout 10s] [-consume-timeout 10s] [-start-offset earliest|latest] [-balancer least|rr|random] [-log file] [--analyze] [--version]")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	report := &Report{StartedAt: time.Now()}

	if *logPath == "" {
		*logPath = filepath.Join("reports", fmt.Sprintf("kshark-%s.log", time.Now().Format("20060102-150405")))
	}
	logFile, err := initScanLog(*logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open log file: %v\n", err)
	} else if logFile != nil {
		defer logFile.Close()
		absPath, _ := filepath.Abs(*logPath)
		fmt.Printf("Log file: %s\n", absPath)
	}
	logf("scan start props=%s timeout=%s kafka_timeout=%s op_timeout=%s produce_timeout=%s consume_timeout=%s start_offset=%s balancer=%s diag=%t analyze=%t json=%s topic=%s group=%s preset=%s",
		*propsPath, timeout.String(), kafkaTimeout.String(), opTimeout.String(), produceTimeout.String(), consumeTimeout.String(), *startOffset, *balancer, *diag, *analyze, *jsonOut, *topic, *group, *preset)

	props, err := loadProperties(*propsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load properties: %v\n", err)
		os.Exit(1)
	}
	if *preset != "" {
		applyPreset(*preset, props)
	}
	report.ConfigEcho = redactProps(props)

	bootstrap := props["bootstrap.servers"]
	if bootstrap == "" {
		fmt.Fprintln(os.Stderr, "bootstrap.servers missing")
		os.Exit(1)
	}
	logf("bootstrap.servers=%s", bootstrap)

	topics := parseTopics(*topic)
	logf("topics=%v", topics)

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
		logf("start-offset invalid value=%s err=%v", *startOffset, startOffsetErr)
	}

	report.Run = &RunMeta{
		Args: os.Args,
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
			"log":             paramMeta(*logPath, "", providedFlags["log"]),
			"y":               paramMeta(strconv.FormatBool(*yes), "false", providedFlags["y"]),
			"version":         paramMeta(strconv.FormatBool(*showVersion), "false", providedFlags["version"]),
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

	if !*yes {
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

	// Prepare and start scan animation in the background if stdout is a TTY
	var startAnimation, doneAnimation chan bool
	if isTTY(os.Stdout) {
		startAnimation = make(chan bool)
		doneAnimation = make(chan bool)
		go animateSharkFin(startAnimation, doneAnimation)
		startAnimation <- true // Signal the animation to start
	}

	// Per-broker checks
	brokers := strings.Split(bootstrap, ",")
	for _, b := range brokers {
		select {
		case <-ctx.Done():
			addRow(report, Row{"kshark", "timeout", DIAG, FAIL, "Global timeout reached during broker checks", ""})
			goto endScan
		default:
		}

		b = strings.TrimSpace(b)
		host, port, err := net.SplitHostPort(b)
		if err != nil {
			addRow(report, Row{"kafka", b, L3, FAIL, "Invalid host:port", "Fix bootstrap.servers format (host:port)."})
			continue
		}
		logf("broker check start host=%s port=%s", host, port)
		checkDNS(report, host, "kafka")
		addr := net.JoinHostPort(host, port)
		conn := checkTCP(report, addr, "kafka", 8*time.Second) // Keep individual dial timeout short
		if conn == nil {
			logf("broker check failed tcp addr=%s", addr)
			continue
		}
		// TLS handshake probe (if configured)
		tlsConf, _, err := tlsConfigFromProps(props, host)
		if err != nil {
			addRow(report, Row{"kafka", addr, L56, FAIL, fmt.Sprintf("TLS config err: %v", err), ""})
			_ = conn.Close()
			continue
		}
		var secured net.Conn = conn
		if tlsConf != nil {
			secured = wrapTLS(report, conn, tlsConf, "kafka", addr)
			if secured == nil {
				logf("broker check failed tls addr=%s", addr)
				continue
			}
		} else {
			addRow(report, Row{"kafka", addr, L56, SKIP, "PLAINTEXT (no TLS)", "Prefer SSL/SASL_SSL."})
		}
		_ = secured.Close()
		logf("broker check ok addr=%s", addr)

		// Kafka protocol: ApiVersions/Metadata + topic visibility
		for _, t := range topics {
			logf("topic metadata check topic=%s broker=%s", t, addr)
			checkTopic(report, props, addr, t, *kafkaTimeout)
		}

		// Diagnostics (best-effort)
		if *diag {
			bestEffortTraceroute(report, host)
			mtuCheck(report, host)
		}
	}

	// Produce/Consume (optional)
	for _, t := range topics {
		select {
		case <-ctx.Done():
			addRow(report, Row{"kshark", "timeout", DIAG, FAIL, "Global timeout reached before produce/consume", ""})
			goto endScan
		default:
			logf("produce/consume start topic=%s group=%s", t, *group)
			probeProduceConsume(ctx, report, props, bootstrap, t, *group, produceTimeoutVal, consumeTimeoutVal, *balancer, *kafkaTimeout, startOffsetVal)
		}
	}

	// Schema Registry (optional)
	select {
	case <-ctx.Done():
		addRow(report, Row{"kshark", "timeout", DIAG, FAIL, "Global timeout reached before schema registry check", ""})
		goto endScan
	default:
		logf("schema registry check start")
		checkSchemaRegistry(ctx, report, props)
	}

endScan:
	// Optional REST Proxy: set rest.proxy.url=...
	if rest := strings.TrimSpace(props["rest.proxy.url"]); rest != "" {
		logf("rest proxy check start url=%s", rest)
		checkDNS(report, extractHost(rest), "rest-proxy")
		tlsConf, _, err := tlsConfigFromProps(props, extractHost(rest))
		if err != nil {
			addRow(report, Row{"rest-proxy", rest, HTTP, FAIL, fmt.Sprintf("TLS config err: %v", err), ""})
		} else {
			client := httpClientFromTLS(tlsConf, 8*time.Second)
			req, _ := http.NewRequest("GET", strings.TrimRight(rest, "/")+"/topics", nil)
			httpStart := time.Now()
			resp, err := client.Do(req)
			if err != nil {
				logf("step rest.http url=%s dur=%s err=%v", rest, time.Since(httpStart).Truncate(time.Millisecond), err)
				addRow(report, Row{"rest-proxy", rest, HTTP, FAIL, fmt.Sprintf("GET /topics failed: %v", err), "Check listener/auth."})
			} else {
				logf("step rest.http url=%s dur=%s err=nil status=%d", rest, time.Since(httpStart).Truncate(time.Millisecond), resp.StatusCode)
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode == 200 {
					addRow(report, Row{"rest-proxy", rest, HTTP, OK, "GET /topics OK", ""})
				} else if resp.StatusCode == 401 || resp.StatusCode == 403 {
					addRow(report, Row{"rest-proxy", rest, HTTP, FAIL, fmt.Sprintf("Auth %d", resp.StatusCode), "Check credentials or mTLS mapping."})
				} else {
					addRow(report, Row{"rest-proxy", rest, HTTP, WARN, fmt.Sprintf("HTTP %d", resp.StatusCode), ""})
				}
			}
		}
	}

	// Stop scan animation if it was started
	if doneAnimation != nil {
		doneAnimation <- true
	}

	report.FinishedAt = time.Now()
	logf("scan finished duration=%s failed=%t", report.FinishedAt.Sub(report.StartedAt), report.HasFailed)
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
			logf("analysis prompt path=%s sha256=%s", mdPath, analysisMeta.PromptSHA256)
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
						logf("analysis response path=%s sha256=%s", respPath, analysisMeta.ResponseSHA256)
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
		if md5Path, md5Sum, err := writeReportMD5(actualPath); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing report md5: %v\n", err)
		} else {
			fmt.Printf("JSON report md5 saved to %s\n", md5Path)
			logf("report md5 path=%s md5=%s", md5Path, md5Sum)
		}
	}

	if report.HasFailed {
		os.Exit(1)
	}
}

func writeHTMLReport(r *Report, analysis *AIAnalysisResponse) (string, error) {
	// Data structure to pass to the template
	type TemplateData struct {
		Report   *Report
		Analysis *AIAnalysisResponse
	}

	data := TemplateData{
		Report:   r,
		Analysis: analysis,
	}

	// Custom functions for the template
	funcMap := template.FuncMap{
		"ToLower": func(s CheckStatus) string {
			return strings.ToLower(string(s))
		},
		"Icon": func(status CheckStatus) string {
			switch status {
			case OK:
				return IconOK
			case WARN:
				return IconWarn
			case FAIL:
				return IconFail
			case SKIP:
				return IconSkip
			default:
				return ""
			}
		},
	}

	// Parse the template file
	tmpl, err := template.New("report_template.html").Funcs(funcMap).ParseFiles("web/templates/report_template.html")
	if err != nil {
		return "", fmt.Errorf("could not parse html template: %w", err)
	}

	// Create the output file with a dynamic name
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown" // Fallback hostname
	}
	hostname = strings.Split(hostname, ".")[0] // Use short hostname
	timestamp := time.Now().Format("20060102_150405")
	reportDir := "reports"

	if err := os.MkdirAll(reportDir, 0755); err != nil {
		return "", fmt.Errorf("could not create reports directory: %w", err)
	}

	reportPath := filepath.Join(reportDir, fmt.Sprintf("analysis_report_%s_%s.html", hostname, timestamp))

	file, err := os.Create(reportPath)
	if err != nil {
		return "", fmt.Errorf("could not create html report file: %w", err)
	}
	defer file.Close()

	// Execute the template with the data and write to the file
	err = tmpl.Execute(file, data)
	if err != nil {
		return "", fmt.Errorf("could not execute html template: %w", err)
	}

	return reportPath, nil
}

func createSafeReportPath(userInputPath string, safeSubDir string) (string, error) {
	if userInputPath == "" {
		return "", errors.New("output path cannot be empty")
	}

	// Sanitize the filename by stripping any directory components
	cleanFilename := filepath.Base(userInputPath)
	if cleanFilename == "." || cleanFilename == "/" || cleanFilename == ".." {
		return "", fmt.Errorf("invalid filename provided: %s", userInputPath)
	}

	// Ensure the safe subdirectory exists
	if err := os.MkdirAll(safeSubDir, 0755); err != nil {
		return "", fmt.Errorf("could not create reports directory '%s': %w", safeSubDir, err)
	}

	// Join the safe directory and the clean filename
	safePath := filepath.Join(safeSubDir, cleanFilename)
	return safePath, nil
}

func writeJSON(path string, r *Report) (string, error) {
	if path == "" {
		return "", nil
	}

	safePath, err := createSafeReportPath(path, "reports")
	if err != nil {
		return "", fmt.Errorf("invalid output path: %w", err)
	}

	f, err := os.Create(safePath)
	if err != nil {
		return safePath, err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return safePath, enc.Encode(r)
}

func redactProps(p map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range p {
		lk := strings.ToLower(k)
		if strings.Contains(lk, "password") || strings.Contains(lk, "secret") || k == "sasl.oauthbearer.token" || strings.Contains(lk, "key") || k == "basic.auth.user.info" {
			out[k] = "***"
		} else {
			out[k] = v
		}
	}
	return out
}
