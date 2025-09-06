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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

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

func (c *AIClient) AnalyzeReport(report *Report) (*AIAnalysisResponse, error) {
	reportJSON, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("could not marshal report to JSON: %w", err)
	}

	systemPrompt := `You are an expert Kafka and network engineer acting as a JSON API. Your task is to analyze a JSON report from the 'kshark' diagnostic tool and provide a root cause analysis.

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

	userPrompt := string(reportJSON)

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

	req, err := http.NewRequest("POST", c.config.APIEndpoint, bytes.NewBuffer(reqBytes))
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
}

type CheckStats struct {
	OK   int `json:"ok"`
	WARN int `json:"warn"`
	FAIL int `json:"fail"`
	SKIP int `json:"skip"`
}

// ---------- Utilities ----------

func addRow(r *Report, row Row) { r.Rows = append(r.Rows, row) }

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
	_, err := net.LookupHost(host)
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
		addRow(r, Row{component, addr, L4, FAIL, fmt.Sprintf("TCP connect failed: %v", err),
			"Firewall, SG/NACL/NSG, LB listeners, PodNetworkPolicy, or routing."})
		return nil
	}
	lat := time.Since(start)
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
	if err := client.Handshake(); err != nil {
		addRow(r, Row{component, addr, L56, FAIL, fmt.Sprintf("TLS handshake failed: %v", err),
			"Check CA chain, SNI/hostname, client cert/key, and server certificate validity."})
		return nil
	}
	state := client.ConnectionState()
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

// ---------- Kafka helpers & policy hints ----------

func kafkaConn(r *Report, p map[string]string, brokerAddr string) (*kafka.Conn, error) {
	host, _, _ := net.SplitHostPort(brokerAddr)
	dialer, _, err := dialerFromProps(p, host)
	if err != nil {
		addRow(r, Row{"kafka", brokerAddr, L7, FAIL, fmt.Sprintf("dialer error: %v", err), "Check security.protocol & sasl.* settings."})
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := dialer.DialContext(ctx, "tcp", brokerAddr)
	if err != nil {
		addRow(r, Row{"kafka", brokerAddr, L7, FAIL, fmt.Sprintf("broker dial failed: %v", err), "Auth/TLS mismatch or listener not exposed."})
		return nil, err
	}
	if _, err := conn.ApiVersions(); err != nil {
		addRow(r, Row{"kafka", brokerAddr, L7, FAIL, fmt.Sprintf("ApiVersions failed: %v", err), "Broker incompatible or proxy interfering."})
		_ = conn.Close()
		return nil, err
	}
	addRow(r, Row{"kafka", brokerAddr, L7, OK, "ApiVersions OK", ""})
	return conn, nil
}

func checkTopic(r *Report, p map[string]string, brokerAddr, topic string) {
	conn, err := kafkaConn(r, p, brokerAddr)
	if err != nil {
		return
	}
	defer conn.Close()

	parts, err := conn.ReadPartitions()
	if err != nil {
		addRow(r, Row{"kafka", brokerAddr, L7, FAIL, policyHint("ReadPartitions", err), hint(err)})
		return
	}
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

func probeProduceConsume(r *Report, p map[string]string, bootstrap, topic, group string) {
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
	w := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(bootstrap, ",")...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		Transport:    &kafka.Transport{SASL: dialer.SASLMechanism, TLS: dialer.TLS, DialTimeout: 8 * time.Second},
	}
	defer w.Close()

	key := fmt.Sprintf("kwire-%d", time.Now().UnixNano())
	msg := kafka.Message{
		Key:     []byte(key),
		Value:   []byte("probe"),
		Headers: []kafka.Header{{Key: "kwirecheck", Value: []byte("1")}},
		Time:    time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, msg); err != nil {
		addRow(r, Row{"kafka", topic, L7, FAIL, policyHint("Produce", err), hint(err)})
		return
	}
	addRow(r, Row{"kafka", topic, L7, OK, "Produce OK", ""})

	if group == "" {
		group = fmt.Sprintf("kwire-%d", time.Now().UnixNano())
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(bootstrap, ","),
		Topic:   topic,
		GroupID: group,
		MaxWait: 3 * time.Second,
		Dialer:  dialer,
	})
	defer reader.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	rec, err := reader.ReadMessage(ctx2)
	if err != nil {
		addRow(r, Row{"kafka", topic, L7, FAIL, policyHint("Consume", err), "Grant Read on topic and Group Read/Describe; check prefixes."})
		return
	}
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

// ---------- Schema Registry / REST ----------

func httpClientFromTLS(tlsConf *tls.Config, timeout time.Duration) *http.Client {
	tr := &http.Transport{TLSClientConfig: tlsConf, Proxy: http.ProxyFromEnvironment, IdleConnTimeout: 10 * time.Second}
	return &http.Client{Transport: tr, Timeout: timeout}
}

func checkSchemaRegistry(r *Report, p map[string]string) {
	url := strings.TrimSpace(p["schema.registry.url"])
	if url == "" {
		return
	}
	host := extractHost(url)
	if _, err := net.LookupHost(host); err != nil {
		addRow(r, Row{"schema-reg", host, L3, FAIL, "DNS failed", "Fix DNS/VPN."})
	} else {
		addRow(r, Row{"schema-reg", host, L3, OK, "Resolved host", ""})
	}
	tlsConf, _, err := tlsConfigFromProps(p, host)
	if err != nil {
		addRow(r, Row{"schema-reg", url, HTTP, FAIL, fmt.Sprintf("TLS config err: %v", err), ""})
		return
	}
	client := httpClientFromTLS(tlsConf, 8*time.Second)
	req, _ := http.NewRequest("GET", strings.TrimRight(url, "/")+"/subjects", nil)
	if info := p["basic.auth.user.info"]; info != "" {
		up := strings.SplitN(info, ":", 2)
		if len(up) == 2 {
			req.SetBasicAuth(up[0], up[1])
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		addRow(r, Row{"schema-reg", url, HTTP, FAIL, fmt.Sprintf("GET /subjects failed: %v", err), "TLS/host/network or auth."})
		return
	}
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

func bestEffortTraceroute(r *Report, host string) {
	// Linux: traceroute or tracepath; macOS: traceroute; Windows: tracert
	if out, err := runCmdIfExists("traceroute", "-n", "-w", "2", "-q", "1", host); err == nil {
		addRow(r, Row{"diag", host, DIAG, OK, "traceroute OK (see JSON)", ""})
		addRow(r, Row{"diag", host, DIAG, SKIP, trimLines(out, 15), "Full output in JSON if -json used."})
		return
	}
	if out, err := runCmdIfExists("tracepath", host); err == nil {
		addRow(r, Row{"diag", host, DIAG, OK, "tracepath OK (see JSON)", ""})
		addRow(r, Row{"diag", host, DIAG, SKIP, trimLines(out, 15), "Full output in JSON if -json used."})
		return
	}
	if out, err := runCmdIfExists("tracert", "-d", "-w", "2000", host); err == nil {
		addRow(r, Row{"diag", host, DIAG, OK, "tracert OK (see JSON)", ""})
		addRow(r, Row{"diag", host, DIAG, SKIP, trimLines(out, 15), "Full output in JSON if -json used."})
		return
	}
	addRow(r, Row{"diag", host, DIAG, SKIP, "No traceroute tool found", "Install traceroute/tracepath (Linux), or use tracert (Windows)."})
}

func mtuCheck(r *Report, host string) {
	// Linux tracepath usually reports pMTU; otherwise try ping DF
	if out, err := runCmdIfExists("tracepath", host); err == nil && strings.Contains(out, "pmtu") {
		addRow(r, Row{"diag", host, DIAG, OK, "pMTU detected via tracepath", ""})
		return
	}
	// Try ping DF with decreasing sizes (Linux/macOS)
	for _, sz := range []int{1472, 1464, 1452, 1400, 1200, 1000} {
		var out string
		var err error
		if runtime.GOOS == "darwin" {
			out, err = runCmdIfExists("ping", "-D", "-s", strconv.Itoa(sz), "-c", "1", host)
		} else {
			out, err = runCmdIfExists("ping", "-M", "do", "-s", strconv.Itoa(sz), "-c", "1", host)
		}
		if err == nil && strings.Contains(strings.ToLower(out), "1 packets transmitted") {
			addRow(r, Row{"diag", host, DIAG, OK, fmt.Sprintf("MTU ok at payload %d", sz), ""})
			return
		}
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

// ---------- Licensing ----------

func checkLicense() bool {
	_, err := os.Stat("license.key")
	return err == nil
}

// ---------- Main ----------

func main() {
	propsPath := flag.String("props", "", "Path to client .properties")
	topic := flag.String("topic", "", "Topic to test (optional for metadata-only)")
	group := flag.String("group", "", "Consumer group for probe (ephemeral by default)")
	jsonOut := flag.String("json", "", "Write JSON report to file (premium feature)")
	analyze := flag.Bool("analyze", false, "Analyze report with AI (premium feature)")
	provider := flag.String("provider", "", "Select AI provider from ai_config.json (e.g., openai, scalytics-connect)")
	timeout := flag.Duration("timeout", 8*time.Second, "Dial/HTTP timeout")
	preset := flag.String("preset", "", "Preset: cc-plain|self-scram")
	diag := flag.Bool("diag", true, "Run traceroute/MTU diagnostics if tools are available")
	flag.Parse()

	if *propsPath == "" {
		fmt.Fprintln(os.Stderr, "Usage: kshark -props client.properties [-topic foo] [-group g] [-json report.json] [--analyze]")
		os.Exit(2)
	}

	hasLicense := checkLicense()
	if (*jsonOut != "" || *analyze) && !hasLicense {
		fmt.Fprintln(os.Stderr, "Error: --json and --analyze are premium features requiring a valid license.key file.")
		os.Exit(1)
	}

	report := &Report{StartedAt: time.Now()}

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

	// Per-broker checks
	brokers := strings.Split(bootstrap, ",")
	for _, b := range brokers {
		b = strings.TrimSpace(b)
		host, port, err := net.SplitHostPort(b)
		if err != nil {
			addRow(report, Row{"kafka", b, L3, FAIL, "Invalid host:port", "Fix bootstrap.servers format (host:port)."})
			continue
		}
		checkDNS(report, host, "kafka")
		addr := net.JoinHostPort(host, port)
		conn := checkTCP(report, addr, "kafka", *timeout)
		if conn == nil {
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
				continue
			}
		} else {
			addRow(report, Row{"kafka", addr, L56, SKIP, "PLAINTEXT (no TLS)", "Prefer SSL/SASL_SSL."})
		}
		_ = secured.Close()

		// Kafka protocol: ApiVersions/Metadata + topic visibility
		checkTopic(report, props, addr, *topic)

		// Diagnostics (best-effort)
		if *diag {
			bestEffortTraceroute(report, host)
			mtuCheck(report, host)
		}
	}

	// Produce/Consume (optional)
	if *topic != "" {
		probeProduceConsume(report, props, bootstrap, *topic, *group)
	}

	// Schema Registry (optional)
	checkSchemaRegistry(report, props)

	// Optional REST Proxy: set rest.proxy.url=...
	if rest := strings.TrimSpace(props["rest.proxy.url"]); rest != "" {
		checkDNS(report, extractHost(rest), "rest-proxy")
		tlsConf, _, err := tlsConfigFromProps(props, extractHost(rest))
		if err != nil {
			addRow(report, Row{"rest-proxy", rest, HTTP, FAIL, fmt.Sprintf("TLS config err: %v", err), ""})
		} else {
			client := httpClientFromTLS(tlsConf, 8*time.Second)
			req, _ := http.NewRequest("GET", strings.TrimRight(rest, "/")+"/topics", nil)
			resp, err := client.Do(req)
			if err != nil {
				addRow(report, Row{"rest-proxy", rest, HTTP, FAIL, fmt.Sprintf("GET /topics failed: %v", err), "Check listener/auth."})
			} else {
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

	report.FinishedAt = time.Now()
	summarize(report)
	printPretty(report)

	if *jsonOut != "" {
		if err := writeJSON(*jsonOut, report); err != nil {
			fmt.Fprintf(os.Stderr, "write json: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("JSON report written to %s\n", *jsonOut)
	}

	if *analyze {
		aiConfig, err := loadAIConfig()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading AI configuration: %v\n", err)
			os.Exit(1)
		}

		providerName := aiConfig.DefaultProvider
		if *provider != "" {
			providerName = *provider
		}

		providerConfig, ok := aiConfig.Providers[providerName]
		if !ok {
			fmt.Fprintf(os.Stderr, "Error: AI provider '%s' not found in ai_config.json\n", providerName)
			os.Exit(1)
		}

		if strings.HasPrefix(providerConfig.APIKey, "YOUR_") || providerConfig.APIKey == "" {
			fmt.Fprintf(os.Stderr, "Error: API key for provider '%s' is a placeholder. Please replace it with your actual API key in ai_config.json\n", providerName)
			os.Exit(1)
		}

		aiClient := NewAIClient(&providerConfig)
		fmt.Printf("\nSubmitting report for AI analysis using provider '%s'...\n", providerName)
		analysis, err := aiClient.AnalyzeReport(report)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error during AI analysis: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("\n--- AI Analysis ---")
		printIllustrativeAnalysis(analysis)
		fmt.Println("-------------------")

		if err := writeHTMLReport(report, analysis); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing HTML report: %v\n", err)
		} else {
			fmt.Println("\nAI analysis report written to analysis_report.html")
		}
	}
}

func writeHTMLReport(r *Report, analysis *AIAnalysisResponse) error {
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
		return fmt.Errorf("could not parse html template: %w", err)
	}

	// Create the output file
	file, err := os.Create("analysis_report.html")
	if err != nil {
		return fmt.Errorf("could not create html report file: %w", err)
	}
	defer file.Close()

	// Execute the template with the data and write to the file
	err = tmpl.Execute(file, data)
	if err != nil {
		return fmt.Errorf("could not execute html template: %w", err)
	}

	return nil
}

func writeJSON(path string, r *Report) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

func redactProps(p map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range p {
		lk := strings.ToLower(k)
		if strings.Contains(lk, "password") || strings.Contains(lk, "secret") || k == "sasl.oauthbearer.token" || strings.Contains(lk, "key") {
			out[k] = "***"
		} else {
			out[k] = v
		}
	}
	return out
}
