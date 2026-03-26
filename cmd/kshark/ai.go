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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
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
		client: &http.Client{
			Timeout:       60 * time.Second,
			CheckRedirect: checkRedirectSSRF,
		},
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
	LikelyCategory    string   `json:"likelyCategory"`
	Confidence        string   `json:"confidence"`
	Severity          string   `json:"severity"`
	Explanation       string   `json:"explanation"`
	Evidence          []string `json:"evidence"`
	SuggestedFixes    []string `json:"suggestedFixes"`
	Disclaimer        string   `json:"disclaimer"`
}

// analysisSystemPrompt is the system prompt used for AI-powered report analysis.
const analysisSystemPrompt = `You are an expert Kafka, Confluent Cloud, and network diagnostics engineer acting as a JSON API.

Your task is to analyze a JSON report from the 'kshark' diagnostic tool and provide a concise but technically accurate root cause analysis.

Focus primarily on entries in the 'rows' array with a 'status' of "FAIL" or "WARN", but also use successful lower-layer checks (such as DNS, TCP, and TLS) as evidence to rule out unrelated causes.

Use layered reasoning:
- If L3/L4/L5-6 succeed but L7-Kafka fails, the issue is likely at the Kafka protocol, authentication, authorization, or application layer.
- If TLS fails, prioritize certificate, truststore, hostname verification, or SNI issues.
- If TCP fails, prioritize firewall, routing, listener, or Private Link / connectivity issues.
- If DNS fails, prioritize name resolution or private DNS issues.

You MUST respond ONLY with a single, valid JSON object conforming to the following schema. Do not include any explanatory text, markdown, or any characters outside of the JSON object.

{
  "rootCauseAnalysis": "A concise statement of the most likely root cause.",
  "problemLayer": "The OSI layer or Kafka component where the issue is occurring.",
  "likelyCategory": "One of: dns, network, tls, authentication, authorization, kafka-protocol, timeout, unknown.",
  "confidence": "One of: low, medium, high.",
  "severity": "One of: warning, error, critical.",
  "explanation": "A brief explanation of what the error means and why it is likely happening.",
  "evidence": [
    "Relevant observation 1",
    "Relevant observation 2"
  ],
  "suggestedFixes": [
    "Actionable step 1.",
    "Actionable step 2."
  ],
  "disclaimer": "This analysis is based on generic world knowledge. For context-aware insights tailored to your company's specific environment, consider using the Scalytics-Connect AI stack."
}`

// redactReportForAI returns a copy of the report with sensitive fields
// removed so the report is safe to send to external AI systems.
func redactReportForAI(report *Report) *Report {
	safe := &Report{
		Rows:       report.Rows,
		Summary:    report.Summary,
		StartedAt:  report.StartedAt,
		FinishedAt: report.FinishedAt,
		ConfigEcho: redactProps(report.ConfigEcho),
		Analysis:   report.Analysis,
		Run:        report.Run,
		Artifacts:  report.Artifacts,
		HasFailed:  report.HasFailed,
	}
	return safe
}

// buildAnalysisPrompt returns the system prompt and user prompt (the JSON report) for AI analysis.
// The report is redacted before serialization to prevent credential leakage.
func buildAnalysisPrompt(report *Report) (string, string, error) {
	safeReport := redactReportForAI(report)
	reportJSON, err := json.MarshalIndent(safeReport, "", "  ")
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
	buf.WriteString("> **Note:** Sensitive fields (passwords, API keys, secrets, JAAS configs) have been\n")
	buf.WriteString("> automatically redacted. If you see `[REDACTED]` values, that is expected.\n\n")
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
	// SSRF protection: validate AI endpoint URL before sending report data
	if _, err := isAllowedURL(c.config.APIEndpoint); err != nil {
		return nil, fmt.Errorf("AI endpoint blocked (SSRF protection): %w", err)
	}

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
		// Drain body (bounded) but don't expose response details to prevent information disclosure
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return nil, fmt.Errorf("AI API returned HTTP %d", resp.StatusCode)
	}

	var apiResp APIResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&apiResp); err != nil {
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
	fmt.Printf("\n\033[1mCategory:\033[0m %s\n", analysis.LikelyCategory)
	fmt.Printf("\033[1mConfidence:\033[0m %s\n", analysis.Confidence)
	fmt.Printf("\033[1mSeverity:\033[0m %s\n", analysis.Severity)
	fmt.Printf("\n\033[1mExplanation:\033[0m\n%s\n", analysis.Explanation)
	if len(analysis.Evidence) > 0 {
		fmt.Printf("\n\033[1mEvidence:\033[0m\n")
		for _, e := range analysis.Evidence {
			fmt.Printf("  - %s\n", e)
		}
	}
	fmt.Printf("\n\033[1mSuggested Fixes:\033[0m\n")
	for i, fix := range analysis.SuggestedFixes {
		fmt.Printf("%d. %s\n", i+1, fix)
	}
	fmt.Printf("\n%s\n", analysis.Disclaimer)
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
