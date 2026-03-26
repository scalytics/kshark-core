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
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// ---------- Schema Registry / REST ----------

func httpClientFromTLS(tlsConf *tls.Config, timeout time.Duration) *http.Client {
	tr := &http.Transport{
		TLSClientConfig: tlsConf,
		Proxy:           http.ProxyFromEnvironment,
		IdleConnTimeout: 10 * time.Second,
	}
	return &http.Client{
		Transport:     tr,
		Timeout:       timeout,
		CheckRedirect: checkRedirectSSRF,
	}
}

func checkSchemaRegistry(ctx context.Context, r *Report, p map[string]string) {
	url := strings.TrimSpace(p["schema.registry.url"])
	if url == "" {
		return
	}

	// SSRF validation
	if warning, err := isAllowedURL(url); err != nil {
		addRow(r, Row{"schema-reg", url, HTTP, FAIL,
			fmt.Sprintf("URL blocked (SSRF protection): %v", err),
			"URL must not point to loopback, link-local, or cloud metadata. RFC1918 is allowed for PrivateLink."})
		return
	} else if warning != "" {
		addRow(r, Row{"schema-reg", url, HTTP, WARN, warning, ""})
	}
	host := extractHost(url)
	dnsStart := time.Now()
	if _, err := net.LookupHost(host); err != nil {
		slog.Debug("schema registry dns", "host", host, "dur", time.Since(dnsStart).Truncate(time.Millisecond), "err", err)
		addRow(r, Row{"schema-reg", host, L3, FAIL, "DNS failed", "Fix DNS/VPN."})
	} else {
		slog.Debug("schema registry dns ok", "host", host, "dur", time.Since(dnsStart).Truncate(time.Millisecond))
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
		slog.Debug("schema registry http", "url", url, "dur", time.Since(httpStart).Truncate(time.Millisecond), "err", err)
		addRow(r, Row{"schema-reg", url, HTTP, FAIL, fmt.Sprintf("GET /subjects failed: %v", err), "TLS/host/network or auth."})
		return
	}
	slog.Debug("schema registry http ok", "url", url, "dur", time.Since(httpStart).Truncate(time.Millisecond), "status", resp.StatusCode)
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
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
