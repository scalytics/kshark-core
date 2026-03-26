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
	"log/slog"
	"net"
	"net/http"
	neturl "net/url"
	"strings"
)

// ---------- SSRF Protection ----------
//
// Two-tier model for PrivateLink-aware SSRF protection:
//   DENY  (hard-block): loopback, link-local (cloud metadata), multicast, reserved
//   WARN  (allow with log): RFC1918 / IPv6 ULA — legitimate in PrivateLink environments

var (
	ssrfDenyNets []*net.IPNet
	ssrfWarnNets []*net.IPNet
)

func init() {
	for _, cidr := range []string{
		"127.0.0.0/8",        // IPv4 loopback
		"::1/128",            // IPv6 loopback
		"169.254.0.0/16",     // link-local (cloud metadata 169.254.169.254)
		"fe80::/10",          // IPv6 link-local
		"0.0.0.0/8",          // current network
		"100.64.0.0/10",      // CGN / shared address space
		"192.0.0.0/24",       // IETF protocol assignments
		"192.0.2.0/24",       // TEST-NET-1
		"198.51.100.0/24",    // TEST-NET-2
		"203.0.113.0/24",     // TEST-NET-3
		"198.18.0.0/15",      // benchmarking
		"224.0.0.0/4",        // multicast
		"240.0.0.0/4",        // reserved
		"255.255.255.255/32", // broadcast
	} {
		_, subnet, _ := net.ParseCIDR(cidr)
		ssrfDenyNets = append(ssrfDenyNets, subnet)
	}

	for _, cidr := range []string{
		"10.0.0.0/8",     // RFC1918
		"172.16.0.0/12",  // RFC1918
		"192.168.0.0/16", // RFC1918
		"fc00::/7",       // IPv6 ULA
	} {
		_, subnet, _ := net.ParseCIDR(cidr)
		ssrfWarnNets = append(ssrfWarnNets, subnet)
	}
}

type ssrfVerdict int

const (
	ssrfAllow ssrfVerdict = iota
	ssrfWarn              // RFC1918 — allowed but logged
	ssrfDeny              // loopback, link-local, metadata — always blocked
)

func classifyIP(ip net.IP) ssrfVerdict {
	for _, n := range ssrfDenyNets {
		if n.Contains(ip) {
			return ssrfDeny
		}
	}
	for _, n := range ssrfWarnNets {
		if n.Contains(ip) {
			return ssrfWarn
		}
	}
	return ssrfAllow
}

// isAllowedURL validates that a URL is safe from an SSRF perspective.
func isAllowedURL(rawURL string) (warning string, err error) {
	u, err := neturl.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("invalid URL: %w", err)
	}

	switch u.Scheme {
	case "https", "http":
		// allowed
	default:
		return "", fmt.Errorf("only http/https schemes allowed, got %q", u.Scheme)
	}

	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("URL has no hostname")
	}

	if u.User != nil {
		return "", fmt.Errorf("URLs with embedded credentials are not allowed")
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		return "", fmt.Errorf("cannot resolve hostname %q: %w", host, err)
	}

	var worstVerdict ssrfVerdict
	var resolvedIPs []string
	for _, ip := range ips {
		resolvedIPs = append(resolvedIPs, ip.String())
		if v := classifyIP(ip); v > worstVerdict {
			worstVerdict = v
		}
	}

	switch worstVerdict {
	case ssrfDeny:
		return "", fmt.Errorf("blocked: %s resolves to loopback/link-local/reserved address (%s)",
			host, strings.Join(resolvedIPs, ", "))
	case ssrfWarn:
		w := fmt.Sprintf("SSRF-WARN: %s resolves to private (RFC1918) address (%s); "+
			"allowing because PrivateLink/VPC targets are common", host, strings.Join(resolvedIPs, ", "))
		slog.Warn("ssrf private address", "warning", w)
		return w, nil
	}

	return "", nil
}

// checkRedirectSSRF blocks redirects to loopback/link-local/metadata IPs.
func checkRedirectSSRF(req *http.Request, via []*http.Request) error {
	if len(via) >= 3 {
		return fmt.Errorf("stopped after 3 redirects")
	}
	_, err := isAllowedURL(req.URL.String())
	if err != nil {
		return fmt.Errorf("redirect to %s blocked: %w", req.URL.Redacted(), err)
	}
	return nil
}
