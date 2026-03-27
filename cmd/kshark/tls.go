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
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"
)

// ---------- DNS, TCP, TLS ----------

func checkDNS(ctx context.Context, r *Report, host string, component string) {
	start := time.Now()
	resolver := net.DefaultResolver
	_, err := resolver.LookupHost(ctx, host)
	slog.Debug("dns lookup", "host", host, "dur", time.Since(start).Truncate(time.Millisecond), "err", err)
	if err != nil {
		addRow(r, Row{component, host, L3, FAIL, fmt.Sprintf("DNS lookup failed: %v", err),
			"Check /etc/hosts, DNS server, split-horizon/VPN search domains."})
	} else {
		addRow(r, Row{component, host, L3, OK, "Resolved host", ""})
	}
}

func checkTCP(ctx context.Context, r *Report, addr string, component string, timeout time.Duration) net.Conn {
	start := time.Now()
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		slog.Debug("tcp connect", "addr", addr, "dur", time.Since(start).Truncate(time.Millisecond), "err", err)
		addRow(r, Row{component, addr, L4, FAIL, fmt.Sprintf("TCP connect failed: %v", err),
			"Firewall, SG/NACL/NSG, LB listeners, PodNetworkPolicy, or routing."})
		return nil
	}
	lat := time.Since(start)
	slog.Debug("tcp connect ok", "addr", addr, "dur", lat.Truncate(time.Millisecond))
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

func wrapTLS(ctx context.Context, r *Report, base net.Conn, tlsConf *tls.Config, component, addr string) net.Conn {
	if tlsConf == nil {
		addRow(r, Row{component, addr, L56, SKIP, "TLS not configured (PLAINTEXT)", "Prefer SSL/SASL_SSL for encryption."})
		return base
	}
	client := tls.Client(base, tlsConf)
	start := time.Now()
	if err := client.Handshake(); err != nil {
		slog.Debug("tls handshake", "addr", addr, "dur", time.Since(start).Truncate(time.Millisecond), "err", err)
		addRow(r, Row{component, addr, L56, FAIL, fmt.Sprintf("TLS handshake failed: %v", err),
			"Check CA chain, SNI/hostname, client cert/key, and server certificate validity."})
		return nil
	}
	state := client.ConnectionState()
	slog.Debug("tls handshake ok", "addr", addr, "dur", time.Since(start).Truncate(time.Millisecond))
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
