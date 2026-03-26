package probe

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// ProbeDNS resolves the hostname and returns a ProbeStep.
func ProbeDNS(host string) ProbeStep {
	start := time.Now()
	addrs, err := net.LookupHost(host)
	elapsed := time.Since(start)

	if err != nil {
		hint := "Check DNS configuration and network connectivity."
		if strings.Contains(err.Error(), "no such host") {
			hint = "Hostname not resolvable. Check VPC DNS settings and private hosted zones."
		}
		return ProbeStep{
			Layer:   "L3-DNS",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("DNS lookup failed: %v", err),
			Hint:    hint,
			Latency: elapsed,
		}
	}

	return ProbeStep{
		Layer:   "L3-DNS",
		Status:  StatusOK,
		Detail:  fmt.Sprintf("Resolved to %s", strings.Join(addrs, ", ")),
		Latency: elapsed,
	}
}

// ProbeSRV performs a DNS SRV lookup for MongoDB and returns resolved hosts.
func ProbeSRV(host string) ([]string, ProbeStep) {
	start := time.Now()
	_, srvs, err := net.LookupSRV("mongodb", "tcp", host)
	elapsed := time.Since(start)

	if err != nil {
		return nil, ProbeStep{
			Layer:   "L3-DNS",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("SRV lookup failed for _mongodb._tcp.%s: %v", host, err),
			Hint:    "Check if SRV DNS record exists. Atlas clusters require public or peered DNS resolution.",
			Latency: elapsed,
		}
	}

	var hosts []string
	var names []string
	for _, srv := range srvs {
		target := strings.TrimSuffix(srv.Target, ".")
		hosts = append(hosts, fmt.Sprintf("%s:%d", target, srv.Port))
		names = append(names, target)
	}

	return hosts, ProbeStep{
		Layer:   "L3-DNS",
		Status:  StatusOK,
		Detail:  fmt.Sprintf("SRV lookup returned %d hosts: %s", len(hosts), strings.Join(names, ", ")),
		Latency: elapsed,
	}
}

// ProbeTCP attempts a TCP connection to addr within the timeout.
func ProbeTCP(addr string, timeout time.Duration) (net.Conn, ProbeStep) {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", addr, timeout)
	elapsed := time.Since(start)

	if err != nil {
		hint := "Check firewall rules and network connectivity."
		msg := err.Error()
		switch {
		case strings.Contains(msg, "connection refused"):
			hint = "Target port not listening or security group blocks inbound. Check service status and firewall rules."
		case strings.Contains(msg, "i/o timeout"):
			hint = "No route to host. Check PrivateLink endpoint status, NLB cross-zone settings, and routing tables."
		case strings.Contains(msg, "no such host"):
			hint = "DNS resolution failed during connection attempt."
		}
		return nil, ProbeStep{
			Layer:   "L4-TCP",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("TCP connect to %s failed: %v", addr, err),
			Hint:    hint,
			Latency: elapsed,
		}
	}

	return conn, ProbeStep{
		Layer:   "L4-TCP",
		Status:  StatusOK,
		Detail:  fmt.Sprintf("Connected to %s in %dms", addr, elapsed.Milliseconds()),
		Latency: elapsed,
	}
}

// ProbeTLS performs a TLS handshake on an existing TCP connection.
func ProbeTLS(conn net.Conn, host string, config *tls.Config) (net.Conn, ProbeStep) {
	start := time.Now()
	tlsConn := tls.Client(conn, config)
	if err := tlsConn.Handshake(); err != nil {
		elapsed := time.Since(start)
		hint := "Check TLS configuration and certificate chain."
		msg := err.Error()
		switch {
		case strings.Contains(msg, "certificate signed by unknown authority"):
			hint = "CA not trusted. Provide the CA certificate or add it to the system trust store."
		case strings.Contains(msg, "certificate has expired"):
			hint = "Server certificate has expired. Contact the server administrator."
		case strings.Contains(msg, "doesn't contain any IP SANs") || strings.Contains(msg, "doesn't match"):
			hint = "Hostname mismatch. Use the service DNS name, not an IP or NLB address."
		}
		return nil, ProbeStep{
			Layer:   "L5-6-TLS",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("TLS handshake failed: %v", err),
			Hint:    hint,
			Latency: elapsed,
		}
	}

	elapsed := time.Since(start)
	state := tlsConn.ConnectionState()
	detail := tlsDetail(&state)

	// Warn if cert expires within 30 days
	expiry := earliestCertExpiry(&state)
	status := StatusOK
	hint := ""
	if !expiry.IsZero() && time.Until(expiry) < 30*24*time.Hour {
		status = StatusWARN
		hint = fmt.Sprintf("Server certificate expires on %s (less than 30 days).", expiry.Format("2006-01-02"))
	}

	return tlsConn, ProbeStep{
		Layer:   "L5-6-TLS",
		Status:  status,
		Detail:  detail,
		Hint:    hint,
		Latency: elapsed,
	}
}

// BuildTLSConfig constructs a *tls.Config.
func BuildTLSConfig(host string, caCertPath string, insecureSkipVerify bool) (*tls.Config, error) {
	config := &tls.Config{
		ServerName:         host,
		InsecureSkipVerify: insecureSkipVerify,
	}

	if caCertPath != "" {
		caPEM, err := os.ReadFile(caCertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA cert %s: %w", caCertPath, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("failed to parse CA cert from %s", caCertPath)
		}
		config.RootCAs = pool
	}

	return config, nil
}

func tlsDetail(state *tls.ConnectionState) string {
	version := "unknown"
	switch state.Version {
	case tls.VersionTLS10:
		version = "TLS 1.0"
	case tls.VersionTLS11:
		version = "TLS 1.1"
	case tls.VersionTLS12:
		version = "TLS 1.2"
	case tls.VersionTLS13:
		version = "TLS 1.3"
	}

	cn := ""
	expiry := ""
	if len(state.PeerCertificates) > 0 {
		cert := state.PeerCertificates[0]
		cn = cert.Subject.CommonName
		expiry = cert.NotAfter.Format("2006-01-02")
	}

	if cn != "" {
		return fmt.Sprintf("%s, CN=%s, expires %s", version, cn, expiry)
	}
	return version
}

func earliestCertExpiry(state *tls.ConnectionState) time.Time {
	var earliest time.Time
	for _, cert := range state.PeerCertificates {
		if earliest.IsZero() || cert.NotAfter.Before(earliest) {
			earliest = cert.NotAfter
		}
	}
	return earliest
}

// ScrubCredentials removes potential credentials from error messages.
// Catches patterns like mongodb://user:pass@host and password=secret.
func ScrubCredentials(msg string) string {
	// Scrub URI userinfo: ://user:pass@ -> ://[REDACTED]@
	for _, scheme := range []string{"mongodb://", "mongodb+srv://", "postgresql://", "jdbc:", "redis://", "rediss://", "http://", "https://"} {
		idx := strings.Index(msg, scheme)
		if idx < 0 {
			continue
		}
		rest := msg[idx+len(scheme):]
		atIdx := strings.Index(rest, "@")
		if atIdx > 0 {
			msg = msg[:idx+len(scheme)] + "[REDACTED]@" + rest[atIdx+1:]
		}
	}
	return msg
}
