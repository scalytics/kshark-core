package probe

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"strings"
	"time"
)

// ElasticsearchProber probes Elasticsearch targets using raw HTTP over TCP.
type ElasticsearchProber struct{}

func NewElasticsearchProber() *ElasticsearchProber { return &ElasticsearchProber{} }
func (p *ElasticsearchProber) Type() string        { return "elasticsearch" }

const (
	esDefaultPort = 9200
)

func (p *ElasticsearchProber) Probe(ctx context.Context, target ProbeTarget) (steps []ProbeStep) {
	// Safety net: convert any remaining panic into a FAIL step.
	defer func() {
		if r := recover(); r != nil {
			steps = append(steps, ProbeStep{
				Layer:  "L7-Protocol",
				Status: StatusFAIL,
				Detail: fmt.Sprintf("protocol parser panic: %v", r),
				Hint:   "Server sent malformed or unexpected response. This may not be an Elasticsearch server.",
			})
		}
	}()

	// L3-DNS
	dnsStep := ProbeDNS(target.Host)
	steps = append(steps, dnsStep)
	if dnsStep.Status == StatusFAIL {
		return steps
	}

	// L4-TCP
	addr := fmt.Sprintf("%s:%d", target.Host, target.Port)
	conn, tcpStep := ProbeTCP(addr, 8*time.Second)
	steps = append(steps, tcpStep)
	if conn == nil {
		return steps
	}
	defer conn.Close()

	// L5-6-TLS (optional)
	var rwConn net.Conn = conn
	if target.TLS {
		tlsCfg, err := BuildTLSConfig(target.Host, target.ExtraProps["sslrootcert"], false)
		if err != nil {
			steps = append(steps, ProbeStep{
				Layer:  "L5-6-TLS",
				Status: StatusFAIL,
				Detail: fmt.Sprintf("TLS config error: %v", err),
				Hint:   "Check SSL configuration for Elasticsearch.",
			})
			return steps
		}
		tlsConn, tlsStep := ProbeTLS(conn, target.Host, tlsCfg)
		steps = append(steps, tlsStep)
		if tlsConn == nil {
			return steps
		}
		rwConn = tlsConn
	} else {
		steps = append(steps, ProbeStep{
			Layer:  "L5-6-TLS",
			Status: StatusSKIP,
			Detail: "SSL not configured",
		})
	}

	timeout := 10 * time.Second

	// L7-HTTP: send GET / without auth first
	httpStart := time.Now()
	statusCode, body, err := esHTTPGet(rwConn, target.Host, target.Port, "", "", timeout)
	httpLatency := time.Since(httpStart)

	if err != nil {
		steps = append(steps, ProbeStep{
			Layer:   "L7-HTTP",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("HTTP request failed: %v", ScrubCredentials(err.Error())),
			Hint:    "Server may not be an Elasticsearch instance or port is wrong.",
			Latency: httpLatency,
		})
		return steps
	}

	// If 401 Unauthorized and we have credentials, try again with auth
	if statusCode == 401 && (target.Username != "" || target.Password != "") {
		steps = append(steps, ProbeStep{
			Layer:   "L7-HTTP",
			Status:  StatusOK,
			Detail:  fmt.Sprintf("HTTP %d - server requires authentication", statusCode),
			Latency: httpLatency,
		})

		// Need a new connection for the authenticated request
		conn2, tcpStep2 := ProbeTCP(addr, 8*time.Second)
		if conn2 == nil {
			steps = append(steps, ProbeStep{
				Layer:   "L7-Auth",
				Status:  StatusFAIL,
				Detail:  fmt.Sprintf("Failed to reconnect for auth: %s", tcpStep2.Detail),
				Hint:    "Network connectivity issue during authentication retry.",
				Latency: tcpStep2.Latency,
			})
			return steps
		}
		defer conn2.Close()

		var rwConn2 net.Conn = conn2
		if target.TLS {
			tlsCfg, _ := BuildTLSConfig(target.Host, target.ExtraProps["sslrootcert"], false)
			tlsConn2, tlsStep2 := ProbeTLS(conn2, target.Host, tlsCfg)
			if tlsConn2 == nil {
				steps = append(steps, ProbeStep{
					Layer:   "L7-Auth",
					Status:  StatusFAIL,
					Detail:  fmt.Sprintf("TLS reconnect failed for auth: %s", tlsStep2.Detail),
					Latency: tlsStep2.Latency,
				})
				return steps
			}
			rwConn2 = tlsConn2
		}

		authStart := time.Now()
		statusCode, body, err = esHTTPGet(rwConn2, target.Host, target.Port, target.Username, target.Password, timeout)
		authLatency := time.Since(authStart)

		if err != nil {
			steps = append(steps, ProbeStep{
				Layer:   "L7-Auth",
				Status:  StatusFAIL,
				Detail:  fmt.Sprintf("Authenticated HTTP request failed: %v", ScrubCredentials(err.Error())),
				Hint:    "Check Elasticsearch credentials and network connectivity.",
				Latency: authLatency,
			})
			return steps
		}

		authStep := classifyESAuthResponse(statusCode, target, authLatency)
		steps = append(steps, authStep)
		if authStep.Status == StatusFAIL {
			return steps
		}
	} else if statusCode == 401 {
		// 401 but no credentials provided
		steps = append(steps, ProbeStep{
			Layer:   "L7-HTTP",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("HTTP %d - server requires authentication but no credentials provided", statusCode),
			Hint:    "Set connection.user and connection.password in connector config.",
			Latency: httpLatency,
		})
		return steps
	} else if statusCode == 403 {
		steps = append(steps, ProbeStep{
			Layer:   "L7-HTTP",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("HTTP %d - access forbidden", statusCode),
			Hint:    "Check Elasticsearch user permissions and roles.",
			Latency: httpLatency,
		})
		return steps
	} else if statusCode >= 400 {
		steps = append(steps, ProbeStep{
			Layer:   "L7-HTTP",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("HTTP %d error from server", statusCode),
			Hint:    "Check Elasticsearch server status and configuration.",
			Latency: httpLatency,
		})
		return steps
	} else {
		steps = append(steps, ProbeStep{
			Layer:   "L7-HTTP",
			Status:  StatusOK,
			Detail:  fmt.Sprintf("HTTP %d OK", statusCode),
			Latency: httpLatency,
		})
	}

	// L7-Cluster: parse JSON response for cluster_name and version
	clusterName, version := parseESRootResponse(body)
	if clusterName != "" || version != "" {
		detail := "Elasticsearch cluster"
		if clusterName != "" && version != "" {
			detail = fmt.Sprintf("Elasticsearch cluster '%s', version %s", clusterName, version)
		} else if clusterName != "" {
			detail = fmt.Sprintf("Elasticsearch cluster '%s'", clusterName)
		} else if version != "" {
			detail = fmt.Sprintf("Elasticsearch version %s", version)
		}
		steps = append(steps, ProbeStep{
			Layer:  "L7-Cluster",
			Status: StatusOK,
			Detail: detail,
		})
	} else {
		steps = append(steps, ProbeStep{
			Layer:  "L7-Cluster",
			Status: StatusWARN,
			Detail: "Could not parse cluster info from response",
			Hint:   "Response may not be from an Elasticsearch server.",
		})
	}

	return steps
}

// esHTTPGet sends a raw HTTP GET / request and returns status code and body.
func esHTTPGet(conn net.Conn, host string, port int, username, password string, timeout time.Duration) (int, string, error) {
	// Build HTTP request
	hostHeader := host
	if port != 80 && port != 443 {
		hostHeader = fmt.Sprintf("%s:%d", host, port)
	}

	var req strings.Builder
	req.WriteString("GET / HTTP/1.1\r\n")
	req.WriteString(fmt.Sprintf("Host: %s\r\n", hostHeader))
	req.WriteString("Accept: application/json\r\n")
	req.WriteString("Connection: close\r\n")

	if username != "" || password != "" {
		creds := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", username, password)))
		req.WriteString(fmt.Sprintf("Authorization: Basic %s\r\n", creds))
	}

	req.WriteString("\r\n")

	conn.SetWriteDeadline(time.Now().Add(timeout))
	if _, err := conn.Write([]byte(req.String())); err != nil {
		return 0, "", fmt.Errorf("write failed: %w", err)
	}

	conn.SetReadDeadline(time.Now().Add(timeout))
	return readHTTPResponse(conn)
}

// readHTTPResponse reads a raw HTTP response and returns status code and body.
func readHTTPResponse(conn net.Conn) (int, string, error) {
	reader := bufio.NewReaderSize(conn, 65536)

	// Read status line
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		return 0, "", fmt.Errorf("failed to read status line: %w", err)
	}
	statusLine = strings.TrimRight(statusLine, "\r\n")

	// Parse status code from "HTTP/1.1 200 OK"
	statusCode, err := parseHTTPStatusCode(statusLine)
	if err != nil {
		return 0, "", err
	}

	// Read headers until empty line
	contentLength := -1
	chunked := false
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return statusCode, "", fmt.Errorf("failed to read headers: %w", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}

		lower := strings.ToLower(line)
		if strings.HasPrefix(lower, "content-length:") {
			lenStr := strings.TrimSpace(strings.TrimPrefix(lower, "content-length:"))
			fmt.Sscanf(lenStr, "%d", &contentLength)
		}
		if strings.HasPrefix(lower, "transfer-encoding:") && strings.Contains(lower, "chunked") {
			chunked = true
		}
	}

	// Read body
	var body string
	if contentLength > 0 {
		if contentLength > 1<<20 { // 1MB safety limit
			contentLength = 1 << 20
		}
		buf := make([]byte, contentLength)
		n, _ := readFullFromBufio(reader, buf)
		body = string(buf[:n])
	} else if chunked {
		body = readChunkedBody(reader)
	} else {
		// Read whatever is available (connection: close)
		var sb strings.Builder
		buf := make([]byte, 4096)
		for {
			n, err := reader.Read(buf)
			if n > 0 {
				sb.Write(buf[:n])
				if sb.Len() > 1<<20 { // 1MB safety limit
					break
				}
			}
			if err != nil {
				break
			}
		}
		body = sb.String()
	}

	return statusCode, body, nil
}

// readFullFromBufio reads exactly len(buf) bytes from a bufio.Reader.
func readFullFromBufio(reader *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := reader.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// readChunkedBody reads a chunked transfer-encoded body.
func readChunkedBody(reader *bufio.Reader) string {
	var sb strings.Builder
	for {
		// Read chunk size line
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimRight(line, "\r\n")
		var chunkSize int
		if _, err := fmt.Sscanf(line, "%x", &chunkSize); err != nil {
			break
		}
		if chunkSize == 0 {
			break
		}
		if chunkSize > 1<<20 { // 1MB safety limit
			break
		}

		// Read chunk data
		buf := make([]byte, chunkSize)
		n, err := readFullFromBufio(reader, buf)
		sb.Write(buf[:n])
		if err != nil {
			break
		}

		// Read trailing \r\n after chunk data
		reader.ReadString('\n')

		if sb.Len() > 1<<20 {
			break
		}
	}
	return sb.String()
}

// parseHTTPStatusCode extracts the status code from an HTTP status line.
func parseHTTPStatusCode(statusLine string) (int, error) {
	// "HTTP/1.1 200 OK" or "HTTP/1.0 200 OK"
	parts := strings.SplitN(statusLine, " ", 3)
	if len(parts) < 2 {
		return 0, fmt.Errorf("malformed HTTP status line: %s", statusLine)
	}
	if !strings.HasPrefix(parts[0], "HTTP/") {
		return 0, fmt.Errorf("not an HTTP response: %s", statusLine)
	}

	var code int
	if _, err := fmt.Sscanf(parts[1], "%d", &code); err != nil {
		return 0, fmt.Errorf("invalid HTTP status code: %s", parts[1])
	}
	return code, nil
}

// classifyESAuthResponse classifies the Elasticsearch authenticated response.
func classifyESAuthResponse(statusCode int, target ProbeTarget, latency time.Duration) ProbeStep {
	switch {
	case statusCode == 200:
		detail := "Authenticated"
		if target.Username != "" {
			detail = fmt.Sprintf("Authenticated as '%s'", target.Username)
		}
		return ProbeStep{
			Layer:   "L7-Auth",
			Status:  StatusOK,
			Detail:  detail,
			Latency: latency,
		}
	case statusCode == 401:
		return ProbeStep{
			Layer:   "L7-Auth",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("HTTP %d - authentication failed", statusCode),
			Hint:    "Invalid username or password. Check connection.user and connection.password.",
			Latency: latency,
		}
	case statusCode == 403:
		return ProbeStep{
			Layer:   "L7-Auth",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("HTTP %d - access forbidden", statusCode),
			Hint:    "User lacks required permissions. Check Elasticsearch roles and privileges.",
			Latency: latency,
		}
	default:
		return ProbeStep{
			Layer:   "L7-Auth",
			Status:  StatusWARN,
			Detail:  fmt.Sprintf("HTTP %d from authenticated request", statusCode),
			Hint:    "Check Elasticsearch server status and user permissions.",
			Latency: latency,
		}
	}
}

// parseESRootResponse extracts cluster_name and version.number from the
// Elasticsearch root endpoint JSON response using simple string parsing
// (no encoding/json dependency to keep it minimal and consistent with
// wire protocol style).
func parseESRootResponse(body string) (clusterName, version string) {
	clusterName = extractJSONStringValue(body, "cluster_name")
	version = extractJSONStringValue(body, "number")
	return clusterName, version
}

// extractJSONStringValue does a simple extraction of "key" : "value" from JSON text.
// This avoids importing encoding/json for a simple probe.
func extractJSONStringValue(json, key string) string {
	// Look for "key" : "value" pattern
	searchKey := fmt.Sprintf(`"%s"`, key)
	idx := strings.Index(json, searchKey)
	if idx < 0 {
		return ""
	}

	rest := json[idx+len(searchKey):]
	// Skip whitespace and colon
	rest = strings.TrimLeft(rest, " \t\n\r")
	if len(rest) == 0 || rest[0] != ':' {
		return ""
	}
	rest = rest[1:]
	rest = strings.TrimLeft(rest, " \t\n\r")

	if len(rest) == 0 || rest[0] != '"' {
		return ""
	}
	rest = rest[1:]

	// Find closing quote (handle escaped quotes)
	var value strings.Builder
	for i := 0; i < len(rest); i++ {
		if rest[i] == '\\' && i+1 < len(rest) {
			value.WriteByte(rest[i+1])
			i++
			continue
		}
		if rest[i] == '"' {
			return value.String()
		}
		value.WriteByte(rest[i])
	}
	return ""
}
