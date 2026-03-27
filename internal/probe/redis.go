package probe

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"time"
)

// RedisProber probes Redis targets using the RESP wire protocol.
type RedisProber struct{}

func NewRedisProber() *RedisProber { return &RedisProber{} }
func (p *RedisProber) Type() string { return "redis" }

const (
	redisDefaultPort = 6379
)

func (p *RedisProber) Probe(ctx context.Context, target ProbeTarget) (steps []ProbeStep) {
	// Safety net: convert any remaining panic into a FAIL step.
	defer func() {
		if r := recover(); r != nil {
			steps = append(steps, ProbeStep{
				Layer:  "L7-Protocol",
				Status: StatusFAIL,
				Detail: fmt.Sprintf("protocol parser panic: %v", r),
				Hint:   "Server sent malformed or unexpected response. This may not be a Redis server.",
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
				Hint:   "Check SSL configuration for Redis.",
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

	// L7-Redis-Auth: if credentials are set, authenticate before PING
	if target.Password != "" {
		authStart := time.Now()
		var authCmd string
		if target.Username != "" {
			// Redis 6+ ACL: AUTH username password
			authCmd = fmt.Sprintf("*3\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
				len(target.Username), target.Username,
				len(target.Password), target.Password)
		} else {
			// Legacy AUTH: AUTH password
			authCmd = fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n",
				len(target.Password), target.Password)
		}

		rwConn.SetWriteDeadline(time.Now().Add(timeout))
		if _, err := rwConn.Write([]byte(authCmd)); err != nil {
			steps = append(steps, ProbeStep{
				Layer:   "L7-Redis-Auth",
				Status:  StatusFAIL,
				Detail:  fmt.Sprintf("Failed to send AUTH command: %v", ScrubCredentials(err.Error())),
				Hint:    "Check network connectivity to Redis server.",
				Latency: time.Since(authStart),
			})
			return steps
		}

		rwConn.SetReadDeadline(time.Now().Add(timeout))
		authResp, err := readRedisLine(rwConn)
		authLatency := time.Since(authStart)

		if err != nil {
			steps = append(steps, ProbeStep{
				Layer:   "L7-Redis-Auth",
				Status:  StatusFAIL,
				Detail:  fmt.Sprintf("Failed to read AUTH response: %v", ScrubCredentials(err.Error())),
				Hint:    "Check Redis server logs for authentication errors.",
				Latency: authLatency,
			})
			return steps
		}

		authStep := classifyRedisAuthResponse(authResp, target, authLatency)
		steps = append(steps, authStep)
		if authStep.Status == StatusFAIL {
			return steps
		}
	}

	// L7-Redis-Ping: send PING, expect PONG
	pingStart := time.Now()
	pingCmd := "*1\r\n$4\r\nPING\r\n"

	rwConn.SetWriteDeadline(time.Now().Add(timeout))
	if _, err := rwConn.Write([]byte(pingCmd)); err != nil {
		steps = append(steps, ProbeStep{
			Layer:   "L7-Redis-Ping",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("Failed to send PING command: %v", ScrubCredentials(err.Error())),
			Hint:    "Check network connectivity to Redis server.",
			Latency: time.Since(pingStart),
		})
		return steps
	}

	rwConn.SetReadDeadline(time.Now().Add(timeout))
	pingResp, err := readRedisLine(rwConn)
	pingLatency := time.Since(pingStart)

	if err != nil {
		steps = append(steps, ProbeStep{
			Layer:   "L7-Redis-Ping",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("Failed to read PING response: %v", ScrubCredentials(err.Error())),
			Hint:    "Server may not be a Redis instance or port is wrong.",
			Latency: pingLatency,
		})
		return steps
	}

	pingStep := classifyRedisPingResponse(pingResp, pingLatency)
	steps = append(steps, pingStep)
	if pingStep.Status == StatusFAIL {
		return steps
	}

	// L7-Redis-Info: send INFO server to get Redis version
	infoStart := time.Now()
	infoCmd := "*2\r\n$4\r\nINFO\r\n$6\r\nserver\r\n"

	rwConn.SetWriteDeadline(time.Now().Add(timeout))
	if _, err := rwConn.Write([]byte(infoCmd)); err != nil {
		// Non-fatal: INFO failure is just a WARN
		steps = append(steps, ProbeStep{
			Layer:   "L7-Redis-Info",
			Status:  StatusWARN,
			Detail:  fmt.Sprintf("Failed to send INFO command: %v", ScrubCredentials(err.Error())),
			Latency: time.Since(infoStart),
		})
		return steps
	}

	rwConn.SetReadDeadline(time.Now().Add(timeout))
	infoResp, err := readRedisBulkString(rwConn)
	infoLatency := time.Since(infoStart)

	if err != nil {
		steps = append(steps, ProbeStep{
			Layer:   "L7-Redis-Info",
			Status:  StatusWARN,
			Detail:  fmt.Sprintf("Failed to read INFO response: %v", ScrubCredentials(err.Error())),
			Latency: infoLatency,
		})
		return steps
	}

	version := parseRedisInfoVersion(infoResp)
	if version != "" {
		steps = append(steps, ProbeStep{
			Layer:   "L7-Redis-Info",
			Status:  StatusOK,
			Detail:  fmt.Sprintf("Redis server version %s", version),
			Latency: infoLatency,
		})
	} else {
		steps = append(steps, ProbeStep{
			Layer:   "L7-Redis-Info",
			Status:  StatusOK,
			Detail:  "Redis server responded to INFO",
			Latency: infoLatency,
		})
	}

	return steps
}

// readRedisLine reads a single RESP simple string, error, or integer line.
// Reads until \r\n.
func readRedisLine(conn net.Conn) (string, error) {
	reader := bufio.NewReaderSize(conn, 4096)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read error: %w", err)
	}
	// Strip trailing \r\n
	line = strings.TrimRight(line, "\r\n")
	return line, nil
}

// readRedisBulkString reads a RESP bulk string response ($len\r\ndata\r\n).
func readRedisBulkString(conn net.Conn) (string, error) {
	reader := bufio.NewReaderSize(conn, 65536)

	// Read the type/length line
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read error: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")

	if len(line) == 0 {
		return "", fmt.Errorf("empty response")
	}

	// Check for error response
	if line[0] == '-' {
		return line, nil
	}

	// Check for bulk string
	if line[0] == '$' {
		lenStr := line[1:]
		var dataLen int
		if _, err := fmt.Sscanf(lenStr, "%d", &dataLen); err != nil {
			return "", fmt.Errorf("invalid bulk string length: %s", lenStr)
		}
		if dataLen < 0 {
			return "", nil // null bulk string
		}
		if dataLen > 1<<20 { // 1MB safety limit
			return "", fmt.Errorf("bulk string too large: %d bytes", dataLen)
		}

		// Read dataLen + 2 bytes (\r\n)
		buf := make([]byte, dataLen+2)
		n, err := readFull(reader, buf)
		if err != nil {
			return string(buf[:n]), fmt.Errorf("short read: %w", err)
		}
		return string(buf[:dataLen]), nil
	}

	// Simple string or other type
	return line, nil
}

// readFull reads exactly len(buf) bytes from reader.
func readFull(reader *bufio.Reader, buf []byte) (int, error) {
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

// classifyRedisPingResponse classifies the Redis PING response.
func classifyRedisPingResponse(resp string, latency time.Duration) ProbeStep {
	if resp == "" {
		return ProbeStep{
			Layer:   "L7-Redis-Ping",
			Status:  StatusFAIL,
			Detail:  "Empty PING response from server",
			Hint:    "Check Redis server status.",
			Latency: latency,
		}
	}

	switch {
	case resp == "+PONG":
		return ProbeStep{
			Layer:   "L7-Redis-Ping",
			Status:  StatusOK,
			Detail:  "Redis PONG received",
			Latency: latency,
		}
	case strings.HasPrefix(resp, "-NOAUTH"):
		return ProbeStep{
			Layer:   "L7-Redis-Ping",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("Redis requires authentication: %s", resp),
			Hint:    "Set redis.password in connector config. Redis server requires AUTH.",
			Latency: latency,
		}
	case strings.HasPrefix(resp, "-"):
		return ProbeStep{
			Layer:   "L7-Redis-Ping",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("Redis error: %s", ScrubCredentials(resp)),
			Hint:    "Check Redis server configuration and connectivity.",
			Latency: latency,
		}
	default:
		return ProbeStep{
			Layer:   "L7-Redis-Ping",
			Status:  StatusWARN,
			Detail:  fmt.Sprintf("Unexpected PING response: %s", ScrubCredentials(resp)),
			Hint:    "Server may not be a Redis instance.",
			Latency: latency,
		}
	}
}

// classifyRedisAuthResponse classifies the Redis AUTH response.
func classifyRedisAuthResponse(resp string, target ProbeTarget, latency time.Duration) ProbeStep {
	if resp == "" {
		return ProbeStep{
			Layer:   "L7-Redis-Auth",
			Status:  StatusFAIL,
			Detail:  "Empty AUTH response from server",
			Hint:    "Check Redis server logs.",
			Latency: latency,
		}
	}

	switch {
	case resp == "+OK":
		detail := "Authenticated"
		if target.Username != "" {
			detail = fmt.Sprintf("Authenticated as '%s'", target.Username)
		}
		return ProbeStep{
			Layer:   "L7-Redis-Auth",
			Status:  StatusOK,
			Detail:  detail,
			Latency: latency,
		}
	case strings.HasPrefix(resp, "-WRONGPASS"), strings.Contains(resp, "invalid password"):
		return ProbeStep{
			Layer:   "L7-Redis-Auth",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("Redis authentication failed: %s", ScrubCredentials(resp)),
			Hint:    "Wrong password. Check redis.password in connector config.",
			Latency: latency,
		}
	case strings.HasPrefix(resp, "-ERR invalid username"):
		return ProbeStep{
			Layer:   "L7-Redis-Auth",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("Redis authentication failed: %s", ScrubCredentials(resp)),
			Hint:    "Invalid username. Check Redis ACL configuration and connector config.",
			Latency: latency,
		}
	case strings.HasPrefix(resp, "-ERR Client sent AUTH"):
		return ProbeStep{
			Layer:   "L7-Redis-Auth",
			Status:  StatusWARN,
			Detail:  "Redis server does not require authentication",
			Hint:    "AUTH was sent but server has no password set. Remove redis.password if not needed.",
			Latency: latency,
		}
	case strings.HasPrefix(resp, "-"):
		return ProbeStep{
			Layer:   "L7-Redis-Auth",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("Redis auth error: %s", ScrubCredentials(resp)),
			Hint:    "Check Redis authentication configuration.",
			Latency: latency,
		}
	default:
		return ProbeStep{
			Layer:   "L7-Redis-Auth",
			Status:  StatusWARN,
			Detail:  fmt.Sprintf("Unexpected AUTH response: %s", ScrubCredentials(resp)),
			Hint:    "Check Redis server version and configuration.",
			Latency: latency,
		}
	}
}

// parseRedisInfoVersion extracts the redis_version from INFO server output.
func parseRedisInfoVersion(info string) string {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "redis_version:") {
			return strings.TrimPrefix(line, "redis_version:")
		}
	}
	return ""
}

