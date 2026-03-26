package probe

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

// MySQLProber probes MySQL targets using the MySQL wire protocol handshake.
type MySQLProber struct{}

func NewMySQLProber() *MySQLProber { return &MySQLProber{} }
func (p *MySQLProber) Type() string { return "mysql" }

// MySQL protocol constants.
const (
	mysqlDefaultPort = 3306

	// Capability flags (selected)
	mysqlClientProtocol41  uint32 = 0x00000200
	mysqlClientSecureConn  uint32 = 0x00008000
	mysqlClientPluginAuth  uint32 = 0x00080000
	mysqlClientConnectDB   uint32 = 0x00000008
	mysqlClientSSL         uint32 = 0x00000800

	// Packet types
	mysqlPacketOK  byte = 0x00
	mysqlPacketERR byte = 0xFF
	mysqlPacketEOF byte = 0xFE
)

// mysqlGreeting holds the parsed server greeting (initial handshake packet).
type mysqlGreeting struct {
	ProtocolVersion byte
	ServerVersion   string
	ConnectionID    uint32
	AuthPluginData  []byte
	CapabilityFlags uint32
	CharacterSet    byte
	StatusFlags     uint16
	AuthPluginName  string
}

func (p *MySQLProber) Probe(ctx context.Context, target ProbeTarget) []ProbeStep {
	var steps []ProbeStep

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
				Hint:   "Check SSL configuration for MySQL.",
			})
			return steps
		}
		// For MySQL, TLS is negotiated after the initial handshake greeting.
		// We read the greeting first, then upgrade if SSL capability is set.
		// For now, we just note that TLS is requested but do the handshake plaintext first.
		_ = tlsCfg
		// Read server greeting first (needed before TLS upgrade in MySQL protocol)
	} else {
		steps = append(steps, ProbeStep{
			Layer:  "L5-6-TLS",
			Status: StatusSKIP,
			Detail: "SSL not configured",
		})
	}

	timeout := 10 * time.Second

	// L7-Handshake: Read server greeting
	start := time.Now()
	greeting, err := readMySQLGreeting(rwConn, timeout)
	greetingLatency := time.Since(start)

	if err != nil {
		steps = append(steps, ProbeStep{
			Layer:   "L7-Handshake",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("MySQL handshake failed: %v", ScrubCredentials(err.Error())),
			Hint:    "Server may not be a MySQL instance or port is wrong.",
			Latency: greetingLatency,
		})
		return steps
	}

	steps = append(steps, ProbeStep{
		Layer:   "L7-Handshake",
		Status:  StatusOK,
		Detail:  fmt.Sprintf("MySQL server %s (protocol v%d)", greeting.ServerVersion, greeting.ProtocolVersion),
		Latency: greetingLatency,
	})

	// If TLS is requested and server supports it, upgrade connection
	if target.TLS {
		if greeting.CapabilityFlags&mysqlClientSSL != 0 {
			tlsCfg, _ := BuildTLSConfig(target.Host, target.ExtraProps["sslrootcert"], false)
			// Send SSL request packet
			sslReq := buildMySQLSSLRequest(greeting)
			rwConn.SetWriteDeadline(time.Now().Add(timeout))
			if _, err := rwConn.Write(sslReq); err != nil {
				steps = append(steps, ProbeStep{
					Layer:  "L5-6-TLS",
					Status: StatusFAIL,
					Detail: fmt.Sprintf("Failed to send SSL request: %v", err),
					Hint:   "Check network connectivity to MySQL server.",
				})
				return steps
			}

			tlsConn, tlsStep := ProbeTLS(rwConn, target.Host, tlsCfg)
			steps = append(steps, tlsStep)
			if tlsConn == nil {
				return steps
			}
			rwConn = tlsConn
		} else {
			steps = append(steps, ProbeStep{
				Layer:  "L5-6-TLS",
				Status: StatusWARN,
				Detail: "Server does not advertise SSL capability",
				Hint:   "Enable SSL on MySQL server with --ssl-cert and --ssl-key options.",
			})
		}
	}

	// L7-Auth: Send handshake response with credentials
	if target.Username != "" {
		authStart := time.Now()
		authResp := buildMySQLHandshakeResponse(greeting, target.Username, target.Password, target.Database)

		rwConn.SetWriteDeadline(time.Now().Add(timeout))
		if _, err := rwConn.Write(authResp); err != nil {
			steps = append(steps, ProbeStep{
				Layer:   "L7-Auth",
				Status:  StatusFAIL,
				Detail:  fmt.Sprintf("Failed to send auth packet: %v", ScrubCredentials(err.Error())),
				Hint:    "Check network connectivity to MySQL server.",
				Latency: time.Since(authStart),
			})
			return steps
		}

		// Read auth response
		rwConn.SetReadDeadline(time.Now().Add(timeout))
		respPkt, err := readMySQLPacket(rwConn)
		authLatency := time.Since(authStart)

		if err != nil {
			steps = append(steps, ProbeStep{
				Layer:   "L7-Auth",
				Status:  StatusFAIL,
				Detail:  fmt.Sprintf("Failed to read auth response: %v", ScrubCredentials(err.Error())),
				Hint:    "Check MySQL server logs for authentication errors.",
				Latency: authLatency,
			})
			return steps
		}

		step := classifyMySQLAuthResponse(respPkt, target, authLatency)
		steps = append(steps, step)
		if step.Status == StatusFAIL {
			return steps
		}

		// L7-Ready: Connection established
		steps = append(steps, ProbeStep{
			Layer:   "L7-Ready",
			Status:  StatusOK,
			Detail:  fmt.Sprintf("Authenticated as '%s', server ready", target.Username),
			Latency: authLatency,
		})
	}

	return steps
}

// readMySQLGreeting reads and parses the MySQL server initial handshake packet.
func readMySQLGreeting(conn net.Conn, timeout time.Duration) (*mysqlGreeting, error) {
	conn.SetReadDeadline(time.Now().Add(timeout))
	pkt, err := readMySQLPacket(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read greeting: %w", err)
	}

	if len(pkt) < 1 {
		return nil, fmt.Errorf("empty greeting packet")
	}

	// Check for ERR packet
	if pkt[0] == mysqlPacketERR {
		errMsg := parseMySQLError(pkt)
		return nil, fmt.Errorf("server error: %s", errMsg)
	}

	g := &mysqlGreeting{}
	r := bytes.NewReader(pkt)

	// Protocol version (1 byte)
	g.ProtocolVersion, _ = r.ReadByte()
	if g.ProtocolVersion != 10 {
		return nil, fmt.Errorf("unsupported protocol version: %d", g.ProtocolVersion)
	}

	// Server version (null-terminated string)
	var verBuf bytes.Buffer
	for {
		b, err := r.ReadByte()
		if err != nil || b == 0 {
			break
		}
		verBuf.WriteByte(b)
	}
	g.ServerVersion = verBuf.String()

	// Connection ID (4 bytes)
	binary.Read(r, binary.LittleEndian, &g.ConnectionID)

	// Auth plugin data part 1 (8 bytes)
	authData1 := make([]byte, 8)
	r.Read(authData1)

	// Filler (1 byte)
	r.ReadByte()

	// Capability flags lower 2 bytes
	var capLower uint16
	binary.Read(r, binary.LittleEndian, &capLower)
	g.CapabilityFlags = uint32(capLower)

	// If there are more bytes, continue parsing
	if r.Len() > 0 {
		// Character set (1 byte)
		g.CharacterSet, _ = r.ReadByte()

		// Status flags (2 bytes)
		binary.Read(r, binary.LittleEndian, &g.StatusFlags)

		// Capability flags upper 2 bytes
		var capUpper uint16
		binary.Read(r, binary.LittleEndian, &capUpper)
		g.CapabilityFlags |= uint32(capUpper) << 16

		// Auth plugin data length (1 byte) or 0x00
		authDataLen, _ := r.ReadByte()

		// Reserved (10 bytes)
		reserved := make([]byte, 10)
		r.Read(reserved)

		// Auth plugin data part 2 (if protocol 41)
		if g.CapabilityFlags&mysqlClientSecureConn != 0 {
			// Length of auth-plugin-data is max(13, authDataLen - 8)
			part2Len := 13
			if int(authDataLen) > 8 {
				part2Len = int(authDataLen) - 8
			}
			authData2 := make([]byte, part2Len)
			r.Read(authData2)

			g.AuthPluginData = make([]byte, 8+part2Len)
			copy(g.AuthPluginData, authData1)
			copy(g.AuthPluginData[8:], authData2)
			// Remove trailing null byte from auth data
			if len(g.AuthPluginData) > 0 && g.AuthPluginData[len(g.AuthPluginData)-1] == 0 {
				g.AuthPluginData = g.AuthPluginData[:len(g.AuthPluginData)-1]
			}
		} else {
			g.AuthPluginData = authData1
		}

		// Auth plugin name (null-terminated string)
		if g.CapabilityFlags&mysqlClientPluginAuth != 0 {
			var pluginBuf bytes.Buffer
			for {
				b, err := r.ReadByte()
				if err != nil || b == 0 {
					break
				}
				pluginBuf.WriteByte(b)
			}
			g.AuthPluginName = pluginBuf.String()
		}
	} else {
		g.AuthPluginData = authData1
	}

	return g, nil
}

// readMySQLPacket reads a MySQL wire protocol packet (4 byte header + payload).
func readMySQLPacket(conn net.Conn) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, fmt.Errorf("failed to read packet header: %w", err)
	}

	// Payload length: 3 bytes little-endian
	payloadLen := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	if payloadLen == 0 {
		return []byte{}, nil
	}
	if payloadLen > 1<<24 {
		return nil, fmt.Errorf("packet too large: %d bytes", payloadLen)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, fmt.Errorf("failed to read packet payload: %w", err)
	}

	return payload, nil
}

// buildMySQLSSLRequest builds a MySQL SSL request packet (sent before TLS upgrade).
func buildMySQLSSLRequest(greeting *mysqlGreeting) []byte {
	caps := mysqlClientProtocol41 | mysqlClientSecureConn | mysqlClientSSL
	if greeting.CapabilityFlags&mysqlClientPluginAuth != 0 {
		caps |= mysqlClientPluginAuth
	}

	var buf bytes.Buffer
	// Capability flags (4 bytes)
	binary.Write(&buf, binary.LittleEndian, caps)
	// Max packet size (4 bytes)
	binary.Write(&buf, binary.LittleEndian, uint32(1<<24-1))
	// Character set (1 byte) - utf8mb4
	buf.WriteByte(0x2d) // utf8mb4_general_ci = 45
	// Reserved (23 bytes of zeros)
	buf.Write(make([]byte, 23))

	payload := buf.Bytes()
	return writeMySQLPacket(payload, 1)
}

// buildMySQLHandshakeResponse builds a MySQL HandshakeResponse41 packet.
func buildMySQLHandshakeResponse(greeting *mysqlGreeting, username, password, database string) []byte {
	caps := mysqlClientProtocol41 | mysqlClientSecureConn
	if greeting.CapabilityFlags&mysqlClientPluginAuth != 0 {
		caps |= mysqlClientPluginAuth
	}
	if database != "" && greeting.CapabilityFlags&mysqlClientConnectDB != 0 {
		caps |= mysqlClientConnectDB
	}

	var buf bytes.Buffer

	// Capability flags (4 bytes)
	binary.Write(&buf, binary.LittleEndian, caps)
	// Max packet size (4 bytes)
	binary.Write(&buf, binary.LittleEndian, uint32(1<<24-1))
	// Character set (1 byte) - utf8mb4
	buf.WriteByte(0x2d)
	// Reserved (23 bytes)
	buf.Write(make([]byte, 23))
	// Username (null-terminated)
	buf.WriteString(username)
	buf.WriteByte(0)

	// Auth response
	authResp := mysqlNativePassword(password, greeting.AuthPluginData)
	buf.WriteByte(byte(len(authResp)))
	buf.Write(authResp)

	// Database (null-terminated, if set)
	if caps&mysqlClientConnectDB != 0 && database != "" {
		buf.WriteString(database)
		buf.WriteByte(0)
	}

	// Auth plugin name (null-terminated)
	if caps&mysqlClientPluginAuth != 0 {
		pluginName := greeting.AuthPluginName
		if pluginName == "" {
			pluginName = "mysql_native_password"
		}
		buf.WriteString(pluginName)
		buf.WriteByte(0)
	}

	payload := buf.Bytes()
	return writeMySQLPacket(payload, 1)
}

// mysqlNativePassword computes the mysql_native_password auth response.
// SHA1(password) XOR SHA1(scramble + SHA1(SHA1(password)))
func mysqlNativePassword(password string, scramble []byte) []byte {
	if password == "" {
		return nil
	}

	// SHA1(password)
	h1 := sha1.Sum([]byte(password))

	// SHA1(SHA1(password))
	h2 := sha1.Sum(h1[:])

	// SHA1(scramble + SHA1(SHA1(password)))
	h := sha1.New()
	h.Write(scramble)
	h.Write(h2[:])
	h3 := h.Sum(nil)

	// XOR SHA1(password) with the above
	result := make([]byte, sha1.Size)
	for i := range result {
		result[i] = h1[i] ^ h3[i]
	}
	return result
}

// writeMySQLPacket wraps payload in a MySQL wire protocol packet header.
func writeMySQLPacket(payload []byte, sequenceID byte) []byte {
	length := len(payload)
	pkt := make([]byte, 4+length)
	pkt[0] = byte(length)
	pkt[1] = byte(length >> 8)
	pkt[2] = byte(length >> 16)
	pkt[3] = sequenceID
	copy(pkt[4:], payload)
	return pkt
}

// parseMySQLError extracts the error message from a MySQL ERR packet.
func parseMySQLError(pkt []byte) string {
	if len(pkt) < 3 {
		return "unknown error"
	}
	// ERR packet: 0xFF + error code (2 bytes LE) + '#' + sqlstate (5 bytes) + message
	errCode := binary.LittleEndian.Uint16(pkt[1:3])
	msg := ""
	offset := 3
	if offset < len(pkt) && pkt[offset] == '#' {
		// Skip SQL state marker and 5-byte state
		offset += 6
	}
	if offset < len(pkt) {
		msg = string(pkt[offset:])
	}
	return fmt.Sprintf("error %d: %s", errCode, msg)
}

// classifyMySQLAuthResponse classifies the MySQL auth response packet.
func classifyMySQLAuthResponse(pkt []byte, target ProbeTarget, latency time.Duration) ProbeStep {
	if len(pkt) == 0 {
		return ProbeStep{
			Layer:   "L7-Auth",
			Status:  StatusFAIL,
			Detail:  "Empty auth response from server",
			Hint:    "Check MySQL server logs.",
			Latency: latency,
		}
	}

	switch pkt[0] {
	case mysqlPacketOK:
		return ProbeStep{
			Layer:   "L7-Auth",
			Status:  StatusOK,
			Detail:  fmt.Sprintf("Authenticated as '%s'", target.Username),
			Latency: latency,
		}
	case mysqlPacketERR:
		errMsg := ScrubCredentials(parseMySQLError(pkt))
		lower := strings.ToLower(errMsg)

		hint := "Check connection.user and connection.password in connector config."
		switch {
		case strings.Contains(lower, "access denied"):
			hint = "Access denied. Verify username and password. Check GRANT privileges."
		case strings.Contains(lower, "unknown database"):
			hint = fmt.Sprintf("Database '%s' does not exist. Create it with CREATE DATABASE.", target.Database)
		case strings.Contains(lower, "too many connections"):
			hint = "Server has too many connections. Increase max_connections or close idle sessions."
		case strings.Contains(lower, "host") && strings.Contains(lower, "not allowed"):
			hint = "Client host is not allowed. Update MySQL user with appropriate host wildcard."
		}

		return ProbeStep{
			Layer:   "L7-Auth",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("MySQL auth failed: %s", errMsg),
			Hint:    hint,
			Latency: latency,
		}
	case mysqlPacketEOF:
		// Auth switch request - server wants a different auth method
		return ProbeStep{
			Layer:   "L7-Auth",
			Status:  StatusWARN,
			Detail:  "Server requested auth method switch (may require caching_sha2_password)",
			Hint:    "Server uses a different auth plugin. Consider mysql_native_password for compatibility.",
			Latency: latency,
		}
	default:
		return ProbeStep{
			Layer:   "L7-Auth",
			Status:  StatusWARN,
			Detail:  fmt.Sprintf("Unexpected auth response type: 0x%02X", pkt[0]),
			Hint:    "Check MySQL server version and auth plugin configuration.",
			Latency: latency,
		}
	}
}
