package probe

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"time"
)

// SQLServerProber probes SQL Server targets using the TDS pre-login handshake.
type SQLServerProber struct{}

func NewSQLServerProber() *SQLServerProber { return &SQLServerProber{} }
func (p *SQLServerProber) Type() string    { return "sqlserver" }

// TDS packet types and pre-login option tokens.
const (
	tdsDefaultPort = 1433

	// TDS packet types
	tdsPacketPreLogin  byte = 0x12
	tdsPacketResponse  byte = 0x04
	tdsPacketLogin7    byte = 0x10

	// TDS packet status
	tdsStatusEOM byte = 0x01 // end of message

	// Pre-Login option tokens
	tdsPreLoginVersion    byte = 0x00
	tdsPreLoginEncryption byte = 0x01
	tdsPreLoginInstOpt    byte = 0x02
	tdsPreLoginThreadID   byte = 0x03
	tdsPreLoginMARS       byte = 0x04
	tdsPreLoginTerminator byte = 0xFF

	// Encryption values
	tdsEncryptOff       byte = 0x00
	tdsEncryptOn        byte = 0x01
	tdsEncryptNotSup    byte = 0x02
	tdsEncryptRequired  byte = 0x03
)

func (p *SQLServerProber) Probe(ctx context.Context, target ProbeTarget) (steps []ProbeStep) {
	// Safety net: convert any remaining panic into a FAIL step.
	defer func() {
		if r := recover(); r != nil {
			steps = append(steps, ProbeStep{
				Layer:  "L7-Protocol",
				Status: StatusFAIL,
				Detail: fmt.Sprintf("protocol parser panic: %v", r),
				Hint:   "Server sent malformed or unexpected response. This may not be a SQL Server.",
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

	timeout := 10 * time.Second

	// L7-PreLogin: Send TDS Pre-Login packet
	start := time.Now()
	preLoginPkt := buildTDSPreLogin(target.TLS)

	conn.SetWriteDeadline(time.Now().Add(timeout))
	if _, err := conn.Write(preLoginPkt); err != nil {
		steps = append(steps, ProbeStep{
			Layer:   "L7-PreLogin",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("Failed to send TDS Pre-Login: %v", err),
			Hint:    "Server may not be a SQL Server instance or port is wrong.",
			Latency: time.Since(start),
		})
		return steps
	}

	// Read TDS Pre-Login response
	conn.SetReadDeadline(time.Now().Add(timeout))
	respType, respData, err := readTDSPacket(conn)
	preLoginLatency := time.Since(start)

	if err != nil {
		steps = append(steps, ProbeStep{
			Layer:   "L7-PreLogin",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("TDS Pre-Login failed: %v", ScrubCredentials(err.Error())),
			Hint:    "Server may not be a SQL Server instance or port is wrong.",
			Latency: preLoginLatency,
		})
		return steps
	}

	// Validate response type
	if respType != tdsPacketResponse {
		steps = append(steps, ProbeStep{
			Layer:   "L7-PreLogin",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("Unexpected TDS response type: 0x%02X (expected 0x%02X)", respType, tdsPacketResponse),
			Hint:    "Server did not respond with expected TDS protocol. Check if this is a SQL Server port.",
			Latency: preLoginLatency,
		})
		return steps
	}

	// Parse pre-login response
	version, encryption, err := parseTDSPreLoginResponse(respData)
	if err != nil {
		steps = append(steps, ProbeStep{
			Layer:   "L7-PreLogin",
			Status:  StatusWARN,
			Detail:  fmt.Sprintf("TDS Pre-Login response parse warning: %v", err),
			Hint:    "Server responded but pre-login options could not be fully parsed.",
			Latency: preLoginLatency,
		})
	} else {
		encStr := tdsEncryptionString(encryption)
		steps = append(steps, ProbeStep{
			Layer:   "L7-PreLogin",
			Status:  StatusOK,
			Detail:  fmt.Sprintf("SQL Server responded: version=%s, encryption=%s", version, encStr),
			Latency: preLoginLatency,
		})
	}

	// L5-6-TLS: SQL Server does TLS after pre-login
	if target.TLS || encryption == tdsEncryptOn || encryption == tdsEncryptRequired {
		tlsCfg, err := BuildTLSConfig(target.Host, target.ExtraProps["sslrootcert"], false)
		if err != nil {
			steps = append(steps, ProbeStep{
				Layer:  "L5-6-TLS",
				Status: StatusFAIL,
				Detail: fmt.Sprintf("TLS config error: %v", err),
				Hint:   "Check SSL configuration for SQL Server.",
			})
			return steps
		}
		tlsConn, tlsStep := ProbeTLS(conn, target.Host, tlsCfg)
		steps = append(steps, tlsStep)
		if tlsConn == nil {
			return steps
		}
		// Note: We don't continue to Login7 with TLS in this probe;
		// successful TLS handshake is sufficient.
	} else if encryption == tdsEncryptNotSup || encryption == tdsEncryptOff {
		steps = append(steps, ProbeStep{
			Layer:  "L5-6-TLS",
			Status: StatusSKIP,
			Detail: fmt.Sprintf("Server encryption: %s", tdsEncryptionString(encryption)),
		})
	} else {
		steps = append(steps, ProbeStep{
			Layer:  "L5-6-TLS",
			Status: StatusSKIP,
			Detail: "TLS not requested",
		})
	}

	// L7-Auth: We don't attempt Login7 (requires full TDS login packet construction).
	// The pre-login handshake is sufficient to verify SQL Server is reachable and responsive.
	if target.Username != "" {
		steps = append(steps, ProbeStep{
			Layer:  "L7-Auth",
			Status: StatusSKIP,
			Detail: "Login7 authentication not implemented in probe (pre-login handshake succeeded)",
			Hint:   "TDS pre-login succeeded. Full Login7 auth probe is not yet supported.",
		})
	}

	return steps
}

// buildTDSPreLogin constructs a TDS Pre-Login packet.
func buildTDSPreLogin(requestEncryption bool) []byte {
	// Build pre-login options
	// Each option: token(1) + offset(2) + length(2)
	// Terminated by 0xFF

	// Calculate offsets for option data
	// Options: VERSION, ENCRYPTION, INSTOPT, THREADID, MARS, TERMINATOR
	numOptions := 5
	headerSize := numOptions*5 + 1 // 5 bytes per option + 1 terminator byte

	versionData := []byte{0x0F, 0x00, 0x10, 0x00, 0x00, 0x00} // 15.0.4100.0 (arbitrary version)
	var encryptData []byte
	if requestEncryption {
		encryptData = []byte{tdsEncryptOn}
	} else {
		encryptData = []byte{tdsEncryptOff}
	}
	instOptData := []byte{0x00} // default instance
	threadIDData := []byte{0x00, 0x00, 0x00, 0x00}
	marsData := []byte{0x00} // MARS off

	offset := uint16(headerSize)
	var optionHeader bytes.Buffer

	// VERSION
	optionHeader.WriteByte(tdsPreLoginVersion)
	binary.Write(&optionHeader, binary.BigEndian, offset)
	binary.Write(&optionHeader, binary.BigEndian, uint16(len(versionData)))
	offset += uint16(len(versionData))

	// ENCRYPTION
	optionHeader.WriteByte(tdsPreLoginEncryption)
	binary.Write(&optionHeader, binary.BigEndian, offset)
	binary.Write(&optionHeader, binary.BigEndian, uint16(len(encryptData)))
	offset += uint16(len(encryptData))

	// INSTOPT
	optionHeader.WriteByte(tdsPreLoginInstOpt)
	binary.Write(&optionHeader, binary.BigEndian, offset)
	binary.Write(&optionHeader, binary.BigEndian, uint16(len(instOptData)))
	offset += uint16(len(instOptData))

	// THREADID
	optionHeader.WriteByte(tdsPreLoginThreadID)
	binary.Write(&optionHeader, binary.BigEndian, offset)
	binary.Write(&optionHeader, binary.BigEndian, uint16(len(threadIDData)))
	offset += uint16(len(threadIDData))

	// MARS
	optionHeader.WriteByte(tdsPreLoginMARS)
	binary.Write(&optionHeader, binary.BigEndian, offset)
	binary.Write(&optionHeader, binary.BigEndian, uint16(len(marsData)))

	// Terminator
	optionHeader.WriteByte(tdsPreLoginTerminator)

	// Combine header and data
	var payload bytes.Buffer
	payload.Write(optionHeader.Bytes())
	payload.Write(versionData)
	payload.Write(encryptData)
	payload.Write(instOptData)
	payload.Write(threadIDData)
	payload.Write(marsData)

	return writeTDSPacket(tdsPacketPreLogin, payload.Bytes())
}

// writeTDSPacket wraps payload in a TDS packet header.
func writeTDSPacket(pktType byte, payload []byte) []byte {
	// TDS packet header: 8 bytes
	// Type(1) + Status(1) + Length(2, big-endian) + SPID(2) + PacketID(1) + Window(1)
	totalLen := 8 + len(payload)
	pkt := make([]byte, totalLen)
	pkt[0] = pktType
	pkt[1] = tdsStatusEOM
	binary.BigEndian.PutUint16(pkt[2:4], uint16(totalLen))
	pkt[4] = 0x00 // SPID
	pkt[5] = 0x00 // SPID
	pkt[6] = 0x01 // PacketID
	pkt[7] = 0x00 // Window
	copy(pkt[8:], payload)
	return pkt
}

// readTDSPacket reads a TDS packet and returns the packet type and payload.
func readTDSPacket(conn net.Conn) (byte, []byte, error) {
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Read TDS header (8 bytes)
	header, err := safeRead(conn, 8)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read TDS header: %w", err)
	}

	pktType := header[0]
	totalLen := binary.BigEndian.Uint16(header[2:4])

	if totalLen < 8 {
		return 0, nil, fmt.Errorf("TDS packet too short: %d bytes", totalLen)
	}
	if totalLen > 32768 {
		return 0, nil, fmt.Errorf("TDS packet too large: %d bytes", totalLen)
	}

	payloadLen := int(totalLen) - 8
	if payloadLen == 0 {
		return pktType, nil, nil
	}

	payload, err := safeRead(conn, payloadLen)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read TDS payload (%d bytes): %w", payloadLen, err)
	}

	return pktType, payload, nil
}

// parseTDSPreLoginResponse parses a TDS Pre-Login response to extract version and encryption.
func parseTDSPreLoginResponse(data []byte) (string, byte, error) {
	if len(data) == 0 {
		return "", 0, fmt.Errorf("empty pre-login response")
	}

	type preLoginOption struct {
		token  byte
		offset uint16
		length uint16
	}

	var options []preLoginOption
	r := bytes.NewReader(data)

	// Parse option tokens using buffered reader
	for {
		token, err := r.ReadByte()
		if err != nil {
			break
		}
		if token == tdsPreLoginTerminator {
			break
		}

		optOffset, err := readUint16BE(r)
		if err != nil {
			return "", 0, fmt.Errorf("truncated pre-login option offset for token 0x%02X", token)
		}
		optLength, err := readUint16BE(r)
		if err != nil {
			return "", 0, fmt.Errorf("truncated pre-login option length for token 0x%02X", token)
		}

		options = append(options, preLoginOption{token: token, offset: optOffset, length: optLength})
	}

	var version string
	var encryption byte

	for _, opt := range options {
		end := int(opt.offset) + int(opt.length)
		// Validate that offset+length fits within the data
		if int(opt.offset) > len(data) || end > len(data) || int(opt.offset) < 0 {
			continue
		}
		optData := data[opt.offset:end]

		switch opt.token {
		case tdsPreLoginVersion:
			if len(optData) >= 6 {
				major := optData[0]
				minor := optData[1]
				build := binary.BigEndian.Uint16(optData[2:4])
				version = fmt.Sprintf("%d.%d.%d", major, minor, build)
			}
		case tdsPreLoginEncryption:
			if len(optData) >= 1 {
				encryption = optData[0]
			}
		}
	}

	if version == "" {
		version = "unknown"
	}

	return version, encryption, nil
}

// tdsEncryptionString returns a human-readable string for the TDS encryption value.
func tdsEncryptionString(enc byte) string {
	switch enc {
	case tdsEncryptOff:
		return "OFF"
	case tdsEncryptOn:
		return "ON"
	case tdsEncryptNotSup:
		return "NOT_SUPPORTED"
	case tdsEncryptRequired:
		return "REQUIRED"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02X)", enc)
	}
}

// classifySQLServerError classifies a SQL Server error for reporting.
func classifySQLServerError(err error, target ProbeTarget) ProbeStep {
	msg := ScrubCredentials(err.Error())
	lower := strings.ToLower(msg)

	switch {
	case strings.Contains(lower, "no such host"):
		return ProbeStep{
			Layer:  "L3-DNS",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("DNS resolution failed: %s", msg),
			Hint:   "Check DNS resolution for the SQL Server host.",
		}
	case strings.Contains(lower, "connection refused"):
		return ProbeStep{
			Layer:  "L4-TCP",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Connection refused on port %d: %s", target.Port, msg),
			Hint:   "SQL Server not running or port is blocked. Check SQL Server Configuration Manager.",
		}
	case strings.Contains(lower, "i/o timeout") || strings.Contains(lower, "context deadline exceeded"):
		return ProbeStep{
			Layer:  "L4-TCP",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Connection timed out: %s", msg),
			Hint:   "Firewall blocking port or no route to host.",
		}
	case strings.Contains(lower, "certificate") || strings.Contains(lower, "tls") || strings.Contains(lower, "x509"):
		return ProbeStep{
			Layer:  "L5-6-TLS",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("TLS error: %s", msg),
			Hint:   "Check TLS/SSL configuration. SQL Server may require encrypt=true in connection string.",
		}
	case strings.Contains(lower, "login failed"):
		return ProbeStep{
			Layer:  "L7-Auth",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Authentication failed: %s", msg),
			Hint:   "Wrong credentials. Check SQL Server authentication mode (Windows vs SQL Auth).",
		}
	default:
		return ProbeStep{
			Layer:  "L7-Ready",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("SQL Server error: %s", msg),
			Hint:   "Check connection parameters and SQL Server error log.",
		}
	}
}
