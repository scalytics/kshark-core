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

// OracleProber probes Oracle targets using a minimal TNS Connect handshake.
type OracleProber struct{}

func NewOracleProber() *OracleProber { return &OracleProber{} }
func (p *OracleProber) Type() string { return "oracle" }

// TNS packet types.
const (
	oracleDefaultPort = 1521

	tnsPacketConnect  byte = 1
	tnsPacketAccept   byte = 2
	tnsPacketRefuse   byte = 4
	tnsPacketRedirect byte = 5
	tnsPacketData     byte = 6
	tnsPacketResend   byte = 11
)

func (p *OracleProber) Probe(ctx context.Context, target ProbeTarget) (steps []ProbeStep) {
	// Safety net: convert any remaining panic into a FAIL step.
	defer func() {
		if r := recover(); r != nil {
			steps = append(steps, ProbeStep{
				Layer:  "L7-Protocol",
				Status: StatusFAIL,
				Detail: fmt.Sprintf("protocol parser panic: %v", r),
				Hint:   "Server sent malformed or unexpected response. This may not be an Oracle server.",
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

	// L5-6-TLS (optional - for Oracle with TCPS protocol)
	var rwConn net.Conn = conn
	if target.TLS {
		tlsCfg, err := BuildTLSConfig(target.Host, target.ExtraProps["sslrootcert"], false)
		if err != nil {
			steps = append(steps, ProbeStep{
				Layer:  "L5-6-TLS",
				Status: StatusFAIL,
				Detail: fmt.Sprintf("TLS config error: %v", err),
				Hint:   "Check SSL configuration for Oracle (wallet or certificate files).",
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
			Detail: "SSL not configured (use TCPS protocol for Oracle SSL)",
		})
	}

	timeout := 10 * time.Second

	// L7-TNS: Send TNS Connect packet
	service := target.Database
	if service == "" {
		service = "ORCL" // default Oracle service name
	}
	connectDescriptor := buildOracleConnectDescriptor(target.Host, target.Port, service)

	start := time.Now()
	tnsPkt := buildTNSConnect(connectDescriptor)

	rwConn.SetWriteDeadline(time.Now().Add(timeout))
	if _, err := rwConn.Write(tnsPkt); err != nil {
		steps = append(steps, ProbeStep{
			Layer:   "L7-TNS",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("Failed to send TNS Connect: %v", err),
			Hint:    "Server may not be an Oracle instance or port is wrong.",
			Latency: time.Since(start),
		})
		return steps
	}

	// Read TNS response
	rwConn.SetReadDeadline(time.Now().Add(timeout))
	tnsType, tnsData, err := readTNSPacket(rwConn)
	tnsLatency := time.Since(start)

	if err != nil {
		steps = append(steps, ProbeStep{
			Layer:   "L7-TNS",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("TNS Connect failed: %v", ScrubCredentials(err.Error())),
			Hint:    "Server may not be an Oracle listener or port is wrong. Check lsnrctl status.",
			Latency: tnsLatency,
		})
		return steps
	}

	switch tnsType {
	case tnsPacketAccept:
		steps = append(steps, ProbeStep{
			Layer:   "L7-TNS",
			Status:  StatusOK,
			Detail:  fmt.Sprintf("Oracle TNS listener accepted connection to service '%s'", service),
			Latency: tnsLatency,
		})
	case tnsPacketRefuse:
		refuseMsg := parseTNSRefuseData(tnsData)
		hint := "Check Oracle listener configuration. Verify service name with lsnrctl services."
		if strings.Contains(strings.ToLower(refuseMsg), "unknown") || strings.Contains(strings.ToLower(refuseMsg), "no listener") {
			hint = fmt.Sprintf("Service '%s' not registered with listener. Check lsnrctl services.", service)
		}
		steps = append(steps, ProbeStep{
			Layer:   "L7-TNS",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("Oracle TNS connection refused: %s", refuseMsg),
			Hint:    hint,
			Latency: tnsLatency,
		})
		return steps
	case tnsPacketRedirect:
		// Redirect is actually a success - the listener is alive and redirecting
		redirectData := ""
		if len(tnsData) > 0 {
			redirectData = string(tnsData)
		}
		steps = append(steps, ProbeStep{
			Layer:   "L7-TNS",
			Status:  StatusOK,
			Detail:  fmt.Sprintf("Oracle TNS listener redirected (listener is active, service '%s')", service),
			Hint:    fmt.Sprintf("Redirect target: %s", ScrubCredentials(redirectData)),
			Latency: tnsLatency,
		})
	case tnsPacketResend:
		steps = append(steps, ProbeStep{
			Layer:   "L7-TNS",
			Status:  StatusWARN,
			Detail:  "Oracle TNS requested resend (listener is active but may be busy)",
			Hint:    "Try again. If persistent, check listener load.",
			Latency: tnsLatency,
		})
	default:
		steps = append(steps, ProbeStep{
			Layer:   "L7-TNS",
			Status:  StatusWARN,
			Detail:  fmt.Sprintf("Oracle TNS response type: 0x%02X (unexpected but listener responded)", tnsType),
			Hint:    "Listener responded but with an unexpected packet type.",
			Latency: tnsLatency,
		})
	}

	// L7-Auth: TNS login is too complex to implement inline.
	if target.Username != "" {
		steps = append(steps, ProbeStep{
			Layer:  "L7-Auth",
			Status: StatusSKIP,
			Detail: "Oracle TNS authentication not implemented in probe (TNS handshake succeeded)",
			Hint:   "TNS listener responded. Full Oracle authentication probe is not yet supported.",
		})
	}

	return steps
}

// buildOracleConnectDescriptor builds a TNS connect descriptor string.
func buildOracleConnectDescriptor(host string, port int, service string) string {
	return fmt.Sprintf(
		"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%d))(CONNECT_DATA=(SERVICE_NAME=%s)(CID=(PROGRAM=kshark)(HOST=kshark-probe)(USER=kshark))))",
		host, port, service,
	)
}

// buildTNSConnect constructs a TNS Connect packet.
func buildTNSConnect(connectDescriptor string) []byte {
	connectData := []byte(connectDescriptor)

	// TNS Connect packet structure:
	// TNS Header (8 bytes):
	//   Packet Length (2 bytes, big-endian)
	//   Packet Checksum (2 bytes) = 0
	//   Packet Type (1 byte) = 1 (Connect)
	//   Reserved (1 byte) = 0
	//   Header Checksum (2 bytes) = 0
	// Connect Header (24 bytes):
	//   Version (2 bytes) = 0x0139 (313 = Oracle 12c compatible)
	//   Compatible Version (2 bytes) = 0x012C (300)
	//   Service Options (2 bytes) = 0x0000 (not used)
	//   SDU Size (2 bytes) = 0x2000 (8192)
	//   TDU Size (2 bytes) = 0x7FFF (32767)
	//   NT Protocol Characteristics (2 bytes) = 0x7F08
	//   Max Packets Before ACK (2 bytes) = 0x0000
	//   Byte Order (2 bytes) = 0x0100
	//   Data Length (2 bytes)
	//   Data Offset (2 bytes) = 0x003A (58 = 8 header + 24 connect + 26 extended)
	//   Max Receivable Data (4 bytes) = 0
	// Extended (optional flags, 2 bytes each for connect flags):
	//   Connect Flags 0 (1 byte) = 0x00
	//   Connect Flags 1 (1 byte) = 0x00

	connectHeaderSize := 24
	tnsHeaderSize := 8
	// Minimal connect packet without extended area
	dataOffset := uint16(tnsHeaderSize + connectHeaderSize)
	totalLen := int(dataOffset) + len(connectData)

	var buf bytes.Buffer

	// TNS Header (8 bytes)
	binary.Write(&buf, binary.BigEndian, uint16(totalLen)) // Packet length
	binary.Write(&buf, binary.BigEndian, uint16(0))        // Packet checksum
	buf.WriteByte(tnsPacketConnect)                        // Packet type
	buf.WriteByte(0)                                       // Reserved
	binary.Write(&buf, binary.BigEndian, uint16(0))        // Header checksum

	// Connect Header (24 bytes)
	binary.Write(&buf, binary.BigEndian, uint16(0x0139))           // Version (313)
	binary.Write(&buf, binary.BigEndian, uint16(0x012C))           // Compatible version (300)
	binary.Write(&buf, binary.BigEndian, uint16(0x0000))           // Service options
	binary.Write(&buf, binary.BigEndian, uint16(0x2000))           // SDU size (8192)
	binary.Write(&buf, binary.BigEndian, uint16(0x7FFF))           // TDU size (32767)
	binary.Write(&buf, binary.BigEndian, uint16(0x7F08))           // NT protocol characteristics
	binary.Write(&buf, binary.BigEndian, uint16(0x0000))           // Max packets before ACK
	binary.Write(&buf, binary.BigEndian, uint16(0x0100))           // Byte order
	binary.Write(&buf, binary.BigEndian, uint16(len(connectData))) // Data length
	binary.Write(&buf, binary.BigEndian, dataOffset)               // Data offset
	binary.Write(&buf, binary.BigEndian, uint32(0))                // Max receivable data

	// Connect data
	buf.Write(connectData)

	return buf.Bytes()
}

// readTNSPacket reads a TNS packet and returns the packet type and data.
func readTNSPacket(conn net.Conn) (byte, []byte, error) {
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Read TNS length header (2 bytes, big-endian)
	totalLen, err := readUint16BE(conn)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read TNS packet length: %w", err)
	}

	if totalLen < 8 {
		return 0, nil, fmt.Errorf("TNS packet too short: %d bytes", totalLen)
	}
	if totalLen > 32768 {
		return 0, nil, fmt.Errorf("TNS packet too large: %d bytes", totalLen)
	}

	// Read the remaining header bytes (6 bytes: checksum(2) + type(1) + reserved(1) + header_checksum(2))
	restHeader, err := safeRead(conn, 6)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read TNS header: %w", err)
	}

	pktType := restHeader[2] // type is at offset 2 within restHeader (offset 4 in full header)

	payloadLen := int(totalLen) - 8
	if payloadLen == 0 {
		return pktType, nil, nil
	}

	payload, err := safeRead(conn, payloadLen)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to read TNS payload (%d bytes): %w", payloadLen, err)
	}

	return pktType, payload, nil
}

// parseTNSRefuseData extracts human-readable information from a TNS Refuse packet.
func parseTNSRefuseData(data []byte) string {
	if len(data) == 0 {
		return "connection refused (no details)"
	}

	// TNS Refuse packet has:
	// Reason User (1 byte) + Reason System (1 byte) + Data Length (2 bytes) + Data
	if len(data) < 4 {
		return fmt.Sprintf("refused (raw: %x)", data)
	}

	r := bytes.NewReader(data)

	reasonUser, err := r.ReadByte()
	if err != nil {
		return "connection refused (could not read reason)"
	}

	reasonSystem, err := r.ReadByte()
	if err != nil {
		return fmt.Sprintf("reason(user=%d)", reasonUser)
	}

	dataLen, err := readUint16BE(r)
	if err != nil {
		return fmt.Sprintf("reason(user=%d, system=%d)", reasonUser, reasonSystem)
	}

	var refuseData string
	if dataLen > 0 {
		// Validate length before reading
		if int(dataLen) > r.Len() {
			// Read whatever is available
			remaining, _ := safeRead(r, r.Len())
			if len(remaining) > 0 {
				refuseData = strings.TrimSpace(string(remaining))
			}
		} else {
			msgBytes, err := safeRead(r, int(dataLen))
			if err == nil {
				refuseData = strings.TrimSpace(string(msgBytes))
			}
		}
	}

	if refuseData != "" {
		return fmt.Sprintf("reason(user=%d, system=%d): %s", reasonUser, reasonSystem, refuseData)
	}
	return fmt.Sprintf("reason(user=%d, system=%d)", reasonUser, reasonSystem)
}

// classifyOracleError classifies an Oracle error for reporting.
func classifyOracleError(err error, target ProbeTarget) ProbeStep {
	msg := ScrubCredentials(err.Error())
	lower := strings.ToLower(msg)

	switch {
	case strings.Contains(lower, "no such host"):
		return ProbeStep{
			Layer:  "L3-DNS",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("DNS resolution failed: %s", msg),
			Hint:   "Check DNS resolution for the Oracle host.",
		}
	case strings.Contains(lower, "connection refused"):
		return ProbeStep{
			Layer:  "L4-TCP",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Connection refused on port %d: %s", target.Port, msg),
			Hint:   "Oracle listener not running. Check lsnrctl status.",
		}
	case strings.Contains(lower, "i/o timeout") || strings.Contains(lower, "context deadline exceeded"):
		return ProbeStep{
			Layer:  "L4-TCP",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Connection timed out: %s", msg),
			Hint:   "Firewall blocking port or no route to host.",
		}
	case strings.Contains(lower, "ora-12514") || strings.Contains(lower, "service") && strings.Contains(lower, "not registered"):
		return ProbeStep{
			Layer:  "L7-TNS",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Service not found: %s", msg),
			Hint:   fmt.Sprintf("Service '%s' not registered. Check lsnrctl services.", target.Database),
		}
	case strings.Contains(lower, "ora-01017"):
		return ProbeStep{
			Layer:  "L7-Auth",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Authentication failed: %s", msg),
			Hint:   "Invalid username/password. Check Oracle credentials.",
		}
	default:
		return ProbeStep{
			Layer:  "L7-Ready",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Oracle error: %s", msg),
			Hint:   "Check connection parameters and Oracle alert log.",
		}
	}
}
