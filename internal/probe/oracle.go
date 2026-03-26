package probe

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
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

func (p *OracleProber) Probe(ctx context.Context, target ProbeTarget) []ProbeStep {
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
	buf.WriteByte(tnsPacketConnect)                         // Packet type
	buf.WriteByte(0)                                       // Reserved
	binary.Write(&buf, binary.BigEndian, uint16(0))        // Header checksum

	// Connect Header (24 bytes)
	binary.Write(&buf, binary.BigEndian, uint16(0x0139)) // Version (313)
	binary.Write(&buf, binary.BigEndian, uint16(0x012C)) // Compatible version (300)
	binary.Write(&buf, binary.BigEndian, uint16(0x0000)) // Service options
	binary.Write(&buf, binary.BigEndian, uint16(0x2000)) // SDU size (8192)
	binary.Write(&buf, binary.BigEndian, uint16(0x7FFF)) // TDU size (32767)
	binary.Write(&buf, binary.BigEndian, uint16(0x7F08)) // NT protocol characteristics
	binary.Write(&buf, binary.BigEndian, uint16(0x0000)) // Max packets before ACK
	binary.Write(&buf, binary.BigEndian, uint16(0x0100)) // Byte order
	binary.Write(&buf, binary.BigEndian, uint16(len(connectData))) // Data length
	binary.Write(&buf, binary.BigEndian, dataOffset)               // Data offset
	binary.Write(&buf, binary.BigEndian, uint32(0))      // Max receivable data

	// Connect data
	buf.Write(connectData)

	return buf.Bytes()
}

// readTNSPacket reads a TNS packet and returns the packet type and data.
func readTNSPacket(conn net.Conn) (byte, []byte, error) {
	// Read TNS header (8 bytes)
	header := make([]byte, 8)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, nil, fmt.Errorf("failed to read TNS header: %w", err)
	}

	totalLen := binary.BigEndian.Uint16(header[0:2])
	pktType := header[4]

	if totalLen < 8 {
		return pktType, nil, fmt.Errorf("TNS packet too short: %d bytes", totalLen)
	}

	payloadLen := int(totalLen) - 8
	if payloadLen == 0 {
		return pktType, nil, nil
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return 0, nil, fmt.Errorf("failed to read TNS payload: %w", err)
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

	reasonUser := data[0]
	reasonSystem := data[1]
	dataLen := binary.BigEndian.Uint16(data[2:4])

	var refuseData string
	if int(dataLen) > 0 && len(data) >= 4+int(dataLen) {
		refuseData = strings.TrimSpace(string(data[4 : 4+int(dataLen)]))
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
