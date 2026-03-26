package probe

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// startMockServer starts a TCP server on a random port, calling handler for
// the first accepted connection. The listener is closed via t.Cleanup.
func startMockServer(t *testing.T, handler func(net.Conn)) (string, int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		handler(conn)
	}()

	addr := ln.Addr().(*net.TCPAddr)
	return addr.IP.String(), addr.Port
}

// buildMockMySQLGreeting builds a realistic MySQL server greeting packet.
// Protocol version 10, server version as given, with standard capabilities.
func buildMockMySQLGreeting(serverVersion string, connID uint32) []byte {
	var payload bytes.Buffer

	// Protocol version
	payload.WriteByte(10)

	// Server version (null-terminated)
	payload.WriteString(serverVersion)
	payload.WriteByte(0)

	// Connection ID (4 bytes LE)
	binary.Write(&payload, binary.LittleEndian, connID)

	// Auth plugin data part 1 (8 bytes of scramble)
	scramble1 := []byte{0x3a, 0x23, 0x4d, 0x56, 0x7a, 0x1e, 0x2b, 0x44}
	payload.Write(scramble1)

	// Filler
	payload.WriteByte(0x00)

	// Capability flags lower 2 bytes (protocol41 + secure_conn + plugin_auth + connect_db)
	var caps uint32 = mysqlClientProtocol41 | mysqlClientSecureConn | mysqlClientPluginAuth | mysqlClientConnectDB
	binary.Write(&payload, binary.LittleEndian, uint16(caps&0xFFFF))

	// Character set (utf8mb4 = 45)
	payload.WriteByte(0x2d)

	// Status flags (2 bytes)
	binary.Write(&payload, binary.LittleEndian, uint16(0x0002))

	// Capability flags upper 2 bytes
	capUpper := uint16((caps >> 16) & 0xFFFF)
	binary.Write(&payload, binary.LittleEndian, capUpper)

	// Auth plugin data length (21 = 8 + 13)
	payload.WriteByte(21)

	// Reserved (10 bytes)
	payload.Write(make([]byte, 10))

	// Auth plugin data part 2 (13 bytes, last byte is null)
	scramble2 := []byte{0x5c, 0x12, 0x6e, 0x0a, 0x3f, 0x47, 0x18, 0x29, 0x55, 0x61, 0x7d, 0x08, 0x00}
	payload.Write(scramble2)

	// Auth plugin name (null-terminated)
	payload.WriteString("mysql_native_password")
	payload.WriteByte(0)

	return writeMySQLPacket(payload.Bytes(), 0)
}

// buildMockMySQLOK builds a MySQL OK packet.
func buildMockMySQLOK() []byte {
	var payload bytes.Buffer
	payload.WriteByte(mysqlPacketOK)
	payload.WriteByte(0) // affected rows
	payload.WriteByte(0) // last insert id
	binary.Write(&payload, binary.LittleEndian, uint16(0x0002)) // status
	binary.Write(&payload, binary.LittleEndian, uint16(0))      // warnings
	return writeMySQLPacket(payload.Bytes(), 2)
}

// buildMockMySQLERR builds a MySQL ERR packet with the given error code and message.
func buildMockMySQLERR(errCode uint16, sqlState string, message string) []byte {
	var payload bytes.Buffer
	payload.WriteByte(mysqlPacketERR)
	binary.Write(&payload, binary.LittleEndian, errCode)
	payload.WriteByte('#')
	payload.WriteString(sqlState)
	payload.WriteString(message)
	return writeMySQLPacket(payload.Bytes(), 2)
}

func TestMySQLProber_Type(t *testing.T) {
	p := NewMySQLProber()
	if got := p.Type(); got != "mysql" {
		t.Errorf("Type() = %q, want %q", got, "mysql")
	}
}

func TestMySQLProber_HappyPath(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		// Send greeting
		conn.Write(buildMockMySQLGreeting("8.0.35", 42))

		// Read client auth (ignore contents)
		buf := make([]byte, 4096)
		conn.Read(buf)

		// Send OK
		conn.Write(buildMockMySQLOK())
	})

	prober := NewMySQLProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Username: "testuser",
		Password: "testpass",
		Database: "testdb",
	})

	// Expect: L3-DNS OK, L4-TCP OK, L5-6-TLS SKIP, L7-Handshake OK, L7-Auth OK, L7-Ready OK
	if len(steps) < 4 {
		t.Fatalf("expected at least 4 steps, got %d: %+v", len(steps), steps)
	}

	// L3-DNS OK
	if steps[0].Layer != "L3-DNS" || steps[0].Status != StatusOK {
		t.Errorf("step[0] = %s/%s, want L3-DNS/OK", steps[0].Layer, steps[0].Status)
	}

	// L4-TCP OK
	if steps[1].Layer != "L4-TCP" || steps[1].Status != StatusOK {
		t.Errorf("step[1] = %s/%s, want L4-TCP/OK", steps[1].Layer, steps[1].Status)
	}

	// Find L7-Handshake OK
	foundHandshake := false
	for _, s := range steps {
		if s.Layer == "L7-Handshake" && s.Status == StatusOK {
			foundHandshake = true
			if !strings.Contains(s.Detail, "8.0.35") {
				t.Errorf("handshake detail should contain server version, got %q", s.Detail)
			}
		}
	}
	if !foundHandshake {
		t.Error("expected L7-Handshake OK step")
	}

	// Find L7-Auth OK
	foundAuth := false
	for _, s := range steps {
		if s.Layer == "L7-Auth" && s.Status == StatusOK {
			foundAuth = true
		}
	}
	if !foundAuth {
		t.Error("expected L7-Auth OK step")
	}
}

func TestMySQLProber_DNSFailure(t *testing.T) {
	prober := NewMySQLProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: "this-host-does-not-exist-kshark-test.invalid",
		Port: 3306,
	})

	if len(steps) == 0 {
		t.Fatal("expected at least 1 step")
	}
	if steps[0].Layer != "L3-DNS" || steps[0].Status != StatusFAIL {
		t.Errorf("step[0] = %s/%s, want L3-DNS/FAIL", steps[0].Layer, steps[0].Status)
	}
	// Should stop after DNS failure
	if len(steps) != 1 {
		t.Errorf("expected exactly 1 step after DNS failure, got %d", len(steps))
	}
}

func TestMySQLProber_TCPFailure(t *testing.T) {
	// Bind a listener and immediately close it to get a known-closed port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	prober := NewMySQLProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: "127.0.0.1",
		Port: port,
	})

	if len(steps) < 2 {
		t.Fatalf("expected at least 2 steps, got %d", len(steps))
	}
	if steps[0].Layer != "L3-DNS" || steps[0].Status != StatusOK {
		t.Errorf("step[0] = %s/%s, want L3-DNS/OK", steps[0].Layer, steps[0].Status)
	}
	if steps[1].Layer != "L4-TCP" || steps[1].Status != StatusFAIL {
		t.Errorf("step[1] = %s/%s, want L4-TCP/FAIL", steps[1].Layer, steps[1].Status)
	}
	// Should stop after TCP failure
	if len(steps) != 2 {
		t.Errorf("expected exactly 2 steps after TCP failure, got %d", len(steps))
	}
}

func TestMySQLProber_AuthFailure(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		// Send greeting
		conn.Write(buildMockMySQLGreeting("8.0.35", 1))

		// Read client auth
		buf := make([]byte, 4096)
		conn.Read(buf)

		// Send ERR (Access denied)
		conn.Write(buildMockMySQLERR(1045, "28000", "Access denied for user 'baduser'@'localhost'"))
	})

	prober := NewMySQLProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Username: "baduser",
		Password: "wrongpass",
	})

	// Find L7-Auth FAIL
	foundAuthFail := false
	for _, s := range steps {
		if s.Layer == "L7-Auth" && s.Status == StatusFAIL {
			foundAuthFail = true
			if !strings.Contains(strings.ToLower(s.Detail), "access denied") {
				t.Errorf("auth detail should mention 'access denied', got %q", s.Detail)
			}
		}
	}
	if !foundAuthFail {
		t.Errorf("expected L7-Auth FAIL step, got steps: %+v", steps)
	}
}

func TestMySQLProber_MalformedGreeting(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		// Send only 3 bytes (not even a full header)
		conn.Write([]byte{0x01, 0x02, 0x03})
	})

	prober := NewMySQLProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	// Should get L3-DNS OK, L4-TCP OK, then L7-Handshake FAIL (not a panic)
	foundFail := false
	for _, s := range steps {
		if s.Status == StatusFAIL && (s.Layer == "L7-Handshake" || s.Layer == "L7-Auth") {
			foundFail = true
		}
	}
	if !foundFail {
		t.Errorf("expected a FAIL step for malformed greeting, got: %+v", steps)
	}
}

func TestMySQLProber_EmptyResponse(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		// Accept TCP then close immediately (no data sent)
	})

	prober := NewMySQLProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	// Should get DNS OK, TCP OK, then a FAIL (not a panic)
	foundFail := false
	for _, s := range steps {
		if s.Status == StatusFAIL {
			foundFail = true
		}
	}
	if !foundFail {
		t.Errorf("expected a FAIL step for empty response, got: %+v", steps)
	}
}

func TestMySQLProber_TruncatedPacket(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		// Send a MySQL packet header claiming 1000 bytes but only send 10 bytes of payload
		header := make([]byte, 4)
		header[0] = 0xE8 // 1000 & 0xFF
		header[1] = 0x03 // 1000 >> 8
		header[2] = 0x00 // 1000 >> 16
		header[3] = 0x00 // sequence id
		conn.Write(header)
		conn.Write(make([]byte, 10)) // only 10 bytes of the promised 1000
	})

	prober := NewMySQLProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	// Must get a FAIL, must NOT panic
	foundFail := false
	for _, s := range steps {
		if s.Status == StatusFAIL {
			foundFail = true
		}
	}
	if !foundFail {
		t.Errorf("expected a FAIL step for truncated packet, got: %+v", steps)
	}
}

func TestMySQLProber_NoUsername(t *testing.T) {
	// When no username is provided, the prober should stop after handshake
	// and NOT attempt auth.
	host, port := startMockServer(t, func(conn net.Conn) {
		conn.Write(buildMockMySQLGreeting("5.7.44", 99))
		// Do not expect auth packet -- connection will close after greeting
		time.Sleep(100 * time.Millisecond)
	})

	prober := NewMySQLProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
		// No Username
	})

	// Should have L7-Handshake OK but no L7-Auth step
	foundHandshake := false
	foundAuth := false
	for _, s := range steps {
		if s.Layer == "L7-Handshake" && s.Status == StatusOK {
			foundHandshake = true
		}
		if s.Layer == "L7-Auth" {
			foundAuth = true
		}
	}
	if !foundHandshake {
		t.Error("expected L7-Handshake OK step when no username provided")
	}
	if foundAuth {
		t.Error("did not expect L7-Auth step when no username provided")
	}
}

func TestClassifyMySQLAuthResponse(t *testing.T) {
	target := ProbeTarget{
		Host:     "db.example.com",
		Port:     3306,
		Username: "admin",
		Database: "mydb",
	}

	tests := []struct {
		name       string
		pkt        []byte
		wantStatus string
		wantLayer  string
		wantInDetail string
	}{
		{
			name:       "OK packet",
			pkt:        []byte{mysqlPacketOK, 0x00, 0x00},
			wantStatus: StatusOK,
			wantLayer:  "L7-Auth",
		},
		{
			name:       "empty packet",
			pkt:        []byte{},
			wantStatus: StatusFAIL,
			wantLayer:  "L7-Auth",
		},
		{
			name: "ERR access denied",
			pkt: func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(mysqlPacketERR)
				binary.Write(&buf, binary.LittleEndian, uint16(1045))
				buf.WriteByte('#')
				buf.WriteString("28000")
				buf.WriteString("Access denied for user 'admin'@'localhost'")
				return buf.Bytes()
			}(),
			wantStatus:   StatusFAIL,
			wantLayer:    "L7-Auth",
			wantInDetail: "Access denied",
		},
		{
			name: "ERR unknown database",
			pkt: func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(mysqlPacketERR)
				binary.Write(&buf, binary.LittleEndian, uint16(1049))
				buf.WriteByte('#')
				buf.WriteString("42000")
				buf.WriteString("Unknown database 'mydb'")
				return buf.Bytes()
			}(),
			wantStatus:   StatusFAIL,
			wantLayer:    "L7-Auth",
			wantInDetail: "Unknown database",
		},
		{
			name: "ERR too many connections",
			pkt: func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(mysqlPacketERR)
				binary.Write(&buf, binary.LittleEndian, uint16(1040))
				buf.WriteByte('#')
				buf.WriteString("08004")
				buf.WriteString("Too many connections")
				return buf.Bytes()
			}(),
			wantStatus: StatusFAIL,
			wantLayer:  "L7-Auth",
		},
		{
			name:       "EOF auth switch request",
			pkt:        []byte{mysqlPacketEOF},
			wantStatus: StatusWARN,
			wantLayer:  "L7-Auth",
		},
		{
			name:       "unexpected response type",
			pkt:        []byte{0x42, 0x01, 0x02},
			wantStatus: StatusWARN,
			wantLayer:  "L7-Auth",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := classifyMySQLAuthResponse(tt.pkt, target, 10*time.Millisecond)
			if step.Layer != tt.wantLayer {
				t.Errorf("layer = %q, want %q", step.Layer, tt.wantLayer)
			}
			if step.Status != tt.wantStatus {
				t.Errorf("status = %q, want %q", step.Status, tt.wantStatus)
			}
			if tt.wantInDetail != "" && !strings.Contains(step.Detail, tt.wantInDetail) {
				t.Errorf("detail %q does not contain %q", step.Detail, tt.wantInDetail)
			}
		})
	}
}

func TestParseMySQLError(t *testing.T) {
	tests := []struct {
		name    string
		pkt     []byte
		wantSub string
	}{
		{
			name:    "too short returns unknown",
			pkt:     []byte{0xFF, 0x00},
			wantSub: "unknown error",
		},
		{
			name: "with sqlstate marker",
			pkt: func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(0xFF)
				binary.Write(&buf, binary.LittleEndian, uint16(1045))
				buf.WriteByte('#')
				buf.WriteString("28000")
				buf.WriteString("Access denied")
				return buf.Bytes()
			}(),
			wantSub: "Access denied",
		},
		{
			name: "without sqlstate marker",
			pkt: func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(0xFF)
				binary.Write(&buf, binary.LittleEndian, uint16(2000))
				buf.WriteString("Some error")
				return buf.Bytes()
			}(),
			wantSub: "Some error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseMySQLError(tt.pkt)
			if !strings.Contains(got, tt.wantSub) {
				t.Errorf("parseMySQLError() = %q, want substring %q", got, tt.wantSub)
			}
		})
	}
}

func TestMySQLProber_ServerERROnGreeting(t *testing.T) {
	// Some MySQL servers send an ERR packet immediately (e.g., max connections)
	host, port := startMockServer(t, func(conn net.Conn) {
		var errPayload bytes.Buffer
		errPayload.WriteByte(mysqlPacketERR)
		binary.Write(&errPayload, binary.LittleEndian, uint16(1040))
		errPayload.WriteByte('#')
		errPayload.WriteString("08004")
		errPayload.WriteString("Too many connections")
		conn.Write(writeMySQLPacket(errPayload.Bytes(), 0))
	})

	prober := NewMySQLProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	// Should fail at handshake
	foundFail := false
	for _, s := range steps {
		if s.Layer == "L7-Handshake" && s.Status == StatusFAIL {
			foundFail = true
		}
	}
	if !foundFail {
		t.Errorf("expected L7-Handshake FAIL when server sends ERR on greeting, got: %+v", steps)
	}
}

func TestMySQLProber_BadProtocolVersion(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		// Send a greeting with protocol version 9 (unsupported)
		var payload bytes.Buffer
		payload.WriteByte(9) // bad protocol version
		payload.WriteString("5.5.0")
		payload.WriteByte(0)
		binary.Write(&payload, binary.LittleEndian, uint32(1))
		payload.Write(make([]byte, 8)) // scramble
		payload.WriteByte(0)
		binary.Write(&payload, binary.LittleEndian, uint16(0))
		conn.Write(writeMySQLPacket(payload.Bytes(), 0))
	})

	prober := NewMySQLProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	foundFail := false
	for _, s := range steps {
		if s.Layer == "L7-Handshake" && s.Status == StatusFAIL {
			foundFail = true
			if !strings.Contains(s.Detail, "unsupported protocol") {
				t.Errorf("detail should mention unsupported protocol, got %q", s.Detail)
			}
		}
	}
	if !foundFail {
		t.Errorf("expected L7-Handshake FAIL for bad protocol version, got: %+v", steps)
	}
}

func TestMySQLNativePassword(t *testing.T) {
	// Verify that empty password returns nil
	result := mysqlNativePassword("", []byte{0x01, 0x02, 0x03})
	if result != nil {
		t.Errorf("expected nil for empty password, got %v", result)
	}

	// Verify non-empty password returns 20-byte hash
	result = mysqlNativePassword("secret", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08})
	if len(result) != 20 {
		t.Errorf("expected 20-byte hash, got %d bytes", len(result))
	}
}

func TestWriteMySQLPacket(t *testing.T) {
	payload := []byte{0x0A, 0x0B, 0x0C}
	pkt := writeMySQLPacket(payload, 5)

	// Header: 3 byte length + 1 byte sequence
	if len(pkt) != 4+3 {
		t.Fatalf("expected 7 bytes, got %d", len(pkt))
	}
	// Length should be 3 (LE)
	gotLen := int(pkt[0]) | int(pkt[1])<<8 | int(pkt[2])<<16
	if gotLen != 3 {
		t.Errorf("payload length = %d, want 3", gotLen)
	}
	// Sequence ID
	if pkt[3] != 5 {
		t.Errorf("sequence id = %d, want 5", pkt[3])
	}
	// Payload
	if !bytes.Equal(pkt[4:], payload) {
		t.Errorf("payload mismatch")
	}
}

func TestBuildMySQLHandshakeResponse(t *testing.T) {
	greeting := &mysqlGreeting{
		ProtocolVersion: 10,
		ServerVersion:   "8.0.35",
		ConnectionID:    42,
		AuthPluginData:  make([]byte, 20),
		CapabilityFlags: mysqlClientProtocol41 | mysqlClientSecureConn | mysqlClientPluginAuth | mysqlClientConnectDB,
		AuthPluginName:  "mysql_native_password",
	}

	pkt := buildMySQLHandshakeResponse(greeting, "testuser", "testpass", "mydb")

	// Should be a valid MySQL packet (4 byte header + payload)
	if len(pkt) < 4 {
		t.Fatalf("packet too short: %d bytes", len(pkt))
	}

	payloadLen := int(pkt[0]) | int(pkt[1])<<8 | int(pkt[2])<<16
	if payloadLen+4 != len(pkt) {
		t.Errorf("packet length mismatch: header says %d, actual payload = %d", payloadLen, len(pkt)-4)
	}

	// Verify username is in the payload
	if !bytes.Contains(pkt[4:], []byte("testuser")) {
		t.Error("packet should contain username 'testuser'")
	}

	// Verify database is in the payload
	if !bytes.Contains(pkt[4:], []byte("mydb")) {
		t.Error("packet should contain database 'mydb'")
	}

	// Verify auth plugin name is in the payload
	if !bytes.Contains(pkt[4:], []byte("mysql_native_password")) {
		t.Error("packet should contain auth plugin name")
	}
}

func TestBuildMySQLSSLRequest(t *testing.T) {
	greeting := &mysqlGreeting{
		ProtocolVersion: 10,
		CapabilityFlags: mysqlClientProtocol41 | mysqlClientSecureConn | mysqlClientPluginAuth,
	}

	pkt := buildMySQLSSLRequest(greeting)
	if len(pkt) < 4 {
		t.Fatalf("SSL request too short: %d bytes", len(pkt))
	}

	// Verify it is a valid MySQL packet
	payloadLen := int(pkt[0]) | int(pkt[1])<<8 | int(pkt[2])<<16
	if payloadLen+4 != len(pkt) {
		t.Errorf("packet length mismatch: header says %d, actual = %d", payloadLen, len(pkt)-4)
	}

	// Payload should be 32 bytes (4 caps + 4 max_pkt + 1 charset + 23 reserved)
	if payloadLen != 32 {
		t.Errorf("SSL request payload = %d bytes, want 32", payloadLen)
	}
}

// Integration-style test using concurrent probes to verify no races.
func TestMySQLProber_ConcurrentProbes(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		conn.Write(buildMockMySQLGreeting("8.0.35", 1))
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockMySQLOK())
	})

	// The mock server only handles one connection. We run two probes:
	// one to the mock (should succeed) and one to a bad host (should fail DNS).
	prober := NewMySQLProber()

	done := make(chan []ProbeStep, 2)

	go func() {
		steps := prober.Probe(context.Background(), ProbeTarget{
			Host:     host,
			Port:     port,
			Username: "u",
			Password: "p",
		})
		done <- steps
	}()

	go func() {
		steps := prober.Probe(context.Background(), ProbeTarget{
			Host: "no-such-host-kshark.invalid",
			Port: 3306,
		})
		done <- steps
	}()

	for i := 0; i < 2; i++ {
		select {
		case steps := <-done:
			if len(steps) == 0 {
				t.Error("expected non-empty steps")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for probe")
		}
	}
}

func TestMySQLProber_AuthEOFSwitch(t *testing.T) {
	// Server sends EOF (auth switch request) instead of OK
	host, port := startMockServer(t, func(conn net.Conn) {
		conn.Write(buildMockMySQLGreeting("8.0.35", 1))
		buf := make([]byte, 4096)
		conn.Read(buf)
		// Send auth switch (EOF packet)
		var payload bytes.Buffer
		payload.WriteByte(mysqlPacketEOF)
		payload.WriteString("caching_sha2_password")
		payload.WriteByte(0)
		payload.Write(make([]byte, 20)) // new scramble
		conn.Write(writeMySQLPacket(payload.Bytes(), 2))
	})

	prober := NewMySQLProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Username: "user",
		Password: "pass",
	})

	// Should get L7-Auth WARN (auth switch)
	foundWarn := false
	for _, s := range steps {
		if s.Layer == "L7-Auth" && s.Status == StatusWARN {
			foundWarn = true
			if !strings.Contains(s.Detail, "auth method switch") {
				t.Errorf("expected 'auth method switch' in detail, got %q", s.Detail)
			}
		}
	}
	if !foundWarn {
		t.Errorf("expected L7-Auth WARN for EOF auth switch, got: %+v", steps)
	}
}

func TestMySQLProber_HostNotAllowed(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		conn.Write(buildMockMySQLGreeting("8.0.35", 1))
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockMySQLERR(1130, "HY000",
			fmt.Sprintf("Host '10.0.0.1' is not allowed to connect to this MySQL server")))
	})

	prober := NewMySQLProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Username: "admin",
		Password: "pass",
	})

	foundFail := false
	for _, s := range steps {
		if s.Layer == "L7-Auth" && s.Status == StatusFAIL {
			foundFail = true
			if !strings.Contains(s.Hint, "host") {
				t.Errorf("hint should mention host restriction, got %q", s.Hint)
			}
		}
	}
	if !foundFail {
		t.Errorf("expected L7-Auth FAIL for host not allowed, got: %+v", steps)
	}
}
