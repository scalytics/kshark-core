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

// buildMockTNSAccept builds a TNS Accept packet.
func buildMockTNSAccept() []byte {
	// TNS Accept has a minimal structure:
	// TNS Header (8 bytes) + Accept payload (at least version + service options)
	var payload bytes.Buffer

	// Accept version (2 bytes)
	binary.Write(&payload, binary.BigEndian, uint16(0x0139))
	// Service options (2 bytes)
	binary.Write(&payload, binary.BigEndian, uint16(0x0000))
	// SDU size (2 bytes)
	binary.Write(&payload, binary.BigEndian, uint16(0x2000))
	// TDU size (2 bytes)
	binary.Write(&payload, binary.BigEndian, uint16(0x7FFF))
	// Some padding
	payload.Write(make([]byte, 8))

	return buildTNSPacket(tnsPacketAccept, payload.Bytes())
}

// buildMockTNSRefuse builds a TNS Refuse packet with the given reason.
func buildMockTNSRefuse(reasonUser, reasonSystem byte, data string) []byte {
	var payload bytes.Buffer

	// Reason user (1 byte)
	payload.WriteByte(reasonUser)
	// Reason system (1 byte)
	payload.WriteByte(reasonSystem)
	// Data length (2 bytes, big-endian)
	binary.Write(&payload, binary.BigEndian, uint16(len(data)))
	// Data
	payload.WriteString(data)

	return buildTNSPacket(tnsPacketRefuse, payload.Bytes())
}

// buildMockTNSRedirect builds a TNS Redirect packet.
func buildMockTNSRedirect(redirectData string) []byte {
	return buildTNSPacket(tnsPacketRedirect, []byte(redirectData))
}

// buildMockTNSResend builds a TNS Resend packet (no payload).
func buildMockTNSResend() []byte {
	return buildTNSPacket(tnsPacketResend, nil)
}

// buildTNSPacket constructs a raw TNS packet with the given type and payload.
func buildTNSPacket(pktType byte, payload []byte) []byte {
	totalLen := 8 + len(payload)
	var buf bytes.Buffer

	// TNS Header (8 bytes)
	binary.Write(&buf, binary.BigEndian, uint16(totalLen)) // Packet length
	binary.Write(&buf, binary.BigEndian, uint16(0))        // Packet checksum
	buf.WriteByte(pktType)                                 // Packet type
	buf.WriteByte(0)                                       // Reserved
	binary.Write(&buf, binary.BigEndian, uint16(0))        // Header checksum

	if payload != nil {
		buf.Write(payload)
	}

	return buf.Bytes()
}

func TestOracleProber_Type(t *testing.T) {
	p := NewOracleProber()
	if got := p.Type(); got != "oracle" {
		t.Errorf("Type() = %q, want %q", got, "oracle")
	}
}

func TestOracleProber_HappyPath(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		// Read client TNS Connect
		buf := make([]byte, 4096)
		conn.Read(buf)

		// Send TNS Accept
		conn.Write(buildMockTNSAccept())
	})

	prober := NewOracleProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Database: "ORCL",
	})

	if len(steps) < 3 {
		t.Fatalf("expected at least 3 steps, got %d: %+v", len(steps), steps)
	}

	// L3-DNS OK
	if steps[0].Layer != "L3-DNS" || steps[0].Status != StatusOK {
		t.Errorf("step[0] = %s/%s, want L3-DNS/OK", steps[0].Layer, steps[0].Status)
	}

	// L4-TCP OK
	if steps[1].Layer != "L4-TCP" || steps[1].Status != StatusOK {
		t.Errorf("step[1] = %s/%s, want L4-TCP/OK", steps[1].Layer, steps[1].Status)
	}

	// L7-TNS OK
	foundTNS := false
	for _, s := range steps {
		if s.Layer == "L7-TNS" && s.Status == StatusOK {
			foundTNS = true
			if !strings.Contains(s.Detail, "ORCL") {
				t.Errorf("TNS detail should contain service name 'ORCL', got %q", s.Detail)
			}
			if !strings.Contains(s.Detail, "accepted") {
				t.Errorf("TNS detail should contain 'accepted', got %q", s.Detail)
			}
		}
	}
	if !foundTNS {
		t.Errorf("expected L7-TNS OK step, got: %+v", steps)
	}
}

func TestOracleProber_HappyPathDefaultService(t *testing.T) {
	// When no Database is set, the prober should default to "ORCL"
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockTNSAccept())
	})

	prober := NewOracleProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
		// No Database set
	})

	foundTNS := false
	for _, s := range steps {
		if s.Layer == "L7-TNS" && s.Status == StatusOK {
			foundTNS = true
			if !strings.Contains(s.Detail, "ORCL") {
				t.Errorf("should default to ORCL service, got %q", s.Detail)
			}
		}
	}
	if !foundTNS {
		t.Errorf("expected L7-TNS OK step, got: %+v", steps)
	}
}

func TestOracleProber_Refused(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockTNSRefuse(0x00, 0x00,
			"(DESCRIPTION=(ERR=1189)(VSNNUM=0)(ERROR_STACK=(ERROR=(CODE=1189)(EMFI=4)(ARGS='ORCL'))))"))
	})

	prober := NewOracleProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Database: "ORCL",
	})

	foundFail := false
	for _, s := range steps {
		if s.Layer == "L7-TNS" && s.Status == StatusFAIL {
			foundFail = true
			if !strings.Contains(s.Detail, "refused") {
				t.Errorf("detail should mention 'refused', got %q", s.Detail)
			}
		}
	}
	if !foundFail {
		t.Errorf("expected L7-TNS FAIL for refused connection, got: %+v", steps)
	}
}

func TestOracleProber_RefusedUnknownService(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockTNSRefuse(0x00, 0x00,
			"(ERR=12514)(VSNNUM=0)(ERROR_STACK=(ERROR=(CODE=12514)(EMFI=4)(ARGS='unknown service')))"))
	})

	prober := NewOracleProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Database: "BADSERVICE",
	})

	foundFail := false
	for _, s := range steps {
		if s.Layer == "L7-TNS" && s.Status == StatusFAIL {
			foundFail = true
			// Hint should mention the service name
			if !strings.Contains(s.Hint, "BADSERVICE") {
				t.Errorf("hint should mention service 'BADSERVICE', got %q", s.Hint)
			}
		}
	}
	if !foundFail {
		t.Errorf("expected L7-TNS FAIL for unknown service, got: %+v", steps)
	}
}

func TestOracleProber_Redirect(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockTNSRedirect("(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.0.2)(PORT=1521))"))
	})

	prober := NewOracleProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Database: "ORCL",
	})

	foundOK := false
	for _, s := range steps {
		if s.Layer == "L7-TNS" && s.Status == StatusOK {
			foundOK = true
			if !strings.Contains(s.Detail, "redirected") {
				t.Errorf("detail should mention 'redirected', got %q", s.Detail)
			}
		}
	}
	if !foundOK {
		t.Errorf("expected L7-TNS OK for redirect, got: %+v", steps)
	}
}

func TestOracleProber_Resend(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockTNSResend())
	})

	prober := NewOracleProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Database: "ORCL",
	})

	foundWarn := false
	for _, s := range steps {
		if s.Layer == "L7-TNS" && s.Status == StatusWARN {
			foundWarn = true
			if !strings.Contains(s.Detail, "resend") {
				t.Errorf("detail should mention 'resend', got %q", s.Detail)
			}
		}
	}
	if !foundWarn {
		t.Errorf("expected L7-TNS WARN for resend, got: %+v", steps)
	}
}

func TestOracleProber_DNSFailure(t *testing.T) {
	prober := NewOracleProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: "this-host-does-not-exist-kshark-test.invalid",
		Port: 1521,
	})

	if len(steps) == 0 {
		t.Fatal("expected at least 1 step")
	}
	if steps[0].Layer != "L3-DNS" || steps[0].Status != StatusFAIL {
		t.Errorf("step[0] = %s/%s, want L3-DNS/FAIL", steps[0].Layer, steps[0].Status)
	}
	if len(steps) != 1 {
		t.Errorf("expected exactly 1 step after DNS failure, got %d", len(steps))
	}
}

func TestOracleProber_TCPFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	prober := NewOracleProber()
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
	if len(steps) != 2 {
		t.Errorf("expected exactly 2 steps after TCP failure, got %d", len(steps))
	}
}

func TestOracleProber_MalformedTNS(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		// Send only 1 byte -- not even a full TNS header
		conn.Write([]byte{0x01})
	})

	prober := NewOracleProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	// Must get FAIL, not panic
	foundFail := false
	for _, s := range steps {
		if s.Status == StatusFAIL && s.Layer == "L7-TNS" {
			foundFail = true
		}
	}
	if !foundFail {
		t.Errorf("expected L7-TNS FAIL for malformed TNS, got: %+v", steps)
	}
}

func TestOracleProber_EmptyResponse(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		// Read client request then close without sending anything
		buf := make([]byte, 4096)
		conn.Read(buf)
	})

	prober := NewOracleProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	// Must FAIL, not panic
	foundFail := false
	for _, s := range steps {
		if s.Status == StatusFAIL {
			foundFail = true
		}
	}
	if !foundFail {
		t.Errorf("expected FAIL step for empty response, got: %+v", steps)
	}
}

func TestOracleProber_TruncatedPayload(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)

		// Send a TNS header claiming 200 bytes total but only provide the 8-byte header
		header := make([]byte, 8)
		binary.BigEndian.PutUint16(header[0:2], 200)
		header[4] = tnsPacketAccept
		conn.Write(header)
		// Send 5 bytes of payload (promised 192)
		conn.Write([]byte{0x01, 0x02, 0x03, 0x04, 0x05})
	})

	prober := NewOracleProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	foundFail := false
	for _, s := range steps {
		if s.Status == StatusFAIL {
			foundFail = true
		}
	}
	if !foundFail {
		t.Errorf("expected FAIL for truncated TNS payload, got: %+v", steps)
	}
}

func TestOracleProber_UnexpectedPacketType(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		// Send TNS Data packet (type 6) -- unexpected for connect handshake
		conn.Write(buildTNSPacket(tnsPacketData, []byte("unexpected data")))
	})

	prober := NewOracleProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Database: "ORCL",
	})

	foundWarn := false
	for _, s := range steps {
		if s.Layer == "L7-TNS" && s.Status == StatusWARN {
			foundWarn = true
			if !strings.Contains(s.Detail, "unexpected") {
				t.Errorf("detail should mention 'unexpected', got %q", s.Detail)
			}
		}
	}
	if !foundWarn {
		t.Errorf("expected L7-TNS WARN for unexpected packet type, got: %+v", steps)
	}
}

func TestOracleProber_WithUsername(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockTNSAccept())
	})

	prober := NewOracleProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Database: "ORCL",
		Username: "sys",
		Password: "oracle",
	})

	// Should get L7-Auth SKIP (TNS auth not implemented)
	foundAuthSkip := false
	for _, s := range steps {
		if s.Layer == "L7-Auth" && s.Status == StatusSKIP {
			foundAuthSkip = true
		}
	}
	if !foundAuthSkip {
		t.Errorf("expected L7-Auth SKIP when username provided, got: %+v", steps)
	}
}

func TestParseTNSRefuseData(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantSub string
	}{
		{
			name:    "empty data",
			data:    []byte{},
			wantSub: "no details",
		},
		{
			name:    "too short (1 byte)",
			data:    []byte{0x01},
			wantSub: "raw:",
		},
		{
			name:    "too short (3 bytes)",
			data:    []byte{0x01, 0x02, 0x03},
			wantSub: "raw:",
		},
		{
			name: "valid refuse with data",
			data: func() []byte {
				msg := "ORA-12514: TNS:listener does not currently know of service"
				var buf bytes.Buffer
				buf.WriteByte(0x00) // reason user
				buf.WriteByte(0x00) // reason system
				binary.Write(&buf, binary.BigEndian, uint16(len(msg)))
				buf.WriteString(msg)
				return buf.Bytes()
			}(),
			wantSub: "ORA-12514",
		},
		{
			name: "refuse without data",
			data: func() []byte {
				var buf bytes.Buffer
				buf.WriteByte(0x01) // reason user
				buf.WriteByte(0x02) // reason system
				binary.Write(&buf, binary.BigEndian, uint16(0))
				return buf.Bytes()
			}(),
			wantSub: "reason(user=1, system=2)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseTNSRefuseData(tt.data)
			if !strings.Contains(got, tt.wantSub) {
				t.Errorf("parseTNSRefuseData() = %q, want substring %q", got, tt.wantSub)
			}
		})
	}
}

func TestBuildOracleConnectDescriptor(t *testing.T) {
	desc := buildOracleConnectDescriptor("myhost", 1521, "MYSERVICE")

	required := []string{
		"(HOST=myhost)",
		"(PORT=1521)",
		"(SERVICE_NAME=MYSERVICE)",
		"(PROGRAM=kshark)",
	}

	for _, sub := range required {
		if !strings.Contains(desc, sub) {
			t.Errorf("descriptor %q does not contain %q", desc, sub)
		}
	}
}

func TestBuildTNSConnect(t *testing.T) {
	desc := "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ORCL)))"
	pkt := buildTNSConnect(desc)

	// Minimum size: 8 header + 24 connect header + descriptor
	if len(pkt) < 32+len(desc) {
		t.Fatalf("packet too short: %d bytes", len(pkt))
	}

	// Check TNS header
	totalLen := binary.BigEndian.Uint16(pkt[0:2])
	if int(totalLen) != len(pkt) {
		t.Errorf("total length field = %d, actual = %d", totalLen, len(pkt))
	}
	if pkt[4] != tnsPacketConnect {
		t.Errorf("packet type = 0x%02X, want 0x%02X (Connect)", pkt[4], tnsPacketConnect)
	}

	// The connect descriptor should be in the packet
	if !bytes.Contains(pkt, []byte(desc)) {
		t.Error("packet should contain the connect descriptor")
	}
}

func TestClassifyOracleError(t *testing.T) {
	target := ProbeTarget{
		Host:     "db.example.com",
		Port:     1521,
		Username: "sys",
		Database: "ORCL",
	}

	tests := []struct {
		name      string
		errMsg    string
		wantLayer string
		wantFail  bool
	}{
		{
			name:      "no such host -> L3-DNS FAIL",
			errMsg:    "lookup db.example.com: no such host",
			wantLayer: "L3-DNS",
			wantFail:  true,
		},
		{
			name:      "connection refused -> L4-TCP FAIL",
			errMsg:    "dial tcp 10.0.0.1:1521: connection refused",
			wantLayer: "L4-TCP",
			wantFail:  true,
		},
		{
			name:      "i/o timeout -> L4-TCP FAIL",
			errMsg:    "dial tcp 10.0.0.1:1521: i/o timeout",
			wantLayer: "L4-TCP",
			wantFail:  true,
		},
		{
			name:      "context deadline exceeded -> L4-TCP FAIL",
			errMsg:    "context deadline exceeded",
			wantLayer: "L4-TCP",
			wantFail:  true,
		},
		{
			name:      "ORA-12514 service not registered -> L7-TNS FAIL",
			errMsg:    "ORA-12514: TNS:listener does not currently know of service",
			wantLayer: "L7-TNS",
			wantFail:  true,
		},
		{
			name:      "ORA-01017 invalid user/password -> L7-Auth FAIL",
			errMsg:    "ORA-01017: invalid username/password; logon denied",
			wantLayer: "L7-Auth",
			wantFail:  true,
		},
		{
			name:      "generic error -> L7-Ready FAIL",
			errMsg:    "some unknown oracle error",
			wantLayer: "L7-Ready",
			wantFail:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := classifyOracleError(fmt.Errorf("%s", tt.errMsg), target)
			if step.Layer != tt.wantLayer {
				t.Errorf("layer = %q, want %q", step.Layer, tt.wantLayer)
			}
			if tt.wantFail && step.Status != StatusFAIL {
				t.Errorf("status = %q, want FAIL", step.Status)
			}
		})
	}
}

func TestOracleProber_ConcurrentProbes(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockTNSAccept())
	})

	prober := NewOracleProber()
	done := make(chan []ProbeStep, 2)

	go func() {
		steps := prober.Probe(context.Background(), ProbeTarget{
			Host:     host,
			Port:     port,
			Database: "ORCL",
		})
		done <- steps
	}()

	go func() {
		steps := prober.Probe(context.Background(), ProbeTarget{
			Host: "no-such-host-kshark.invalid",
			Port: 1521,
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

func TestOracleProber_RefuseNoListener(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockTNSRefuse(0x00, 0x00,
			"(DESCRIPTION=(ERR=1189)(VSNNUM=0)(ERROR_STACK=(ERROR=(CODE=1189)(EMFI=4)(ARGS='no listener'))))"))
	})

	prober := NewOracleProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Database: "MYDB",
	})

	foundFail := false
	for _, s := range steps {
		if s.Layer == "L7-TNS" && s.Status == StatusFAIL {
			foundFail = true
			// Hint should mention the service since "no listener" is in the refuse data
			if !strings.Contains(s.Hint, "MYDB") {
				t.Errorf("hint should mention service 'MYDB', got %q", s.Hint)
			}
		}
	}
	if !foundFail {
		t.Errorf("expected L7-TNS FAIL for no listener refuse, got: %+v", steps)
	}
}
