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

// buildMockTDSPreLoginResponse builds a TDS Pre-Login response packet.
// Returns a complete TDS packet (header + payload).
func buildMockTDSPreLoginResponse(major, minor byte, build uint16, encryption byte) []byte {
	// Build pre-login option data
	// Options: VERSION (6 bytes), ENCRYPTION (1 byte)
	// Option header: token(1) + offset(2,BE) + length(2,BE) per option + terminator(1)
	numOptions := 2
	headerSize := numOptions*5 + 1 // 2 options * 5 bytes each + 1 terminator

	versionData := make([]byte, 6)
	versionData[0] = major
	versionData[1] = minor
	binary.BigEndian.PutUint16(versionData[2:4], build)
	versionData[4] = 0x00
	versionData[5] = 0x00

	encryptData := []byte{encryption}

	offset := uint16(headerSize)

	var payload bytes.Buffer

	// VERSION option header
	payload.WriteByte(tdsPreLoginVersion)
	binary.Write(&payload, binary.BigEndian, offset)
	binary.Write(&payload, binary.BigEndian, uint16(len(versionData)))
	offset += uint16(len(versionData))

	// ENCRYPTION option header
	payload.WriteByte(tdsPreLoginEncryption)
	binary.Write(&payload, binary.BigEndian, offset)
	binary.Write(&payload, binary.BigEndian, uint16(len(encryptData)))

	// Terminator
	payload.WriteByte(tdsPreLoginTerminator)

	// Option data
	payload.Write(versionData)
	payload.Write(encryptData)

	return writeTDSPacket(tdsPacketResponse, payload.Bytes())
}

func TestSQLServerProber_Type(t *testing.T) {
	p := NewSQLServerProber()
	if got := p.Type(); got != "sqlserver" {
		t.Errorf("Type() = %q, want %q", got, "sqlserver")
	}
}

func TestSQLServerProber_HappyPath(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		// Read client Pre-Login request (just consume it)
		buf := make([]byte, 4096)
		conn.Read(buf)

		// Send valid TDS Pre-Login response
		resp := buildMockTDSPreLoginResponse(15, 0, 4100, tdsEncryptOff)
		conn.Write(resp)
	})

	prober := NewSQLServerProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
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

	// L7-PreLogin OK
	foundPreLogin := false
	for _, s := range steps {
		if s.Layer == "L7-PreLogin" && s.Status == StatusOK {
			foundPreLogin = true
			if !strings.Contains(s.Detail, "15.0.4100") {
				t.Errorf("pre-login detail should contain version '15.0.4100', got %q", s.Detail)
			}
			if !strings.Contains(s.Detail, "OFF") {
				t.Errorf("pre-login detail should contain encryption 'OFF', got %q", s.Detail)
			}
		}
	}
	if !foundPreLogin {
		t.Errorf("expected L7-PreLogin OK step, got: %+v", steps)
	}
}

func TestSQLServerProber_HappyPathWithUsername(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockTDSPreLoginResponse(16, 0, 1000, tdsEncryptOff))
	})

	prober := NewSQLServerProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Username: "sa",
		Password: "MyP@ssw0rd",
	})

	// Should get L7-Auth SKIP (Login7 not implemented)
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

func TestSQLServerProber_DNSFailure(t *testing.T) {
	prober := NewSQLServerProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: "this-host-does-not-exist-kshark-test.invalid",
		Port: 1433,
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

func TestSQLServerProber_TCPFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	prober := NewSQLServerProber()
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

func TestSQLServerProber_MalformedPreLogin(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)

		// Send only 4 bytes -- not even a complete TDS header (needs 8)
		conn.Write([]byte{0x04, 0x01, 0x00, 0x04})
	})

	prober := NewSQLServerProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	// Should get a FAIL (not a panic)
	foundFail := false
	for _, s := range steps {
		if s.Status == StatusFAIL && s.Layer == "L7-PreLogin" {
			foundFail = true
		}
	}
	if !foundFail {
		t.Errorf("expected L7-PreLogin FAIL for malformed response, got: %+v", steps)
	}
}

func TestSQLServerProber_EmptyResponse(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		// Read the pre-login request then close without responding
		buf := make([]byte, 4096)
		conn.Read(buf)
	})

	prober := NewSQLServerProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	// Should get DNS OK, TCP OK, then L7-PreLogin FAIL (not a panic)
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

func TestSQLServerProber_NotTDSServer(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)

		// Send an HTTP response instead of TDS -- but as a valid 8-byte TDS header
		// with wrong packet type (0x48 = 'H' from HTTP/1.1)
		httpResp := []byte("HTTP/1.1 400 Bad Request\r\n\r\n")
		conn.Write(httpResp)
	})

	prober := NewSQLServerProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	// The prober will try to read 8 bytes as TDS header.
	// "HTTP/1.1" is 8 bytes, so it reads them, then interprets the length field.
	// This should fail when trying to read the payload or with a bad packet type.
	foundFail := false
	for _, s := range steps {
		if s.Status == StatusFAIL && s.Layer == "L7-PreLogin" {
			foundFail = true
		}
	}
	if !foundFail {
		// Could also be WARN if it parses but gets unexpected type
		foundWarnOrFail := false
		for _, s := range steps {
			if s.Layer == "L7-PreLogin" && (s.Status == StatusFAIL || s.Status == StatusWARN) {
				foundWarnOrFail = true
			}
		}
		if !foundWarnOrFail {
			t.Errorf("expected L7-PreLogin FAIL or WARN for non-TDS server, got: %+v", steps)
		}
	}
}

func TestSQLServerProber_WrongResponseType(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)

		// Send a TDS packet with wrong type (PreLogin=0x12 instead of Response=0x04)
		resp := writeTDSPacket(tdsPacketPreLogin, []byte{0xFF}) // terminator only
		conn.Write(resp)
	})

	prober := NewSQLServerProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	foundFail := false
	for _, s := range steps {
		if s.Layer == "L7-PreLogin" && s.Status == StatusFAIL {
			foundFail = true
			if !strings.Contains(s.Detail, "Unexpected TDS response type") {
				t.Errorf("detail should mention unexpected type, got %q", s.Detail)
			}
		}
	}
	if !foundFail {
		t.Errorf("expected L7-PreLogin FAIL for wrong response type, got: %+v", steps)
	}
}

func TestSQLServerProber_EncryptionValues(t *testing.T) {
	tests := []struct {
		name         string
		encryption   byte
		wantInDetail string
	}{
		{name: "encrypt OFF", encryption: tdsEncryptOff, wantInDetail: "OFF"},
		{name: "encrypt ON", encryption: tdsEncryptOn, wantInDetail: "ON"},
		{name: "encrypt NOT_SUPPORTED", encryption: tdsEncryptNotSup, wantInDetail: "NOT_SUPPORTED"},
		{name: "encrypt REQUIRED", encryption: tdsEncryptRequired, wantInDetail: "REQUIRED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port := startMockServer(t, func(conn net.Conn) {
				buf := make([]byte, 4096)
				conn.Read(buf)
				conn.Write(buildMockTDSPreLoginResponse(15, 0, 4100, tt.encryption))
			})

			prober := NewSQLServerProber()
			steps := prober.Probe(context.Background(), ProbeTarget{
				Host: host,
				Port: port,
			})

			found := false
			for _, s := range steps {
				if s.Layer == "L7-PreLogin" && (s.Status == StatusOK || s.Status == StatusWARN) {
					found = true
					if !strings.Contains(s.Detail, tt.wantInDetail) {
						t.Errorf("detail should contain %q, got %q", tt.wantInDetail, s.Detail)
					}
				}
			}
			if !found {
				t.Errorf("expected L7-PreLogin OK/WARN, got: %+v", steps)
			}
		})
	}
}

func TestTDSEncryptionString(t *testing.T) {
	tests := []struct {
		enc  byte
		want string
	}{
		{tdsEncryptOff, "OFF"},
		{tdsEncryptOn, "ON"},
		{tdsEncryptNotSup, "NOT_SUPPORTED"},
		{tdsEncryptRequired, "REQUIRED"},
		{0x99, "UNKNOWN(0x99)"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tdsEncryptionString(tt.enc)
			if got != tt.want {
				t.Errorf("tdsEncryptionString(0x%02X) = %q, want %q", tt.enc, got, tt.want)
			}
		})
	}
}

func TestParseTDSPreLoginResponse(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantVer string
		wantEnc byte
		wantErr bool
	}{
		{
			name:    "empty data",
			data:    []byte{},
			wantVer: "",
			wantErr: true,
		},
		{
			name:    "terminator only",
			data:    []byte{tdsPreLoginTerminator},
			wantVer: "unknown",
			wantEnc: 0,
			wantErr: false,
		},
		{
			name:    "truncated option header",
			data:    []byte{tdsPreLoginVersion, 0x00}, // only 2 bytes, need 5
			wantErr: true,
		},
		{
			name: "valid response",
			data: func() []byte {
				// Build a valid pre-login response with VERSION and ENCRYPTION
				headerSize := 2*5 + 1                                // 2 options * 5 + terminator
				versionData := []byte{15, 0, 0x10, 0x10, 0x00, 0x00} // 15.0.4112
				encData := []byte{tdsEncryptOff}

				offset := uint16(headerSize)
				var buf bytes.Buffer

				// VERSION
				buf.WriteByte(tdsPreLoginVersion)
				binary.Write(&buf, binary.BigEndian, offset)
				binary.Write(&buf, binary.BigEndian, uint16(len(versionData)))
				offset += uint16(len(versionData))

				// ENCRYPTION
				buf.WriteByte(tdsPreLoginEncryption)
				binary.Write(&buf, binary.BigEndian, offset)
				binary.Write(&buf, binary.BigEndian, uint16(len(encData)))

				// Terminator
				buf.WriteByte(tdsPreLoginTerminator)

				// Data
				buf.Write(versionData)
				buf.Write(encData)

				return buf.Bytes()
			}(),
			wantVer: "15.0.4112",
			wantEnc: tdsEncryptOff,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ver, enc, err := parseTDSPreLoginResponse(tt.data)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ver != tt.wantVer {
				t.Errorf("version = %q, want %q", ver, tt.wantVer)
			}
			if enc != tt.wantEnc {
				t.Errorf("encryption = 0x%02X, want 0x%02X", enc, tt.wantEnc)
			}
		})
	}
}

func TestBuildTDSPreLogin(t *testing.T) {
	tests := []struct {
		name              string
		requestEncryption bool
	}{
		{"encryption off", false},
		{"encryption on", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pkt := buildTDSPreLogin(tt.requestEncryption)

			// Minimum size: 8 byte TDS header + at least a few bytes payload
			if len(pkt) < 9 {
				t.Fatalf("packet too short: %d bytes", len(pkt))
			}

			// Verify TDS header
			if pkt[0] != tdsPacketPreLogin {
				t.Errorf("packet type = 0x%02X, want 0x%02X", pkt[0], tdsPacketPreLogin)
			}
			if pkt[1] != tdsStatusEOM {
				t.Errorf("status = 0x%02X, want 0x%02X (EOM)", pkt[1], tdsStatusEOM)
			}

			totalLen := binary.BigEndian.Uint16(pkt[2:4])
			if int(totalLen) != len(pkt) {
				t.Errorf("length field = %d, actual = %d", totalLen, len(pkt))
			}
		})
	}
}

func TestWriteTDSPacket(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03}
	pkt := writeTDSPacket(0x12, payload)

	if len(pkt) != 8+3 {
		t.Fatalf("expected 11 bytes, got %d", len(pkt))
	}
	if pkt[0] != 0x12 {
		t.Errorf("type = 0x%02X, want 0x12", pkt[0])
	}
	if pkt[1] != tdsStatusEOM {
		t.Errorf("status = 0x%02X, want 0x%02X", pkt[1], tdsStatusEOM)
	}
	totalLen := binary.BigEndian.Uint16(pkt[2:4])
	if totalLen != 11 {
		t.Errorf("length = %d, want 11", totalLen)
	}
}

func TestClassifySQLServerError(t *testing.T) {
	target := ProbeTarget{
		Host:     "db.example.com",
		Port:     1433,
		Username: "sa",
		Database: "master",
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
			errMsg:    "dial tcp 10.0.0.1:1433: connection refused",
			wantLayer: "L4-TCP",
			wantFail:  true,
		},
		{
			name:      "i/o timeout -> L4-TCP FAIL",
			errMsg:    "dial tcp 10.0.0.1:1433: i/o timeout",
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
			name:      "certificate error -> L5-6-TLS FAIL",
			errMsg:    "tls: certificate signed by unknown authority",
			wantLayer: "L5-6-TLS",
			wantFail:  true,
		},
		{
			name:      "x509 error -> L5-6-TLS FAIL",
			errMsg:    "x509: certificate has expired",
			wantLayer: "L5-6-TLS",
			wantFail:  true,
		},
		{
			name:      "login failed -> L7-Auth FAIL",
			errMsg:    "Login failed for user 'sa'",
			wantLayer: "L7-Auth",
			wantFail:  true,
		},
		{
			name:      "generic error -> L7-Ready FAIL",
			errMsg:    "some unknown error",
			wantLayer: "L7-Ready",
			wantFail:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := classifySQLServerError(fmt.Errorf("%s", tt.errMsg), target)
			if step.Layer != tt.wantLayer {
				t.Errorf("layer = %q, want %q", step.Layer, tt.wantLayer)
			}
			if tt.wantFail && step.Status != StatusFAIL {
				t.Errorf("status = %q, want FAIL", step.Status)
			}
		})
	}
}

func TestSQLServerProber_TruncatedPayload(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)

		// Send a TDS header claiming 100 bytes total but only provide the 8-byte header
		header := make([]byte, 8)
		header[0] = tdsPacketResponse
		header[1] = tdsStatusEOM
		binary.BigEndian.PutUint16(header[2:4], 100) // claims 100 bytes total
		conn.Write(header)
		// Only send 5 bytes of payload (promised 92)
		conn.Write([]byte{0x01, 0x02, 0x03, 0x04, 0x05})
	})

	prober := NewSQLServerProber()
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
		t.Errorf("expected FAIL step for truncated TDS payload, got: %+v", steps)
	}
}

func TestSQLServerProber_ConcurrentProbes(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write(buildMockTDSPreLoginResponse(15, 0, 4100, tdsEncryptOff))
	})

	prober := NewSQLServerProber()
	done := make(chan []ProbeStep, 2)

	go func() {
		steps := prober.Probe(context.Background(), ProbeTarget{
			Host: host,
			Port: port,
		})
		done <- steps
	}()

	go func() {
		steps := prober.Probe(context.Background(), ProbeTarget{
			Host: "no-such-host-kshark.invalid",
			Port: 1433,
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
