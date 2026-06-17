package probe

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

func TestRedisProber_Type(t *testing.T) {
	p := NewRedisProber()
	if got := p.Type(); got != "redis" {
		t.Errorf("Type() = %q, want %q", got, "redis")
	}
}

func TestRedisProber_HappyPath(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)

		// Read PING command
		conn.Read(buf)
		// Send +PONG
		conn.Write([]byte("+PONG\r\n"))

		// Read INFO command
		conn.Read(buf)
		// Send bulk string with INFO response
		info := "# Server\r\nredis_version:7.2.4\r\nredis_mode:standalone\r\n"
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(info), info)))
	})

	prober := NewRedisProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	// Expect: L3-DNS OK, L4-TCP OK, L5-6-TLS SKIP, L7-Redis-Ping OK, L7-Redis-Info OK
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

	// Find L7-Redis-Ping OK
	foundPing := false
	for _, s := range steps {
		if s.Layer == "L7-Redis-Ping" && s.Status == StatusOK {
			foundPing = true
			if !strings.Contains(s.Detail, "PONG") {
				t.Errorf("ping detail should contain PONG, got %q", s.Detail)
			}
		}
	}
	if !foundPing {
		t.Error("expected L7-Redis-Ping OK step")
	}

	// Find L7-Redis-Info OK with version
	foundInfo := false
	for _, s := range steps {
		if s.Layer == "L7-Redis-Info" && s.Status == StatusOK {
			foundInfo = true
			if !strings.Contains(s.Detail, "7.2.4") {
				t.Errorf("info detail should contain version 7.2.4, got %q", s.Detail)
			}
		}
	}
	if !foundInfo {
		t.Error("expected L7-Redis-Info OK step")
	}
}

func TestRedisProber_HappyPathWithAuth(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)

		// Read AUTH command
		conn.Read(buf)
		// Send +OK
		conn.Write([]byte("+OK\r\n"))

		// Read PING command
		conn.Read(buf)
		// Send +PONG
		conn.Write([]byte("+PONG\r\n"))

		// Read INFO command
		conn.Read(buf)
		info := "# Server\r\nredis_version:7.0.0\r\n"
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(info), info)))
	})

	prober := NewRedisProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Username: "default",
		Password: "secret123",
	})

	// Find L7-Redis-Auth OK
	foundAuth := false
	for _, s := range steps {
		if s.Layer == "L7-Redis-Auth" && s.Status == StatusOK {
			foundAuth = true
		}
	}
	if !foundAuth {
		t.Errorf("expected L7-Redis-Auth OK step, got: %+v", steps)
	}

	// Find L7-Redis-Ping OK
	foundPing := false
	for _, s := range steps {
		if s.Layer == "L7-Redis-Ping" && s.Status == StatusOK {
			foundPing = true
		}
	}
	if !foundPing {
		t.Errorf("expected L7-Redis-Ping OK step, got: %+v", steps)
	}
}

func TestRedisProber_DNSFailure(t *testing.T) {
	prober := NewRedisProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: "this-host-does-not-exist-kshark-test.invalid",
		Port: 6379,
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

func TestRedisProber_TCPFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	prober := NewRedisProber()
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

func TestRedisProber_AuthFailure(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)

		// Read AUTH command
		conn.Read(buf)
		// Send WRONGPASS error
		conn.Write([]byte("-WRONGPASS invalid username-password pair or user is disabled.\r\n"))
	})

	prober := NewRedisProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Password: "wrongpass",
	})

	foundAuthFail := false
	for _, s := range steps {
		if s.Layer == "L7-Redis-Auth" && s.Status == StatusFAIL {
			foundAuthFail = true
			if !strings.Contains(s.Detail, "WRONGPASS") {
				t.Errorf("auth detail should mention WRONGPASS, got %q", s.Detail)
			}
		}
	}
	if !foundAuthFail {
		t.Errorf("expected L7-Redis-Auth FAIL step, got steps: %+v", steps)
	}
}

func TestRedisProber_NoAuthRequired(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)

		// Read AUTH command
		conn.Read(buf)
		// Send error - no auth required
		conn.Write([]byte("-ERR Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?\r\n"))
	})

	prober := NewRedisProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Password: "unnecessary",
	})

	// Should get WARN, not FAIL
	foundWarn := false
	for _, s := range steps {
		if s.Layer == "L7-Redis-Auth" && s.Status == StatusWARN {
			foundWarn = true
		}
	}
	if !foundWarn {
		t.Errorf("expected L7-Redis-Auth WARN step when no auth required, got: %+v", steps)
	}
}

func TestRedisProber_RequiresAuth(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)

		// Read PING command
		conn.Read(buf)
		// Send NOAUTH error
		conn.Write([]byte("-NOAUTH Authentication required.\r\n"))
	})

	prober := NewRedisProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	foundFail := false
	for _, s := range steps {
		if s.Layer == "L7-Redis-Ping" && s.Status == StatusFAIL {
			foundFail = true
			if !strings.Contains(s.Detail, "NOAUTH") {
				t.Errorf("detail should mention NOAUTH, got %q", s.Detail)
			}
			if !strings.Contains(s.Hint, "redis.password") {
				t.Errorf("hint should suggest redis.password, got %q", s.Hint)
			}
		}
	}
	if !foundFail {
		t.Errorf("expected L7-Redis-Ping FAIL for NOAUTH, got: %+v", steps)
	}
}

func TestRedisProber_MalformedResponse(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		// Send garbage data
		conn.Write([]byte{0x00, 0x01, 0x02, 0x03, 0x0d, 0x0a})
	})

	prober := NewRedisProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host: host,
		Port: port,
	})

	// Should get a WARN or non-crash response (not panic)
	foundStep := false
	for _, s := range steps {
		if s.Layer == "L7-Redis-Ping" {
			foundStep = true
		}
	}
	if !foundStep {
		// If we got here without panic, it's fine - just need some kind of step
		if len(steps) < 3 {
			t.Errorf("expected at least 3 steps, got %d: %+v", len(steps), steps)
		}
	}
}

func TestRedisProber_EmptyResponse(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		// Accept TCP then close immediately (no data sent)
	})

	prober := NewRedisProber()
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

func TestRedisProber_ConcurrentProbes(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)
		conn.Read(buf)
		conn.Write([]byte("+PONG\r\n"))
		conn.Read(buf)
		info := "# Server\r\nredis_version:7.0.0\r\n"
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(info), info)))
	})

	prober := NewRedisProber()
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
			Port: 6379,
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

func TestClassifyRedisPingResponse(t *testing.T) {
	tests := []struct {
		name         string
		resp         string
		wantStatus   string
		wantInDetail string
	}{
		{
			name:         "PONG",
			resp:         "+PONG",
			wantStatus:   StatusOK,
			wantInDetail: "PONG",
		},
		{
			name:         "empty",
			resp:         "",
			wantStatus:   StatusFAIL,
			wantInDetail: "Empty",
		},
		{
			name:         "NOAUTH",
			resp:         "-NOAUTH Authentication required.",
			wantStatus:   StatusFAIL,
			wantInDetail: "NOAUTH",
		},
		{
			name:         "error response",
			resp:         "-ERR some error",
			wantStatus:   StatusFAIL,
			wantInDetail: "ERR",
		},
		{
			name:       "unexpected response",
			resp:       ":42",
			wantStatus: StatusWARN,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := classifyRedisPingResponse(tt.resp, 10*time.Millisecond)
			if step.Status != tt.wantStatus {
				t.Errorf("status = %q, want %q", step.Status, tt.wantStatus)
			}
			if tt.wantInDetail != "" && !strings.Contains(step.Detail, tt.wantInDetail) {
				t.Errorf("detail %q does not contain %q", step.Detail, tt.wantInDetail)
			}
		})
	}
}

func TestClassifyRedisAuthResponse(t *testing.T) {
	target := ProbeTarget{
		Host:     "redis.example.com",
		Port:     6379,
		Username: "admin",
		Password: "secret",
	}

	tests := []struct {
		name         string
		resp         string
		wantStatus   string
		wantInDetail string
	}{
		{
			name:       "OK",
			resp:       "+OK",
			wantStatus: StatusOK,
		},
		{
			name:         "empty",
			resp:         "",
			wantStatus:   StatusFAIL,
			wantInDetail: "Empty",
		},
		{
			name:         "WRONGPASS",
			resp:         "-WRONGPASS invalid username-password pair",
			wantStatus:   StatusFAIL,
			wantInDetail: "WRONGPASS",
		},
		{
			name:         "invalid username",
			resp:         "-ERR invalid username",
			wantStatus:   StatusFAIL,
			wantInDetail: "invalid username",
		},
		{
			name:       "no auth needed",
			resp:       "-ERR Client sent AUTH, but no password is set",
			wantStatus: StatusWARN,
		},
		{
			name:       "other error",
			resp:       "-ERR unknown command",
			wantStatus: StatusFAIL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := classifyRedisAuthResponse(tt.resp, target, 10*time.Millisecond)
			if step.Status != tt.wantStatus {
				t.Errorf("status = %q, want %q", step.Status, tt.wantStatus)
			}
			if tt.wantInDetail != "" && !strings.Contains(step.Detail, tt.wantInDetail) {
				t.Errorf("detail %q does not contain %q", step.Detail, tt.wantInDetail)
			}
		})
	}
}

func TestParseRedisInfoVersion(t *testing.T) {
	tests := []struct {
		name string
		info string
		want string
	}{
		{
			name: "standard info",
			info: "# Server\r\nredis_version:7.2.4\r\nredis_mode:standalone\r\n",
			want: "7.2.4",
		},
		{
			name: "no version",
			info: "# Server\r\nredis_mode:standalone\r\n",
			want: "",
		},
		{
			name: "empty",
			info: "",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseRedisInfoVersion(tt.info)
			if got != tt.want {
				t.Errorf("parseRedisInfoVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedisProber_AuthWithUsernamePassword(t *testing.T) {
	host, port := startMockServer(t, func(conn net.Conn) {
		buf := make([]byte, 4096)

		// Read AUTH command - should be AUTH username password (RESP array with 3 elements)
		n, _ := conn.Read(buf)
		authCmd := string(buf[:n])
		if !strings.Contains(authCmd, "AUTH") {
			return
		}
		// Verify it's a 3-element array (AUTH username password)
		if !strings.HasPrefix(authCmd, "*3") {
			conn.Write([]byte("-ERR wrong number of arguments for 'auth' command\r\n"))
			return
		}
		conn.Write([]byte("+OK\r\n"))

		// Read PING
		conn.Read(buf)
		conn.Write([]byte("+PONG\r\n"))

		// Read INFO
		conn.Read(buf)
		info := "# Server\r\nredis_version:6.2.0\r\n"
		conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(info), info)))
	})

	prober := NewRedisProber()
	steps := prober.Probe(context.Background(), ProbeTarget{
		Host:     host,
		Port:     port,
		Username: "myuser",
		Password: "mypass",
	})

	foundAuth := false
	for _, s := range steps {
		if s.Layer == "L7-Redis-Auth" && s.Status == StatusOK {
			foundAuth = true
			if !strings.Contains(s.Detail, "myuser") {
				t.Errorf("auth detail should mention username, got %q", s.Detail)
			}
		}
	}
	if !foundAuth {
		t.Errorf("expected L7-Redis-Auth OK step, got: %+v", steps)
	}
}
