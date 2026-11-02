package probe

import (
	"context"
	"fmt"
	"net"
	"testing"
)

const (
	testRedisImage = "redis:7.4-alpine"
	testMySQLImage = "mysql:8.4"
)

func TestRedisProber_Container(t *testing.T) {
	requireDocker(t)
	port := startContainer(t, testRedisImage, 6379, nil, "redis-server", "--requirepass", testDBPassword)
	p := NewRedisProber()
	waitUntil(t, "redis", func(ctx context.Context) error {
		steps := p.Probe(ctx, ProbeTarget{Host: "127.0.0.1", Port: port, Password: testDBPassword})
		if s, ok := stepByLayer(steps, "L7-Redis-Info"); !ok || s.Status != StatusOK {
			return fmt.Errorf("not ready: %+v", steps)
		}
		return nil
	})

	t.Run("password authenticates and reports version", func(t *testing.T) {
		steps := p.Probe(context.Background(), ProbeTarget{Host: "127.0.0.1", Port: port, Password: testDBPassword})
		requireStep(t, steps, "L5-6-TLS", StatusSKIP, "SSL not configured")
		requireStep(t, steps, "L7-Redis-Auth", StatusOK, "Authenticated")
		requireStep(t, steps, "L7-Redis-Ping", StatusOK, "Redis PONG received")
		requireStep(t, steps, "L7-Redis-Info", StatusOK, "Redis server version 7.4")
	})

	t.Run("username and password authenticate", func(t *testing.T) {
		steps := p.Probe(context.Background(), ProbeTarget{Host: "127.0.0.1", Port: port, Username: "default", Password: testDBPassword})
		requireStep(t, steps, "L7-Redis-Auth", StatusOK, "Authenticated as 'default'")
	})

	t.Run("wrong password fails auth", func(t *testing.T) {
		steps := p.Probe(context.Background(), ProbeTarget{Host: "127.0.0.1", Port: port, Password: "wrong-password"})
		requireStep(t, steps, "L7-Redis-Auth", StatusFAIL, "WRONGPASS")
		if s, _ := stepByLayer(steps, "L7-Redis-Auth"); s.Hint != "Wrong password. Check redis.password in connector config." {
			t.Fatalf("unexpected hint %q", s.Hint)
		}
		if _, ok := stepByLayer(steps, "L7-Redis-Ping"); ok {
			t.Fatalf("ping must not run after failed auth: %+v", steps)
		}
	})

	t.Run("unknown user fails auth", func(t *testing.T) {
		steps := p.Probe(context.Background(), ProbeTarget{Host: "127.0.0.1", Port: port, Username: "nobody", Password: "x"})
		requireStep(t, steps, "L7-Redis-Auth", StatusFAIL, "WRONGPASS")
	})

	t.Run("missing password reports NOAUTH on ping", func(t *testing.T) {
		steps := p.Probe(context.Background(), ProbeTarget{Host: "127.0.0.1", Port: port})
		if _, ok := stepByLayer(steps, "L7-Redis-Auth"); ok {
			t.Fatalf("auth must not run without a password: %+v", steps)
		}
		requireStep(t, steps, "L7-Redis-Ping", StatusFAIL, "Redis requires authentication: -NOAUTH")
	})

	t.Run("closed port fails at TCP", func(t *testing.T) {
		steps := p.Probe(context.Background(), ProbeTarget{Host: "127.0.0.1", Port: closedPort(t), Password: testDBPassword})
		requireStep(t, steps, "L4-TCP", StatusFAIL, "")
	})
}

func TestMySQLProber_Container(t *testing.T) {
	requireDocker(t)
	env := map[string]string{
		"MYSQL_ROOT_PASSWORD": testDBPassword,
		"MYSQL_DATABASE":      "shop",
		"MYSQL_USER":          testDBUser,
		"MYSQL_PASSWORD":      testDBPassword,
	}
	port := startContainer(t, testMySQLImage, 3306, env, "--mysql-native-password=ON", "--authentication-policy=mysql_native_password")
	p := NewMySQLProber()
	good := ProbeTarget{Host: "127.0.0.1", Port: port, Username: testDBUser, Password: testDBPassword, Database: "shop"}
	waitUntil(t, "mysql", func(ctx context.Context) error {
		steps := p.Probe(ctx, good)
		if s, ok := stepByLayer(steps, "L7-Ready"); !ok || s.Status != StatusOK {
			return fmt.Errorf("not ready: %+v", steps)
		}
		return nil
	})

	t.Run("native password authenticates", func(t *testing.T) {
		steps := p.Probe(context.Background(), good)
		requireStep(t, steps, "L5-6-TLS", StatusSKIP, "SSL not configured")
		requireStep(t, steps, "L7-Handshake", StatusOK, "MySQL server 8.4")
		requireStep(t, steps, "L7-Auth", StatusOK, "Authenticated as 'kshark'")
		requireStep(t, steps, "L7-Ready", StatusOK, "server ready")
	})

	t.Run("wrong password is access denied", func(t *testing.T) {
		tg := good
		tg.Password = "wrong-password"
		steps := p.Probe(context.Background(), tg)
		requireStep(t, steps, "L7-Auth", StatusFAIL, "error 1045")
		if s, _ := stepByLayer(steps, "L7-Auth"); s.Hint != "Access denied. Verify username and password. Check GRANT privileges." {
			t.Fatalf("unexpected hint %q", s.Hint)
		}
		if _, ok := stepByLayer(steps, "L7-Ready"); ok {
			t.Fatalf("ready must not be reported after failed auth: %+v", steps)
		}
	})

	t.Run("database without grant is denied", func(t *testing.T) {
		tg := good
		tg.Database = "nope"
		steps := p.Probe(context.Background(), tg)
		requireStep(t, steps, "L7-Auth", StatusFAIL, "error 1044")
	})

	t.Run("no username stops after handshake", func(t *testing.T) {
		steps := p.Probe(context.Background(), ProbeTarget{Host: "127.0.0.1", Port: port})
		requireStep(t, steps, "L7-Handshake", StatusOK, "protocol v10")
		if _, ok := stepByLayer(steps, "L7-Auth"); ok {
			t.Fatalf("auth must not run without a username: %+v", steps)
		}
	})

	t.Run("TLS against self-signed server cert fails verification", func(t *testing.T) {
		tg := good
		tg.TLS = true
		steps := p.Probe(context.Background(), tg)
		requireStep(t, steps, "L5-6-TLS", StatusFAIL, "TLS handshake failed")
		if _, ok := stepByLayer(steps, "L7-Auth"); ok {
			t.Fatalf("auth must not run after failed TLS: %+v", steps)
		}
	})

	t.Run("closed port fails at TCP", func(t *testing.T) {
		tg := good
		tg.Port = closedPort(t)
		steps := p.Probe(context.Background(), tg)
		requireStep(t, steps, "L4-TCP", StatusFAIL, "")
	})
}

func closedPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}
