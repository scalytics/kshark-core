package probe

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	testPostgresImage    = "postgres:17-alpine"
	testMongoImage       = "mongo:8.0"
	testDBUser           = "kshark"
	testDBPassword       = "kshark-test-password"
	testContainerStartup = 120 * time.Second
)

func requireDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("container tests skipped in -short mode")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found, skipping container tests")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon not reachable, skipping container tests")
	}
}

func startContainer(t *testing.T, image string, containerPort int, env map[string]string, cmd ...string) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	args := []string{"run", "-d", "--rm", "-p", fmt.Sprintf("127.0.0.1:%d:%d", port, containerPort)}
	for k, v := range env {
		args = append(args, "-e", k+"="+v)
	}
	args = append(args, image)
	args = append(args, cmd...)
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run %s: %v: %s", image, err, out)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", id).Run() })
	return port
}

func waitUntil(t *testing.T, what string, ready func(ctx context.Context) error) {
	t.Helper()
	deadline := time.Now().Add(testContainerStartup)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		lastErr = ready(ctx)
		cancel()
		if lastErr == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("%s not ready: %v", what, lastErr)
}

func stepByLayer(steps []ProbeStep, layer string) (ProbeStep, bool) {
	for _, s := range steps {
		if s.Layer == layer {
			return s, true
		}
	}
	return ProbeStep{}, false
}

func requireStep(t *testing.T, steps []ProbeStep, layer, status, detail string) {
	t.Helper()
	s, ok := stepByLayer(steps, layer)
	if !ok || s.Status != status || !strings.Contains(s.Detail, detail) {
		t.Fatalf("want %s %s containing %q, got steps %+v", layer, status, detail, steps)
	}
}

func TestPostgresProber_Container(t *testing.T) {
	requireDocker(t)
	port := startContainer(t, testPostgresImage, 5432, map[string]string{
		"POSTGRES_USER":     testDBUser,
		"POSTGRES_PASSWORD": testDBPassword,
		"POSTGRES_DB":       "orders",
	})
	waitUntil(t, "postgres", func(ctx context.Context) error {
		conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/orders?sslmode=disable", testDBUser, testDBPassword, port))
		if err != nil {
			return err
		}
		return conn.Close(ctx)
	})

	p := NewPostgresProber()
	if p.Type() != "postgresql" {
		t.Errorf("Type = %q", p.Type())
	}
	base := ProbeTarget{Host: "127.0.0.1", Port: port, Username: testDBUser, Password: testDBPassword, Database: "orders", SSLMode: "disable"}

	t.Run("connects without tls", func(t *testing.T) {
		steps := p.Probe(context.Background(), base)
		requireStep(t, steps, "L3-DNS", StatusOK, "Resolved to 127.0.0.1")
		requireStep(t, steps, "L4-TCP", StatusOK, "Connected to")
		requireStep(t, steps, "L5-6-TLS", StatusSKIP, "TLS disabled (sslmode=disable)")
		requireStep(t, steps, "L7-Auth", StatusOK, "Authenticated as 'kshark'")
		requireStep(t, steps, "L7-Ready", StatusOK, "Connected to database 'orders'")
	})
	t.Run("wrong password", func(t *testing.T) {
		target := base
		target.Password = "wrong-password"
		steps := p.Probe(context.Background(), target)
		requireStep(t, steps, "L7-Auth", StatusFAIL, "Authentication failed")
		if s, _ := stepByLayer(steps, "L7-Auth"); strings.Contains(s.Detail, "wrong-password") {
			t.Errorf("detail leaks the password: %q", s.Detail)
		}
	})
	t.Run("missing database", func(t *testing.T) {
		target := base
		target.Database = "nope"
		steps := p.Probe(context.Background(), target)
		requireStep(t, steps, "L7-Startup", StatusFAIL, "Database not found")
	})
	t.Run("tls required but not offered", func(t *testing.T) {
		target := base
		target.SSLMode = "require"
		steps := p.Probe(context.Background(), target)
		requireStep(t, steps, "L5-6-TLS", StatusFAIL, "TLS error")
	})
	t.Run("port closed", func(t *testing.T) {
		target := base
		target.Port = 1
		steps := p.Probe(context.Background(), target)
		requireStep(t, steps, "L4-TCP", StatusFAIL, "connection refused")
		if len(steps) != 2 {
			t.Errorf("probe must stop after the tcp failure: %+v", steps)
		}
	})
	t.Run("unresolvable host", func(t *testing.T) {
		target := base
		target.Host = "pg.invalid"
		steps := p.Probe(context.Background(), target)
		requireStep(t, steps, "L3-DNS", StatusFAIL, "DNS lookup failed")
		if len(steps) != 1 {
			t.Errorf("probe must stop after the dns failure: %+v", steps)
		}
	})
}

func TestMongoProber_Container(t *testing.T) {
	requireDocker(t)
	port := startContainer(t, testMongoImage, 27017, map[string]string{
		"MONGO_INITDB_ROOT_USERNAME": testDBUser,
		"MONGO_INITDB_ROOT_PASSWORD": testDBPassword,
	})
	adminURI := fmt.Sprintf("mongodb://%s:%s@127.0.0.1:%d/admin", testDBUser, testDBPassword, port)
	waitUntil(t, "mongo", func(ctx context.Context) error {
		client, err := mongo.Connect(options.Client().ApplyURI(adminURI).SetServerSelectionTimeout(3 * time.Second))
		if err != nil {
			return err
		}
		defer client.Disconnect(context.Background())
		return client.Database("shop").CreateCollection(ctx, "orders")
	})

	p := NewMongoProber()
	if p.Type() != "mongodb" {
		t.Errorf("Type = %q", p.Type())
	}
	authed := ProbeTarget{Host: "127.0.0.1", Port: port, Username: testDBUser, Password: testDBPassword, Database: "shop", Collection: "orders",
		ExtraProps: map[string]string{"connection.uri": fmt.Sprintf("mongodb://%s:%s@127.0.0.1:%d/shop?authSource=admin", testDBUser, testDBPassword, port)}}

	t.Run("authenticated with existing collection", func(t *testing.T) {
		steps := p.Probe(context.Background(), authed)
		requireStep(t, steps, "L3-DNS", StatusOK, "Resolved to 127.0.0.1")
		requireStep(t, steps, "L5-6-TLS", StatusSKIP, "Plain connection (no TLS)")
		requireStep(t, steps, "L7-Auth", StatusOK, "Authentication succeeded")
		requireStep(t, steps, "L7-Ping", StatusOK, "Database 'shop' responded to ping")
		requireStep(t, steps, "L7-Collection", StatusOK, "Collection 'orders' is accessible")
	})
	t.Run("missing collection", func(t *testing.T) {
		target := authed
		target.Collection = "invoices"
		steps := p.Probe(context.Background(), target)
		requireStep(t, steps, "L7-Collection", StatusWARN, "Collection 'invoices' not found")
	})
	t.Run("unauthenticated cannot list collections", func(t *testing.T) {
		target := ProbeTarget{Host: "127.0.0.1", Port: port, Database: "shop", Collection: "orders"}
		steps := p.Probe(context.Background(), target)
		requireStep(t, steps, "L7-Ping", StatusOK, "responded to ping")
		requireStep(t, steps, "L7-Collection", StatusWARN, "Failed to check collection 'orders'")
		if _, ok := stepByLayer(steps, "L7-Auth"); ok {
			t.Errorf("no auth step expected without a username: %+v", steps)
		}
	})
	t.Run("wrong password", func(t *testing.T) {
		target := ProbeTarget{Host: "127.0.0.1", Port: port, Username: testDBUser, Password: "wrong-password"}
		steps := p.Probe(context.Background(), target)
		requireStep(t, steps, "L7-Auth", StatusFAIL, "Authentication failed")
		if s, _ := stepByLayer(steps, "L7-Auth"); strings.Contains(s.Detail, "wrong-password") {
			t.Errorf("detail leaks the password: %q", s.Detail)
		}
	})
	t.Run("port closed", func(t *testing.T) {
		steps := p.Probe(context.Background(), ProbeTarget{Host: "127.0.0.1", Port: 1})
		requireStep(t, steps, "L4-TCP", StatusFAIL, "connection refused")
	})
	t.Run("unresolvable host", func(t *testing.T) {
		steps := p.Probe(context.Background(), ProbeTarget{Host: "mongo.invalid", Port: 27017})
		requireStep(t, steps, "L3-DNS", StatusFAIL, "DNS lookup failed")
	})
	t.Run("invalid uri", func(t *testing.T) {
		target := ProbeTarget{Host: "127.0.0.1", Port: port, ExtraProps: map[string]string{"connection.uri": "mongodb://127.0.0.1:" + fmt.Sprint(port) + "/?connectTimeoutMS=abc"}}
		steps := p.Probe(context.Background(), target)
		last := steps[len(steps)-1]
		if last.Status != StatusFAIL || last.Layer != "L7-Ping" {
			t.Errorf("last step = %+v", last)
		}
	})
}

func TestMongoProber_SRVLookupFails(t *testing.T) {
	steps := NewMongoProber().Probe(context.Background(), ProbeTarget{Host: "cluster0.invalid", SRV: true})
	if len(steps) != 1 || steps[0].Layer != "L3-DNS" || steps[0].Status != StatusFAIL || !strings.Contains(steps[0].Detail, "SRV lookup failed for _mongodb._tcp.cluster0.invalid") {
		t.Errorf("steps = %+v", steps)
	}
}
