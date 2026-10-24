package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func rowsOf(r *Report) []Row {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Row(nil), r.Rows...)
}

func findRow(r *Report, target string, layer Layer) (Row, bool) {
	for _, row := range rowsOf(r) {
		if row.Target == target && row.Layer == layer {
			return row, true
		}
	}
	return Row{}, false
}

func requireRow(t *testing.T, r *Report, target string, layer Layer, status CheckStatus, detailPrefix string) Row {
	t.Helper()
	for _, row := range rowsOf(r) {
		if row.Target == target && row.Layer == layer && row.Status == status && strings.HasPrefix(row.Detail, detailPrefix) {
			return row
		}
	}
	t.Fatalf("no row target=%q layer=%s status=%s detail^=%q in %+v", target, layer, status, detailPrefix, rowsOf(r))
	return Row{}
}

func TestKafka_CheckTopic(t *testing.T) {
	tk := requireKafka(t)
	tests := []struct {
		name   string
		props  map[string]string
		addr   string
		topic  string
		status CheckStatus
		detail string
	}{
		{"visible topic", plainProps(tk.plain), tk.plain, testKafkaTopic, OK, "Topic visible; leader partitions=1"},
		{"missing topic", plainProps(tk.plain), tk.plain, "kshark-no-such-topic", FAIL, "Topic not found / not authorized"},
		{"sasl user without describe acl", saslProps(tk.sasl, testSASLPassword), tk.sasl, testKafkaTopic, FAIL, "Topic not found / not authorized"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Report{}
			checkTopic(r, tt.props, tt.addr, tt.topic, 5*time.Second)
			requireRow(t, r, tt.addr, L7, OK, "ApiVersions OK")
			requireRow(t, r, tt.topic, L7, tt.status, tt.detail)
		})
	}
}

func TestKafka_KafkaConn_SASLWrongPassword(t *testing.T) {
	tk := requireKafka(t)
	r := &Report{}
	conn, err := kafkaConn(r, saslProps(tk.sasl, "wrong-password"), tk.sasl, 5*time.Second)
	if err == nil {
		conn.Close()
		t.Fatal("expected SASL authentication error")
	}
	row := requireRow(t, r, tk.sasl, L7, FAIL, "broker dial failed:")
	if !strings.Contains(row.Detail, "SASL Authentication Failed") {
		t.Errorf("detail = %q, want SASL Authentication Failed", row.Detail)
	}
	if row.Hint != "Auth/TLS mismatch or listener not exposed." {
		t.Errorf("hint = %q", row.Hint)
	}
}

func TestKafka_KafkaConn_SASLGoodPassword(t *testing.T) {
	tk := requireKafka(t)
	r := &Report{}
	conn, err := kafkaConn(r, saslProps(tk.sasl, testSASLPassword), tk.sasl, 0)
	if err != nil {
		t.Fatalf("kafkaConn: %v", err)
	}
	conn.Close()
	requireRow(t, r, tk.sasl, L7, OK, "ApiVersions OK")
}

func TestKafka_ProduceConsume(t *testing.T) {
	tk := requireKafka(t)
	tests := []struct {
		name     string
		group    string
		balancer string
	}{
		{"direct partition read", "", "least"},
		{"consumer group read", "kshark-it-group", "rr"},
		{"random balancer", "", "random"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Report{}
			probeProduceConsume(context.Background(), r, plainProps(tk.plain), tk.plain, testKafkaTopic, tt.group,
				5*time.Second, 15*time.Second, tt.balancer, 5*time.Second, -2)
			requireRow(t, r, testKafkaTopic, L7, OK, "Produce OK")
			requireRow(t, r, testKafkaTopic, L7, OK, "Consume OK (offset ")
			if r.HasFailed {
				t.Errorf("unexpected failure rows: %+v", rowsOf(r))
			}
		})
	}
}

func TestKafka_ProduceConsume_DefaultTimeouts(t *testing.T) {
	tk := requireKafka(t)
	r := &Report{}
	probeProduceConsume(context.Background(), r, plainProps(tk.plain), tk.plain, testKafkaTopic, "", 0, 0, "", 0, -2)
	requireRow(t, r, testKafkaTopic, L7, OK, "Produce OK")
	requireRow(t, r, testKafkaTopic, L7, OK, "Consume OK (offset ")
}

func TestKafka_ProduceConsume_SASLWrongPassword(t *testing.T) {
	tk := requireKafka(t)
	r := &Report{}
	probeProduceConsume(context.Background(), r, saslProps(tk.sasl, "wrong-password"), tk.sasl, testKafkaTopic, "",
		3*time.Second, 3*time.Second, "least", 3*time.Second, -2)
	row := requireRow(t, r, testKafkaTopic, L7, FAIL, "Produce failed: SASL auth failure")
	if row.Hint != "Verify sasl.mechanism, credentials, clocks (JWT), and listener SASL config." {
		t.Errorf("hint = %q", row.Hint)
	}
	if _, ok := findRow(r, testKafkaTopic, L7); !ok {
		t.Fatal("missing row")
	}
	for _, row := range rowsOf(r) {
		if strings.HasPrefix(row.Detail, "Consume") {
			t.Errorf("consume must not run after a failed produce: %+v", row)
		}
	}
}

func TestKafka_ProduceConsume_SASLUserWithoutACL(t *testing.T) {
	tk := requireKafka(t)
	r := &Report{}
	probeProduceConsume(context.Background(), r, saslProps(tk.sasl, testSASLPassword), tk.sasl, testKafkaTopic, "",
		3*time.Second, 3*time.Second, "least", 3*time.Second, -2)
	requireRow(t, r, testKafkaTopic, L7, FAIL, "Produce failed: ")
}

func TestKafka_TopicLeaders(t *testing.T) {
	tk := requireKafka(t)
	leaders := topicLeaders(plainProps(tk.plain), tk.plain, testKafkaTopic, 0)
	if len(leaders) != 1 {
		t.Fatalf("leaders = %+v, want one partition", leaders)
	}
	if leaders[0].Host != "localhost" || leaders[0].ID != 1 {
		t.Errorf("leader = %+v, want localhost broker 1", leaders[0])
	}
	if got := topicLeaders(plainProps(tk.plain), tk.plain, "kshark-no-such-topic", time.Second); len(got) != 0 {
		t.Errorf("leaders for missing topic = %+v, want none", got)
	}
}

func TestKafka_BrokerDiscovery(t *testing.T) {
	tk := requireKafka(t)
	t.Run("advertised host differs from bootstrap but is reachable", func(t *testing.T) {
		r := &Report{}
		brokerDiscoveryScan(r, plainProps(tk.plainIP), []string{tk.plainIP}, 5*time.Second)
		requireRow(t, r, tk.plain, DIAG, OK, "Discovered broker reachable")
	})
	t.Run("advertised listener refuses tcp", func(t *testing.T) {
		r := &Report{}
		brokerDiscoveryScan(r, plainProps(tk.discovery), []string{tk.discovery}, 5*time.Second)
		row := requireRow(t, r, testKafkaDeadPort, L4, FAIL, "Discovered broker unreachable")
		if !strings.Contains(row.Hint, "advertised.listeners") {
			t.Errorf("hint = %q", row.Hint)
		}
	})
	t.Run("advertised host does not resolve", func(t *testing.T) {
		r := &Report{}
		brokerDiscoveryScan(r, plainProps(tk.badDNS), []string{tk.badDNS}, 5*time.Second)
		requireRow(t, r, testKafkaBadHost, L3, FAIL, "Discovered broker DNS failed")
	})
	t.Run("bootstrap is the advertised host", func(t *testing.T) {
		r := &Report{}
		brokerDiscoveryScan(r, plainProps(tk.plain), []string{tk.plain}, 5*time.Second)
		if rows := rowsOf(r); len(rows) != 0 {
			t.Errorf("rows = %+v, want none", rows)
		}
	})
}

func TestKafka_RunScan_EndToEnd(t *testing.T) {
	tk := requireKafka(t)
	r := &Report{}
	cfg := scanConfig{
		props:          plainProps(tk.plain),
		bootstrap:      tk.plain,
		topics:         []string{testKafkaTopic},
		kafkaTimeout:   5 * time.Second,
		produceTimeout: 5 * time.Second,
		consumeTimeout: 10 * time.Second,
		balancer:       "least",
		startOffset:    -2,
		probeDirection: "up",
	}
	runScan(context.Background(), r, cfg)
	requireRow(t, r, "localhost", L3, OK, "Resolved host")
	requireRow(t, r, tk.plain, L4, OK, "Connected in")
	requireRow(t, r, tk.plain, L56, SKIP, "PLAINTEXT (no TLS)")
	requireRow(t, r, testKafkaTopic, L7, OK, "Topic visible")
	requireRow(t, r, testKafkaTopic, L7, OK, "Produce OK")
	requireRow(t, r, testKafkaTopic, L7, OK, "Consume OK")
	if r.HasFailed {
		t.Errorf("unexpected failures: %+v", rowsOf(r))
	}
}

func TestKafka_RunConnectivityCheck(t *testing.T) {
	tk := requireKafka(t)
	r := &Report{}
	runConnectivityCheck(r, runnerConfig{bootstrap: tk.plainIP, props: plainProps(tk.plainIP), kafkaTimeout: 5 * time.Second})
	requireRow(t, r, tk.plainIP, L7, OK, "ApiVersions OK")
	requireRow(t, r, tk.plain, DIAG, OK, "Discovered broker reachable")

	r = &Report{}
	runConnectivityCheck(r, runnerConfig{bootstrap: tk.sasl, props: saslProps(tk.sasl, "wrong-password"), kafkaTimeout: 5 * time.Second})
	requireRow(t, r, tk.sasl, L7, FAIL, "broker dial failed")
	if len(rowsOf(r)) != 1 {
		t.Errorf("discovery must not run after a failed dial: %+v", rowsOf(r))
	}
}

func TestKafka_RunRoutineCheck_KafkaKinds(t *testing.T) {
	tk := requireKafka(t)
	rc := runnerConfig{bootstrap: tk.plain, props: plainProps(tk.plain), kafkaTimeout: 5 * time.Second, opTimeout: 10 * time.Second, activeTier: TierCore}
	tests := []struct {
		name  string
		check RoutineCheck
		want  BVBCheckStatus
	}{
		{"produce-consume", RoutineCheck{ID: "pc", Kind: KindProduceConsume, Params: map[string]any{"topic": testKafkaTopic}}, CheckPASS},
		{"topic-presence ok", RoutineCheck{ID: "tp", Kind: KindTopicPresence, Params: map[string]any{"topics": []any{testKafkaTopic}}}, CheckPASS},
		{"topic-presence missing", RoutineCheck{ID: "tp2", Kind: KindTopicPresence, Params: map[string]any{"topics": []any{"kshark-no-such-topic"}}}, CheckFAIL},
		{"connectivity", RoutineCheck{ID: "conn", Kind: KindConnectivity}, CheckPASS},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Report{}
			res := runRoutineCheck(context.Background(), r, rc, tt.check)
			if res.Status != tt.want {
				t.Errorf("status = %s, want %s; evidence %v", res.Status, tt.want, res.Evidence)
			}
			if len(res.Rows) == 0 {
				t.Error("expected row back-references")
			}
		})
	}
}

func TestKafka_MainFullScanGreen(t *testing.T) {
	tk := requireKafka(t)
	dir := t.TempDir()
	t.Chdir(dir)
	props := writeTestFile(t, filepath.Join(dir, "client.properties"), "bootstrap.servers="+tk.plain+"\nsecurity.protocol=PLAINTEXT\n")
	res := runMain(t, "", "-props", props, "-topic", testKafkaTopic, "-y", "-diag=false", "-balancer", "rr",
		"-produce-timeout", "10s", "-consume-timeout", "15s", "-json", "reports/green.json", "-bundle", "green.tar.gz")
	if res.exited {
		t.Fatalf("exit = %d, want clean return\nstdout: %s\nstderr: %s", res.code, res.stdout, res.stderr)
	}
	r := readReport(t, filepath.Join(dir, "reports", "green.json"))
	if r.Summary == nil {
		t.Fatal("report has no summary")
	}
	produced, consumed := false, false
	for _, row := range r.Rows {
		if row.Status == FAIL {
			t.Errorf("unexpected failure row: %+v", row)
		}
		produced = produced || strings.HasPrefix(row.Detail, "Produce OK")
		consumed = consumed || strings.HasPrefix(row.Detail, "Consume OK")
	}
	if !produced || !consumed {
		t.Errorf("rows lack produce/consume success: %+v", r.Rows)
	}
	if _, err := os.Stat(filepath.Join(dir, "green.tar.gz")); err != nil {
		t.Errorf("bundle missing: %v", err)
	}
	if !strings.Contains(res.stdout, "Diagnostics bundle created:") {
		t.Errorf("stdout = %s", res.stdout)
	}
}

func writeRoutine(t *testing.T, dir, body string) string {
	t.Helper()
	return writeTestFile(t, filepath.Join(dir, "routine.yaml"), "routine_id: it-routine\nroutine_version: 0.1.0\nblueprint: bp-it\nmanifest_version: \"1.0\"\nbackend:\n  family: kafscale\nchecks:\n"+body)
}

func TestKafka_ValidateBlueprintGreen(t *testing.T) {
	tk := requireKafka(t)
	allowLoopback(t)
	srv := newStatusServer(t, map[string]int{"/healthz": http.StatusOK, "/subjects": http.StatusOK, "/subjects/kshark-it-value/versions": http.StatusOK})
	t.Setenv("KSHARK_IT_COMPONENT_URL", srv.URL)
	dir := t.TempDir()
	copyReportTemplate(t, dir)
	t.Chdir(dir)
	props := writeTestFile(t, filepath.Join(dir, "client.properties"), "bootstrap.servers="+tk.plain+"\nschema.registry.url="+srv.URL+"\n")
	digests := writeTestFile(t, filepath.Join(dir, "digests.json"), `[{"image":"kafscale/proxy","digest":"sha256:abc"}]`)
	routine := writeRoutine(t, dir, `  - id: SCEN-it-01
    kind: connectivity
    claim: "proxy reachable"
  - id: SCEN-it-02
    kind: topic-presence
    claim: "topic exists"
    topics: [`+testKafkaTopic+`]
  - id: SCEN-it-03
    kind: produce-consume
    claim: "round trip"
    params: {topic: `+testKafkaTopic+`}
  - id: SCEN-it-04
    kind: schema-presence
    claim: "schema registered"
    subjects: [kshark-it-value]
  - id: SCEN-it-05
    kind: component-health
    claim: "app healthy"
    components: [app]
    params:
      probes:
        app: {url_env: KSHARK_IT_COMPONENT_URL}
  - id: SCEN-it-06
    kind: http-check
    tier: needs-headroom
    claim: "heavy query"
    http: {url_env: KSHARK_IT_COMPONENT_URL, path: /healthz}
`)
	res := runMain(t, "", "validate-blueprint", "--bp", "bp-override", "--routine", routine, "-props", props,
		"--digests", digests, "--backend", "confluent", "--out", "reports/bvb.json", "--bundle", "bvb.tar.gz", "--kafka-timeout", "5s", "--op-timeout", "15s")
	if res.exited {
		t.Fatalf("exit = %d, want GREEN\nstdout: %s\nstderr: %s", res.code, res.stdout, res.stderr)
	}
	r := readReport(t, filepath.Join(dir, "reports", "bvb.json"))
	if r.BVB == nil {
		t.Fatal("report has no bvb block")
	}
	if r.BVB.Verdict.Status != VerdictGreen || r.BVB.Verdict.Pass != 5 || r.BVB.Verdict.TierExcluded != 1 {
		t.Errorf("verdict = %+v", r.BVB.Verdict)
	}
	if r.BVB.BlueprintID != "bp-override" || r.BVB.Backend.Family != "confluent" || len(r.BVB.ImageDigests) != 1 {
		t.Errorf("bvb identity = %+v", r.BVB)
	}
	for _, want := range []string{"Verdict: GREEN", "HTML report written to", "BVB report written to", "Diagnostics bundle created:"} {
		if !strings.Contains(res.stdout, want) {
			t.Errorf("stdout missing %q", want)
		}
	}
}

func TestKafka_ValidateBlueprintHeadroomTierRuns(t *testing.T) {
	tk := requireKafka(t)
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("SMOKE_TIER", TierNeedsHeadroom)
	props := writeTestFile(t, filepath.Join(dir, "client.properties"), "bootstrap.servers="+tk.plain+"\n")
	routine := writeRoutine(t, dir, `  - id: SCEN-it-01
    kind: topic-presence
    tier: needs-headroom
    claim: "topic exists"
    topics: [kshark-it-missing]
`)
	res := runMain(t, "", "validate-blueprint", "--routine", routine, "-props", props, "--kafka-timeout", "5s")
	if !res.exited || res.code != 1 {
		t.Fatalf("exit = %v/%d, want RED exit 1", res.exited, res.code)
	}
	if !strings.Contains(res.stdout, "Verdict: RED") || !strings.Contains(res.stderr, "HTML render skipped") {
		t.Errorf("stdout = %s\nstderr = %s", res.stdout, res.stderr)
	}
}
