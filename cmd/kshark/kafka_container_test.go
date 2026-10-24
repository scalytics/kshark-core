package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	testKafkaImage    = "apache/kafka:4.3.2"
	testKafkaTopic    = "kshark-it"
	testSASLUser      = "kshark"
	testSASLPassword  = "kshark-test-password"
	testKafkaStartup  = 120 * time.Second
	testKafkaDeadPort = "127.0.0.1:1"
	testKafkaBadHost  = "kshark-broker.invalid:9097"
)

type testKafka struct {
	plain     string
	plainIP   string
	sasl      string
	discovery string
	badDNS    string
}

var (
	kafkaOnce      sync.Once
	kafkaInstance  *testKafka
	kafkaErr       error
	kafkaContainer string
)

func TestMain(m *testing.M) {
	code := m.Run()
	if kafkaContainer != "" {
		_ = exec.Command("docker", "rm", "-f", kafkaContainer).Run()
	}
	os.Exit(code)
}

func requireKafka(t *testing.T) *testKafka {
	t.Helper()
	if testing.Short() {
		t.Skip("Kafka container tests skipped in -short mode")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found, skipping Kafka container tests")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon not reachable, skipping Kafka container tests")
	}
	kafkaOnce.Do(func() {
		kafkaInstance, kafkaErr = startKafka()
	})
	if kafkaErr != nil {
		t.Fatalf("start Kafka container: %v", kafkaErr)
	}
	return kafkaInstance
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func startKafka() (*testKafka, error) {
	var ports [4]int
	for i := range ports {
		p, err := freePort()
		if err != nil {
			return nil, err
		}
		ports[i] = p
	}
	plainPort, saslPort, discPort, dnsPort := ports[0], ports[1], ports[2], ports[3]
	env := map[string]string{
		"KAFKA_NODE_ID":                                   "1",
		"KAFKA_PROCESS_ROLES":                             "broker,controller",
		"KAFKA_CONTROLLER_QUORUM_VOTERS":                  "1@localhost:9093",
		"KAFKA_CONTROLLER_LISTENER_NAMES":                 "CONTROLLER",
		"KAFKA_INTER_BROKER_LISTENER_NAME":                "BROKER",
		"KAFKA_LISTENERS":                                 "PLAINTEXT://:9092,CONTROLLER://:9093,SASL://:9094,DISCOVERY://:9095,BROKER://:9096,BADDNS://:9097",
		"KAFKA_ADVERTISED_LISTENERS":                      fmt.Sprintf("PLAINTEXT://localhost:%d,SASL://127.0.0.1:%d,DISCOVERY://%s,BROKER://localhost:9096,BADDNS://%s", plainPort, saslPort, testKafkaDeadPort, testKafkaBadHost),
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":            "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,SASL:SASL_PLAINTEXT,DISCOVERY:PLAINTEXT,BROKER:PLAINTEXT,BADDNS:PLAINTEXT",
		"KAFKA_SASL_ENABLED_MECHANISMS":                   "PLAIN",
		"KAFKA_LISTENER_NAME_SASL_PLAIN_SASL_JAAS_CONFIG": fmt.Sprintf("org.apache.kafka.common.security.plain.PlainLoginModule required user_%s=%q;", testSASLUser, testSASLPassword),
		"KAFKA_AUTHORIZER_CLASS_NAME":                     "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
		"KAFKA_SUPER_USERS":                               "User:ANONYMOUS",
		"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":          "1",
		"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR":  "1",
		"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":             "1",
		"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":          "0",
		"KAFKA_NUM_PARTITIONS":                            "1",
	}
	args := []string{"run", "-d", "--rm",
		"-p", fmt.Sprintf("127.0.0.1:%d:9092", plainPort),
		"-p", fmt.Sprintf("127.0.0.1:%d:9094", saslPort),
		"-p", fmt.Sprintf("127.0.0.1:%d:9095", discPort),
		"-p", fmt.Sprintf("127.0.0.1:%d:9097", dnsPort),
	}
	for k, v := range env {
		args = append(args, "-e", k+"="+v)
	}
	args = append(args, testKafkaImage)
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("docker run: %v: %s", err, out)
	}
	kafkaContainer = strings.TrimSpace(string(out))

	tk := &testKafka{
		plain:     fmt.Sprintf("localhost:%d", plainPort),
		plainIP:   fmt.Sprintf("127.0.0.1:%d", plainPort),
		sasl:      fmt.Sprintf("127.0.0.1:%d", saslPort),
		discovery: fmt.Sprintf("localhost:%d", discPort),
		badDNS:    fmt.Sprintf("127.0.0.1:%d", dnsPort),
	}
	if err := waitForTopic(tk.plain, testKafkaTopic); err != nil {
		logs, _ := exec.Command("docker", "logs", "--tail", "50", kafkaContainer).CombinedOutput()
		return nil, fmt.Errorf("%v\n%s", err, logs)
	}
	return tk, nil
}

func waitForTopic(addr, topic string) error {
	deadline := time.Now().Add(testKafkaStartup)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = createTopic(addr, topic)
		if lastErr == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("Kafka not ready after %s: %w", testKafkaStartup, lastErr)
}

func createTopic(addr, topic string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil && err != kafka.TopicAlreadyExists {
		return err
	}
	parts, err := conn.ReadPartitions(topic)
	if err != nil {
		return err
	}
	for _, p := range parts {
		if p.Leader.Host == "" {
			return fmt.Errorf("topic %s has no leader yet", topic)
		}
	}
	return nil
}

func plainProps(bootstrap string) map[string]string {
	return map[string]string{"bootstrap.servers": bootstrap, "security.protocol": "PLAINTEXT"}
}

func saslProps(bootstrap, password string) map[string]string {
	return map[string]string{
		"bootstrap.servers": bootstrap,
		"security.protocol": "SASL_PLAINTEXT",
		"sasl.mechanism":    "PLAIN",
		"sasl.username":     testSASLUser,
		"sasl.password":     password,
	}
}
