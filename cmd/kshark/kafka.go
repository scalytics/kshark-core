// Copyright 2024-2026 Scalytics GmbH and kshark Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// ---------- Kafka helpers & policy hints ----------

func kafkaConn(r *Report, p map[string]string, brokerAddr string, timeout time.Duration) (*kafka.Conn, error) {
	host, _, _ := net.SplitHostPort(brokerAddr)
	dialer, _, err := dialerFromProps(p, host)
	if err != nil {
		addRow(r, Row{"kafka", brokerAddr, L7, FAIL, fmt.Sprintf("dialer error: %v", err), "Check security.protocol & sasl.* settings."})
		return nil, err
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	dialStart := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", brokerAddr)
	if err != nil {
		slog.Debug("kafka dial", "addr", brokerAddr, "dur", time.Since(dialStart).Truncate(time.Millisecond), "err", err)
		addRow(r, Row{"kafka", brokerAddr, L7, FAIL, fmt.Sprintf("broker dial failed: %v", err), "Auth/TLS mismatch or listener not exposed."})
		return nil, err
	}
	slog.Debug("kafka dial ok", "addr", brokerAddr, "dur", time.Since(dialStart).Truncate(time.Millisecond))
	apiStart := time.Now()
	if _, err := conn.ApiVersions(); err != nil {
		slog.Debug("kafka apiversions", "addr", brokerAddr, "dur", time.Since(apiStart).Truncate(time.Millisecond), "err", err)
		addRow(r, Row{"kafka", brokerAddr, L7, FAIL, fmt.Sprintf("ApiVersions failed: %v", err), "Broker incompatible or proxy interfering."})
		_ = conn.Close()
		return nil, err
	}
	slog.Debug("kafka apiversions ok", "addr", brokerAddr, "dur", time.Since(apiStart).Truncate(time.Millisecond))
	addRow(r, Row{"kafka", brokerAddr, L7, OK, "ApiVersions OK", ""})
	return conn, nil
}

func checkTopic(r *Report, p map[string]string, brokerAddr, topic string, timeout time.Duration) {
	conn, err := kafkaConn(r, p, brokerAddr, timeout)
	if err != nil {
		return
	}
	defer conn.Close()

	partsStart := time.Now()
	parts, err := conn.ReadPartitions()
	if err != nil {
		slog.Debug("kafka readpartitions", "addr", brokerAddr, "dur", time.Since(partsStart).Truncate(time.Millisecond), "err", err)
		addRow(r, Row{"kafka", brokerAddr, L7, FAIL, policyHint("ReadPartitions", err), hint(err)})
		return
	}
	slog.Debug("kafka readpartitions ok", "addr", brokerAddr, "dur", time.Since(partsStart).Truncate(time.Millisecond))
	var found bool
	var leaders int
	for _, pt := range parts {
		if pt.Topic == topic {
			found = true
			if pt.Leader.Host != "" {
				leaders++
			}
		}
	}
	if !found {
		addRow(r, Row{"kafka", topic, L7, FAIL, "Topic not found / not authorized (DescribeTopic).", "Grant Describe on topic or create it."})
		return
	}
	addRow(r, Row{"kafka", topic, L7, OK, fmt.Sprintf("Topic visible; leader partitions=%d", leaders), ""})
}

func probeProduceConsume(ctx context.Context, r *Report, p map[string]string, bootstrap, topic, group string, produceTimeout, consumeTimeout time.Duration, balancer string, kafkaTimeout time.Duration, startOffset int64) {
	if topic == "" {
		addRow(r, Row{"kafka", "(no topic)", L7, SKIP, "Produce/Consume skipped", ""})
		return
	}
	// Empty ServerName so the Dialer auto-derives it from each broker address.
	// A hardcoded bootstrap ServerName causes wrong SNI when the Reader connects
	// to partition leaders or group coordinators on Confluent Cloud, making the
	// load balancer route the connection to the wrong broker.
	dialer, _, err := dialerFromProps(p, "")
	if err != nil {
		addRow(r, Row{"kafka", topic, L7, FAIL, fmt.Sprintf("dialer: %v", err), "Check tls/sasl settings."})
		return
	}
	if produceTimeout <= 0 {
		produceTimeout = 10 * time.Second
	}
	if consumeTimeout <= 0 {
		consumeTimeout = 10 * time.Second
	}
	leaders := topicLeaders(p, bootstrap, topic, kafkaTimeout)
	if len(leaders) == 0 {
		slog.Debug("kafka leaders unavailable", "topic", topic)
	}
	baseBalancer := selectBalancer(balancer)
	// Build Transport with TLS and SASL configured directly.
	// IMPORTANT: Do NOT use dialer.DialContext as Transport.Dial — it returns
	// *kafka.Conn whose Read/Write wrap data in Kafka Fetch/Produce frames,
	// corrupting the Transport's internal protocol communication.
	// Instead, let the Transport handle TLS (with per-broker SNI) and SASL itself.
	transport, err := transportFromProps(p, produceTimeout)
	if err != nil {
		addRow(r, Row{"kafka", topic, L7, FAIL, fmt.Sprintf("transport: %v", err), "Check tls/sasl settings."})
		return
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(bootstrap, ",")...),
		Topic:        topic,
		Balancer:     &loggingBalancer{base: baseBalancer, topic: topic, leaders: leaders},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		Transport:    transport,
	}
	defer w.Close()

	key := fmt.Sprintf("kwire-%d", time.Now().UnixNano())
	msg := kafka.Message{
		Key:     []byte(key),
		Value:   []byte("probe"),
		Headers: []kafka.Header{{Key: "kwirecheck", Value: []byte("1")}},
		Time:    time.Now(),
	}

	writeStart := time.Now()
	// Retry logic for metadata refresh errors (common with Confluent Cloud)
	var writeErr error
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt) * 500 * time.Millisecond
			slog.Debug("kafka produce retry", "topic", topic, "attempt", attempt, "backoff", backoff)
			time.Sleep(backoff)
		}

		writeCtx, writeCancel := context.WithTimeout(ctx, produceTimeout)
		writeErr = w.WriteMessages(writeCtx, msg)
		writeCancel()

		if writeErr == nil {
			slog.Debug("kafka produce ok", "topic", topic, "dur", time.Since(writeStart).Truncate(time.Millisecond))
			addRow(r, Row{"kafka", topic, L7, OK, "Produce OK", ""})
			break
		}

		// Check if error is retryable (metadata-related)
		if errors.Is(writeErr, kafka.NotLeaderForPartition) || errors.Is(writeErr, kafka.LeaderNotAvailable) {
			slog.Debug("kafka produce retrying", "topic", topic, "dur", time.Since(writeStart).Truncate(time.Millisecond), "err", writeErr)
			continue
		}

		// Non-retryable error, break immediately
		slog.Debug("kafka produce not retryable", "topic", topic, "dur", time.Since(writeStart).Truncate(time.Millisecond), "err", writeErr)
		break
	}

	if writeErr != nil {
		slog.Debug("kafka produce failed", "topic", topic, "dur", time.Since(writeStart).Truncate(time.Millisecond), "err", writeErr)
		addRow(r, Row{"kafka", topic, L7, FAIL, policyHint("Produce", writeErr), hint(writeErr)})
		return
	}

	// Use direct partition read (no consumer group) for the consume probe.
	// Consumer group coordination (JoinGroup/SyncGroup/Heartbeat) adds seconds
	// of overhead on Confluent Cloud and is unnecessary for a connectivity check.
	// If a group is explicitly requested, honour it; otherwise probe partition 0.
	if group != "" {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     strings.Split(bootstrap, ","),
			Topic:       topic,
			GroupID:     group,
			StartOffset: startOffset,
			MaxWait:     3 * time.Second,
			Dialer:      dialer,
		})
		defer reader.Close()

		readStart := time.Now()
		readCtx, readCancel := context.WithTimeout(ctx, consumeTimeout)
		defer readCancel()
		rec, err := reader.ReadMessage(readCtx)
		if err != nil {
			slog.Debug("kafka consume", "topic", topic, "group", group, "dur", time.Since(readStart).Truncate(time.Millisecond), "err", err)
			addRow(r, Row{"kafka", topic, L7, FAIL, policyHint("Consume", err), "Grant Read on topic and Group Read/Describe; check prefixes."})
			return
		}
		slog.Debug("kafka consume ok", "topic", topic, "group", group, "dur", time.Since(readStart).Truncate(time.Millisecond))
		addRow(r, Row{"kafka", topic, L7, OK, fmt.Sprintf("Consume OK (offset %d)", rec.Offset), ""})
		return
	}

	// Direct partition fetch — fast, no group coordination needed.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   strings.Split(bootstrap, ","),
		Topic:     topic,
		Partition: 0,
		MaxWait:   3 * time.Second,
		Dialer:    dialer,
	})
	defer reader.Close()
	if err := reader.SetOffset(kafka.FirstOffset); err != nil {
		slog.Debug("kafka consume seek failed", "topic", topic, "err", err)
		addRow(r, Row{"kafka", topic, L7, FAIL, fmt.Sprintf("Consume seek failed: %v", err), "Check topic/partition exists."})
		return
	}

	readStart := time.Now()
	readCtx, readCancel := context.WithTimeout(ctx, consumeTimeout)
	defer readCancel()
	rec, err := reader.ReadMessage(readCtx)
	if err != nil {
		slog.Debug("kafka consume", "topic", topic, "dur", time.Since(readStart).Truncate(time.Millisecond), "err", err)
		addRow(r, Row{"kafka", topic, L7, FAIL, policyHint("Consume", err), "Grant Read on topic and Group Read/Describe; check prefixes."})
		return
	}
	slog.Debug("kafka consume ok", "topic", topic, "dur", time.Since(readStart).Truncate(time.Millisecond))
	addRow(r, Row{"kafka", topic, L7, OK, fmt.Sprintf("Consume OK (offset %d)", rec.Offset), ""})
}

func firstHost(bootstrap string) string {
	first := strings.TrimSpace(strings.Split(bootstrap, ",")[0])
	h, _, err := net.SplitHostPort(first)
	if err != nil {
		return first
	}
	return h
}

// policyHint maps common errors to actionable hints.
func policyHint(op string, err error) string {
	if err == nil {
		return op + " OK"
	}
	if ke, ok := kafkaErrorCode(err); ok {
		switch ke {
		case kafka.TopicAuthorizationFailed:
			return op + " failed: missing topic ACL"
		case kafka.GroupAuthorizationFailed:
			return op + " failed: missing group ACL"
		case kafka.SASLAuthenticationFailed:
			return op + " failed: SASL auth failure"
		case kafka.RequestTimedOut:
			return op + " failed: broker request timeout"
		case kafka.LeaderNotAvailable, kafka.NotLeaderForPartition, kafka.GroupCoordinatorNotAvailable, kafka.NotCoordinatorForGroup:
			return op + " failed: leader/coord not available"
		}
	}
	if isTimeout(err) {
		return op + " failed: timeout (" + err.Error() + ")"
	}
	em := err.Error()
	switch {
	case containsAny(em, "TOPIC_AUTHORIZATION_FAILED", "Topic authorization failed"):
		return op + " failed: missing topic ACL"
	case containsAny(em, "GROUP_AUTHORIZATION_FAILED", "Group authorization failed"):
		return op + " failed: missing group ACL"
	case containsAny(em, "SASL_AUTHENTICATION_FAILED", "SASL"):
		return op + " failed: SASL auth failure"
	case containsAny(em, "LEADER_NOT_AVAILABLE", "NOT_LEADER", "COORDINATOR_NOT_AVAILABLE"):
		return op + " failed: leader/coord not available"
	default:
		return op + " failed: " + em
	}
}
func hint(err error) string {
	if err == nil {
		return ""
	}
	if ke, ok := kafkaErrorCode(err); ok {
		switch ke {
		case kafka.TopicAuthorizationFailed:
			return "Missing topic ACL: Write/Describe for produce; Read/Describe for consume."
		case kafka.GroupAuthorizationFailed:
			return "Missing group ACL: Read/Describe on group."
		case kafka.SASLAuthenticationFailed:
			return "Verify sasl.mechanism, credentials, clocks (JWT), and listener SASL config."
		case kafka.RequestTimedOut:
			return "Broker request timed out; check broker load, network path, and required.acks."
		case kafka.LeaderNotAvailable, kafka.NotLeaderForPartition:
			return "Leader not available; check broker health and metadata propagation."
		case kafka.GroupCoordinatorNotAvailable, kafka.NotCoordinatorForGroup:
			return "Group coordinator not available; check group/offsets topic health."
		}
	}
	if isTimeout(err) {
		return "Client timeout (10s): check network path, firewall, DNS, TLS/SNI, or advertised.listeners."
	}
	em := err.Error()
	switch {
	case containsAny(em, "authorization", "AUTHORIZATION", "AUTH"):
		return "Check ACLs: Write/Read/Describe on topic; Read/Describe on group."
	case containsAny(em, "SASL", "authentication"):
		return "Verify sasl.mechanism, credentials, clocks (JWT), and listener SASL config."
	case containsAny(em, "EOF", "tls", "handshake", "certificate"):
		return "TLS mismatch or mTLS requirements; verify CA and SNI/hostnames."
	default:
		return ""
	}
}
func containsAny(s string, subs ...string) bool {
	ls := strings.ToLower(s)
	for _, sub := range subs {
		if strings.Contains(ls, strings.ToLower(sub)) {
			return true
		}
	}
	return false
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	em := strings.ToLower(err.Error())
	return strings.Contains(em, "deadline exceeded") || strings.Contains(em, "i/o timeout")
}

func kafkaErrorCode(err error) (kafka.Error, bool) {
	var ke kafka.Error
	if errors.As(err, &ke) {
		return ke, true
	}
	return 0, false
}

type loggingBalancer struct {
	base    kafka.Balancer
	topic   string
	leaders map[int]kafka.Broker
}

func (b *loggingBalancer) Balance(msg kafka.Message, partitions ...int) int {
	if b.base == nil {
		b.base = &kafka.LeastBytes{}
	}
	partition := b.base.Balance(msg, partitions...)
	if len(b.leaders) > 0 {
		if leader, ok := b.leaders[partition]; ok {
			slog.Debug("kafka balance", "topic", b.topic, "partition", partition, "leader_host", leader.Host, "leader_port", leader.Port, "broker_id", leader.ID)
			return partition
		}
	}
	slog.Debug("kafka balance", "topic", b.topic, "partition", partition, "leader", "unknown")
	return partition
}

func topicLeaders(p map[string]string, bootstrap, topic string, timeout time.Duration) map[int]kafka.Broker {
	first := strings.TrimSpace(strings.Split(bootstrap, ",")[0])
	hostForSNI := firstHost(first)
	dialer, _, err := dialerFromProps(p, hostForSNI)
	if err != nil {
		slog.Debug("kafka leaders dialer error", "err", err)
		return nil
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", first)
	if err != nil {
		slog.Debug("kafka leaders dial", "addr", first, "dur", time.Since(start).Truncate(time.Millisecond), "err", err)
		return nil
	}
	defer conn.Close()
	slog.Debug("kafka leaders dial ok", "addr", first, "dur", time.Since(start).Truncate(time.Millisecond))

	partsStart := time.Now()
	parts, err := conn.ReadPartitions()
	if err != nil {
		slog.Debug("kafka leaders readpartitions", "dur", time.Since(partsStart).Truncate(time.Millisecond), "err", err)
		return nil
	}
	slog.Debug("kafka leaders readpartitions ok", "dur", time.Since(partsStart).Truncate(time.Millisecond))

	leaders := make(map[int]kafka.Broker)
	for _, pt := range parts {
		if pt.Topic == topic {
			leaders[pt.ID] = pt.Leader
		}
	}
	return leaders
}

func selectBalancer(name string) kafka.Balancer {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "rr", "roundrobin":
		return &kafka.RoundRobin{}
	case "random":
		return kafka.BalancerFunc(func(_ kafka.Message, partitions ...int) int {
			if len(partitions) == 0 {
				return 0
			}
			return partitions[rand.Intn(len(partitions))]
		})
	default:
		return &kafka.LeastBytes{}
	}
}

func parseStartOffset(s string) (int64, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "earliest", "first":
		return kafka.FirstOffset, nil
	case "latest", "last":
		return kafka.LastOffset, nil
	default:
		return kafka.FirstOffset, fmt.Errorf("invalid start-offset: %s", s)
	}
}
