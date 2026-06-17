package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// netTimeoutErr implements net.Error with Timeout() == true.
type netTimeoutErr struct{}

func (netTimeoutErr) Error() string   { return "simulated net timeout" }
func (netTimeoutErr) Timeout() bool   { return true }
func (netTimeoutErr) Temporary() bool { return true }

// Verify it satisfies net.Error at compile time.
var _ net.Error = netTimeoutErr{}

func TestPolicyHint(t *testing.T) {
	tests := []struct {
		name     string
		op       string
		err      error
		contains string
	}{
		{
			name:     "nil error",
			op:       "Produce",
			err:      nil,
			contains: "Produce OK",
		},
		{
			name:     "timeout error",
			op:       "Consume",
			err:      context.DeadlineExceeded,
			contains: "timeout",
		},
		{
			name:     "TOPIC_AUTHORIZATION_FAILED string",
			op:       "Produce",
			err:      errors.New("TOPIC_AUTHORIZATION_FAILED"),
			contains: "missing topic ACL",
		},
		{
			name:     "GROUP_AUTHORIZATION_FAILED string",
			op:       "Consume",
			err:      errors.New("GROUP_AUTHORIZATION_FAILED"),
			contains: "missing group ACL",
		},
		{
			name:     "SASL_AUTHENTICATION_FAILED string",
			op:       "Produce",
			err:      errors.New("SASL_AUTHENTICATION_FAILED"),
			contains: "SASL auth failure",
		},
		{
			name:     "LEADER_NOT_AVAILABLE string",
			op:       "Produce",
			err:      errors.New("LEADER_NOT_AVAILABLE"),
			contains: "leader/coord not available",
		},
		{
			name:     "kafka TopicAuthorizationFailed error code",
			op:       "Produce",
			err:      kafka.TopicAuthorizationFailed,
			contains: "missing topic ACL",
		},
		{
			name:     "kafka GroupAuthorizationFailed error code",
			op:       "Consume",
			err:      kafka.GroupAuthorizationFailed,
			contains: "missing group ACL",
		},
		{
			name:     "kafka SASLAuthenticationFailed error code",
			op:       "Produce",
			err:      kafka.SASLAuthenticationFailed,
			contains: "SASL auth failure",
		},
		{
			name:     "generic error falls through",
			op:       "Produce",
			err:      errors.New("something unexpected"),
			contains: "something unexpected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := policyHint(tt.op, tt.err)
			if !strings.Contains(got, tt.contains) {
				t.Errorf("policyHint(%q, %v) = %q, want substring %q", tt.op, tt.err, got, tt.contains)
			}
		})
	}
}

func TestHint(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		contains string
		empty    bool
	}{
		{
			name:  "nil error returns empty",
			err:   nil,
			empty: true,
		},
		{
			name:     "authorization error",
			err:      errors.New("authorization failed"),
			contains: "ACL",
		},
		{
			name:     "SASL error without auth substring",
			err:      errors.New("SASL mechanism misconfigured"),
			contains: "sasl.mechanism",
		},
		{
			name:     "TLS/EOF error",
			err:      errors.New("EOF during handshake"),
			contains: "TLS mismatch",
		},
		{
			name:     "certificate error without auth substring",
			err:      errors.New("tls: certificate verify failed"),
			contains: "TLS mismatch",
		},
		{
			name:     "timeout error",
			err:      context.DeadlineExceeded,
			contains: "Client timeout",
		},
		{
			name:     "kafka TopicAuthorizationFailed error code",
			err:      kafka.TopicAuthorizationFailed,
			contains: "Missing topic ACL",
		},
		{
			name:     "kafka GroupAuthorizationFailed error code",
			err:      kafka.GroupAuthorizationFailed,
			contains: "Missing group ACL",
		},
		{
			name:  "generic error returns empty",
			err:   errors.New("some random error"),
			empty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hint(tt.err)
			if tt.empty {
				if got != "" {
					t.Errorf("hint(%v) = %q, want empty", tt.err, got)
				}
				return
			}
			if !strings.Contains(got, tt.contains) {
				t.Errorf("hint(%v) = %q, want substring %q", tt.err, got, tt.contains)
			}
		})
	}
}

func TestContainsAny(t *testing.T) {
	tests := []struct {
		name string
		s    string
		subs []string
		want bool
	}{
		{
			name: "match first substring",
			s:    "TOPIC_AUTHORIZATION_FAILED",
			subs: []string{"TOPIC_AUTHORIZATION", "GROUP_AUTHORIZATION"},
			want: true,
		},
		{
			name: "match last substring",
			s:    "GROUP_AUTHORIZATION_FAILED",
			subs: []string{"TOPIC_AUTHORIZATION", "GROUP_AUTHORIZATION"},
			want: true,
		},
		{
			name: "no match",
			s:    "something else",
			subs: []string{"TOPIC_AUTHORIZATION", "GROUP_AUTHORIZATION"},
			want: false,
		},
		{
			name: "case insensitive match",
			s:    "topic_authorization_failed",
			subs: []string{"TOPIC_AUTHORIZATION"},
			want: true,
		},
		{
			name: "empty subs",
			s:    "anything",
			subs: []string{},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := containsAny(tt.s, tt.subs...)
			if got != tt.want {
				t.Errorf("containsAny(%q, %v) = %v, want %v", tt.s, tt.subs, got, tt.want)
			}
		})
	}
}

func TestIsTimeout(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "context.DeadlineExceeded",
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "net timeout error",
			err:  netTimeoutErr{},
			want: true,
		},
		{
			name: "i/o timeout string",
			err:  errors.New("read tcp 10.0.0.1:9092: i/o timeout"),
			want: true,
		},
		{
			name: "deadline exceeded string",
			err:  errors.New("context deadline exceeded"),
			want: true,
		},
		{
			name: "non-timeout error",
			err:  errors.New("connection refused"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTimeout(tt.err)
			if got != tt.want {
				t.Errorf("isTimeout(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestParseStartOffset(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{name: "earliest", input: "earliest", want: kafka.FirstOffset},
		{name: "latest", input: "latest", want: kafka.LastOffset},
		{name: "first", input: "first", want: kafka.FirstOffset},
		{name: "last", input: "last", want: kafka.LastOffset},
		{name: "uppercase EARLIEST", input: "EARLIEST", want: kafka.FirstOffset},
		{name: "with whitespace", input: "  earliest  ", want: kafka.FirstOffset},
		{name: "invalid value", input: "invalid", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseStartOffset(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseStartOffset(%q) error = %v, wantErr = %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parseStartOffset(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestSelectBalancer(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantType string
	}{
		{name: "rr", input: "rr", wantType: "*kafka.RoundRobin"},
		{name: "roundrobin", input: "roundrobin", wantType: "*kafka.RoundRobin"},
		{name: "random", input: "random", wantType: "kafka.BalancerFunc"},
		{name: "default falls to LeastBytes", input: "least", wantType: "*kafka.LeastBytes"},
		{name: "empty falls to LeastBytes", input: "", wantType: "*kafka.LeastBytes"},
		{name: "unknown falls to LeastBytes", input: "unknown", wantType: "*kafka.LeastBytes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := selectBalancer(tt.input)
			if got == nil {
				t.Fatal("selectBalancer() returned nil")
			}
			gotType := fmt.Sprintf("%T", got)
			if gotType != tt.wantType {
				t.Errorf("selectBalancer(%q) type = %s, want %s", tt.input, gotType, tt.wantType)
			}
		})
	}
}

func TestSelectBalancer_RandomActuallyBalances(t *testing.T) {
	b := selectBalancer("random")
	msg := kafka.Message{Key: []byte("test")}
	partitions := []int{0, 1, 2, 3, 4}

	// Call many times and verify at least two different partitions are picked.
	seen := map[int]bool{}
	for i := 0; i < 100; i++ {
		p := b.Balance(msg, partitions...)
		seen[p] = true
	}
	if len(seen) < 2 {
		t.Errorf("random balancer returned only %d distinct partitions in 100 calls, expected variety", len(seen))
	}
}

// Silence logf calls that happen inside policyHint/hint via addRow paths.
func init() {
	// scanLog is nil by default, logf is a no-op. No setup needed.
	_ = time.Second // keep time import used
}

func TestFirstHost(t *testing.T) {
	tests := []struct {
		name      string
		bootstrap string
		want      string
	}{
		{
			name:      "host with port",
			bootstrap: "broker:9092",
			want:      "broker",
		},
		{
			name:      "multiple brokers returns first host",
			bootstrap: "broker1:9092,broker2:9092",
			want:      "broker1",
		},
		{
			name:      "host only no port",
			bootstrap: "host-only",
			want:      "host-only",
		},
		{
			name:      "FQDN with port",
			bootstrap: "kafka.example.com:9092",
			want:      "kafka.example.com",
		},
		{
			name:      "whitespace around first broker",
			bootstrap: "  broker1:9092 , broker2:9092 ",
			want:      "broker1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := firstHost(tt.bootstrap)
			if got != tt.want {
				t.Errorf("firstHost(%q) = %q, want %q", tt.bootstrap, got, tt.want)
			}
		})
	}
}

func TestLoggingBalancer_Balance_NilBase(t *testing.T) {
	b := &loggingBalancer{
		base:    nil, // should default to LeastBytes
		topic:   "test-topic",
		leaders: nil,
	}
	msg := kafka.Message{Key: []byte("test-key")}
	partitions := []int{0, 1, 2}

	p := b.Balance(msg, partitions...)
	// Just verify it returns a valid partition index
	found := false
	for _, pp := range partitions {
		if p == pp {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Balance() returned %d, which is not in partitions %v", p, partitions)
	}
}

func TestLoggingBalancer_Balance_WithRoundRobin(t *testing.T) {
	b := &loggingBalancer{
		base:    &kafka.RoundRobin{},
		topic:   "test-topic",
		leaders: nil,
	}
	msg := kafka.Message{Key: []byte("test-key")}
	partitions := []int{0, 1, 2}

	p := b.Balance(msg, partitions...)
	found := false
	for _, pp := range partitions {
		if p == pp {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Balance() returned %d, not in %v", p, partitions)
	}
}

func TestLoggingBalancer_Balance_WithLeaders(t *testing.T) {
	b := &loggingBalancer{
		base:  &kafka.LeastBytes{},
		topic: "test-topic",
		leaders: map[int]kafka.Broker{
			0: {Host: "broker1", Port: 9092, ID: 1},
			1: {Host: "broker2", Port: 9092, ID: 2},
		},
	}
	msg := kafka.Message{Key: []byte("test-key")}
	partitions := []int{0, 1}

	p := b.Balance(msg, partitions...)
	found := false
	for _, pp := range partitions {
		if p == pp {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Balance() returned %d, not in %v", p, partitions)
	}
}
