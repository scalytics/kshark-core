package connectapi

import (
	"strings"
	"testing"
)

func TestRedactConnectorConfig(t *testing.T) {
	cfg := map[string]string{
		"connector.class":     "com.mongodb.kafka.connect.MongoSinkConnector",
		"connection.uri":      "mongodb+srv://user:pass@host/db",
		"connection.password": "secret123",
		"database":            "analytics",
		"api.key":             "mykey",
		"api.secret":          "mysecret",
	}

	redacted := RedactConnectorConfig(cfg)

	if redacted["connector.class"] != "com.mongodb.kafka.connect.MongoSinkConnector" {
		t.Error("non-sensitive field should not be redacted")
	}
	if redacted["database"] != "analytics" {
		t.Error("database should not be redacted")
	}
	if redacted["connection.password"] != "[REDACTED]" {
		t.Errorf("password = %q, want [REDACTED]", redacted["connection.password"])
	}
	if redacted["api.secret"] != "[REDACTED]" {
		t.Errorf("api.secret = %q, want [REDACTED]", redacted["api.secret"])
	}
	if redacted["api.key"] != "[REDACTED]" {
		t.Errorf("api.key = %q, want [REDACTED]", redacted["api.key"])
	}

	// URI should have userinfo redacted
	uri := redacted["connection.uri"]
	if uri == "mongodb+srv://user:pass@host/db" {
		t.Error("connection.uri should be redacted")
	}
	if uri == "" {
		t.Error("connection.uri should not be empty")
	}
}

func TestRedactMongoURI(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantNot string // should NOT contain this
	}{
		{
			name:    "SRV with credentials",
			input:   "mongodb+srv://myuser:mypass@cluster0.abc.mongodb.net/db",
			wantNot: "mypass",
		},
		{
			name:    "standard with credentials",
			input:   "mongodb://user:password@host:27017/db",
			wantNot: "password",
		},
		{
			name:    "no credentials",
			input:   "mongodb://host:27017/db",
			wantNot: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RedactMongoURI(tt.input)
			if tt.wantNot != "" {
				if contains(result, tt.wantNot) {
					t.Errorf("redacted URI still contains %q: %s", tt.wantNot, result)
				}
			}
		})
	}
}

func contains(s, sub string) bool {
	return len(sub) > 0 && len(s) >= len(sub) && (s == sub || len(s) > 0 && containsInner(s, sub))
}

func containsInner(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestRedactMongoURI_Detailed(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantSub    string // substring that must appear in output
		wantAbsent string // substring that must not appear in output
	}{
		{
			name:       "standard mongodb with creds redacted",
			input:      "mongodb://user:pass@host:27017/db",
			wantSub:    "@host:27017/db",
			wantAbsent: "pass",
		},
		{
			name:    "URI without credentials unchanged host",
			input:   "mongodb://host:27017/db",
			wantSub: "host:27017/db",
		},
		{
			name:       "mongodb+srv with creds redacted",
			input:      "mongodb+srv://admin:s3cret@cluster0.mongodb.net/mydb",
			wantSub:    "@cluster0.mongodb.net/mydb",
			wantAbsent: "s3cret",
		},
		{
			name:    "empty string returns empty",
			input:   "",
			wantSub: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RedactMongoURI(tt.input)
			if tt.wantSub != "" {
				if !strings.Contains(result, tt.wantSub) {
					t.Errorf("result %q does not contain %q", result, tt.wantSub)
				}
			}
			if tt.wantAbsent != "" {
				if strings.Contains(result, tt.wantAbsent) {
					t.Errorf("result %q still contains %q", result, tt.wantAbsent)
				}
			}
			if tt.input == "" && result != "" {
				t.Errorf("empty input should return empty, got %q", result)
			}
		})
	}
}

func TestRedactMongoURI_ContainsRedactedMarker(t *testing.T) {
	result := RedactMongoURI("mongodb://user:pass@host:27017/db")
	if !strings.Contains(result, "REDACTED") {
		t.Errorf("result %q should contain REDACTED marker", result)
	}
}

func TestRedactConnectorConfig_TokenField(t *testing.T) {
	cfg := map[string]string{
		"bearer.token": "secret-token-value",
		"normal.field": "visible",
	}
	redacted := RedactConnectorConfig(cfg)
	if redacted["bearer.token"] != "[REDACTED]" {
		t.Errorf("token field = %q, want [REDACTED]", redacted["bearer.token"])
	}
	if redacted["normal.field"] != "visible" {
		t.Errorf("normal field should not be redacted")
	}
}
