package connectapi

import (
	"testing"
)

func TestRedactConnectorConfig(t *testing.T) {
	cfg := map[string]string{
		"connector.class":    "com.mongodb.kafka.connect.MongoSinkConnector",
		"connection.uri":     "mongodb+srv://user:pass@host/db",
		"connection.password": "secret123",
		"database":           "analytics",
		"api.key":            "mykey",
		"api.secret":         "mysecret",
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
