package connectapi

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDetectConnectorType(t *testing.T) {
	tests := []struct {
		name string
		cfg  map[string]string
		want ConnectorType
	}{
		{
			name: "MongoDB source",
			cfg:  map[string]string{"connector.class": "com.mongodb.kafka.connect.MongoSourceConnector"},
			want: TypeMongoDB,
		},
		{
			name: "MongoDB sink",
			cfg:  map[string]string{"connector.class": "com.mongodb.kafka.connect.MongoSinkConnector"},
			want: TypeMongoDB,
		},
		{
			name: "JDBC DB2",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
				"connection.url":  "jdbc:db2://host:50000/MYDB",
			},
			want: TypeDB2,
		},
		{
			name: "JDBC PostgreSQL",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
				"connection.url":  "jdbc:postgresql://host:5432/mydb",
			},
			want: TypePostgreSQL,
		},
		{
			name: "Unknown class",
			cfg:  map[string]string{"connector.class": "com.example.CustomConnector"},
			want: TypeUnknown,
		},
		{
			name: "JDBC with unsupported URL",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
				"connection.url":  "jdbc:mysql://host/db",
			},
			want: TypeUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectConnectorType(tt.cfg)
			if got != tt.want {
				t.Errorf("detectConnectorType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseConnectorConfig_MongoDB(t *testing.T) {
	cfg := map[string]string{
		"connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
		"connection.uri":  "mongodb+srv://user:pass@cluster0.abc123.mongodb.net/analytics",
		"database":        "analytics",
		"collection":      "events",
	}

	parsed, err := ParseConnectorConfig("test-mongo", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.Type != TypeMongoDB {
		t.Errorf("type = %q, want mongodb", parsed.Type)
	}
	if parsed.Target.Host != "cluster0.abc123.mongodb.net" {
		t.Errorf("host = %q, want cluster0.abc123.mongodb.net", parsed.Target.Host)
	}
	if !parsed.Target.SRV {
		t.Error("expected SRV=true")
	}
	if !parsed.Target.TLS {
		t.Error("expected TLS=true for mongodb+srv")
	}
	if parsed.Target.Username != "user" {
		t.Errorf("username = %q, want user", parsed.Target.Username)
	}
	if parsed.Target.Database != "analytics" {
		t.Errorf("database = %q, want analytics", parsed.Target.Database)
	}
	if parsed.Target.Collection != "events" {
		t.Errorf("collection = %q, want events", parsed.Target.Collection)
	}
}

func TestParseConnectorConfig_DB2(t *testing.T) {
	cfg := map[string]string{
		"connector.class":  "io.confluent.connect.jdbc.JdbcSourceConnector",
		"connection.url":   "jdbc:db2://db2host:50000/PRODDB:sslConnection=true;",
		"connection.user":  "db2admin",
		"connection.password": "secret",
	}

	parsed, err := ParseConnectorConfig("test-db2", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.Type != TypeDB2 {
		t.Errorf("type = %q, want db2", parsed.Type)
	}
	if parsed.Target.Host != "db2host" {
		t.Errorf("host = %q, want db2host", parsed.Target.Host)
	}
	if parsed.Target.Port != 50000 {
		t.Errorf("port = %d, want 50000", parsed.Target.Port)
	}
	if !parsed.Target.TLS {
		t.Error("expected TLS=true for sslConnection=true")
	}
	if parsed.Target.Username != "db2admin" {
		t.Errorf("username = %q, want db2admin", parsed.Target.Username)
	}
	if parsed.Target.Database != "PRODDB" {
		t.Errorf("database = %q, want PRODDB", parsed.Target.Database)
	}
}

func TestParseConnectorConfig_PostgreSQL(t *testing.T) {
	cfg := map[string]string{
		"connector.class":  "io.confluent.connect.jdbc.JdbcSourceConnector",
		"connection.url":   "jdbc:postgresql://pghost:5432/appdb?sslmode=require",
		"connection.user":  "pguser",
		"connection.password": "secret",
	}

	parsed, err := ParseConnectorConfig("test-pg", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.Type != TypePostgreSQL {
		t.Errorf("type = %q, want postgresql", parsed.Type)
	}
	if parsed.Target.Host != "pghost" {
		t.Errorf("host = %q, want pghost", parsed.Target.Host)
	}
	if parsed.Target.Port != 5432 {
		t.Errorf("port = %d, want 5432", parsed.Target.Port)
	}
	if parsed.Target.SSLMode != "require" {
		t.Errorf("sslmode = %q, want require", parsed.Target.SSLMode)
	}
	if !parsed.Target.TLS {
		t.Error("expected TLS=true for sslmode=require")
	}
}

func TestParseConnectorConfig_MissingURI(t *testing.T) {
	cfg := map[string]string{
		"connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
	}

	_, err := ParseConnectorConfig("test", cfg)
	if err == nil {
		t.Fatal("expected error for missing connection.uri")
	}
}

func TestLoadConnectorConfigFile(t *testing.T) {
	// Write a temporary JSON file
	dir := t.TempDir()
	path := filepath.Join(dir, "my-connector.json")
	content := `{
		"name": "test-connector",
		"connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
		"connection.uri": "mongodb://host:27017/db"
	}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	cfg, name, err := LoadConnectorConfigFile(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "test-connector" {
		t.Errorf("name = %q, want test-connector", name)
	}
	if cfg["connector.class"] != "com.mongodb.kafka.connect.MongoSinkConnector" {
		t.Error("connector.class not loaded")
	}
}

func TestLoadConnectorConfigFile_NoName(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mongo-sink.json")
	content := `{"connector.class": "com.mongodb.kafka.connect.MongoSinkConnector"}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, name, err := LoadConnectorConfigFile(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if name != "mongo-sink" {
		t.Errorf("name = %q, want mongo-sink (derived from filename)", name)
	}
}

func TestLoadConnectorConfigFile_NotFound(t *testing.T) {
	_, _, err := LoadConnectorConfigFile("/nonexistent/file.json")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadConnectorConfigFile_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte("{not json}"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, _, err := LoadConnectorConfigFile(path)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
