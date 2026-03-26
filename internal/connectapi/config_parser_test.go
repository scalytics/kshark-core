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
			name: "JDBC with MySQL URL",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
				"connection.url":  "jdbc:mysql://host/db",
			},
			want: TypeMySQL,
		},
		{
			name: "JDBC with unsupported URL",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
				"connection.url":  "jdbc:h2://mem:test",
			},
			want: TypeUnknown,
		},
		{
			name: "Redis sink",
			cfg:  map[string]string{"connector.class": "com.redis.kafka.connect.RedisSinkConnector"},
			want: TypeRedis,
		},
		{
			name: "Elasticsearch sink",
			cfg:  map[string]string{"connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector"},
			want: TypeElasticsearch,
		},
		{
			name: "JDBC with SQLServer URL",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
				"connection.url":  "jdbc:sqlserver://host:1433;databaseName=mydb",
			},
			want: TypeSQLServer,
		},
		{
			name: "JDBC with Oracle URL",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
				"connection.url":  "jdbc:oracle:thin:@host:1521/ORCL",
			},
			want: TypeOracle,
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

func TestExtractMySQLTarget(t *testing.T) {
	cfg := map[string]string{
		"connector.class":    "io.confluent.connect.jdbc.JdbcSourceConnector",
		"connection.url":     "jdbc:mysql://myhost:3306/testdb?useSSL=true",
		"connection.user":    "myuser",
		"connection.password": "mypass",
	}

	parsed, err := ParseConnectorConfig("test-mysql", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.Type != TypeMySQL {
		t.Errorf("type = %q, want mysql", parsed.Type)
	}
	if parsed.Target.Host != "myhost" {
		t.Errorf("host = %q, want myhost", parsed.Target.Host)
	}
	if parsed.Target.Port != 3306 {
		t.Errorf("port = %d, want 3306", parsed.Target.Port)
	}
	if parsed.Target.Database != "testdb" {
		t.Errorf("database = %q, want testdb", parsed.Target.Database)
	}
	if !parsed.Target.TLS {
		t.Error("expected TLS=true for useSSL=true")
	}
	if parsed.Target.Username != "myuser" {
		t.Errorf("username = %q, want myuser", parsed.Target.Username)
	}
	if parsed.Target.Password != "mypass" {
		t.Errorf("password = %q, want mypass", parsed.Target.Password)
	}
}

func TestExtractSQLServerTarget(t *testing.T) {
	cfg := map[string]string{
		"connector.class":    "io.confluent.connect.jdbc.JdbcSourceConnector",
		"connection.url":     "jdbc:sqlserver://sqlhost:1433;databaseName=mydb;encrypt=true",
		"connection.user":    "sqluser",
		"connection.password": "sqlpass",
	}

	parsed, err := ParseConnectorConfig("test-sqlserver", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.Type != TypeSQLServer {
		t.Errorf("type = %q, want sqlserver", parsed.Type)
	}
	if parsed.Target.Host != "sqlhost" {
		t.Errorf("host = %q, want sqlhost", parsed.Target.Host)
	}
	if parsed.Target.Port != 1433 {
		t.Errorf("port = %d, want 1433", parsed.Target.Port)
	}
	if parsed.Target.Database != "mydb" {
		t.Errorf("database = %q, want mydb", parsed.Target.Database)
	}
	if !parsed.Target.TLS {
		t.Error("expected TLS=true for encrypt=true")
	}
	if parsed.Target.Username != "sqluser" {
		t.Errorf("username = %q, want sqluser", parsed.Target.Username)
	}
	if parsed.Target.Password != "sqlpass" {
		t.Errorf("password = %q, want sqlpass", parsed.Target.Password)
	}
}

func TestExtractOracleTarget(t *testing.T) {
	cfg := map[string]string{
		"connector.class":    "io.confluent.connect.jdbc.JdbcSourceConnector",
		"connection.url":     "jdbc:oracle:thin:@orahost:1521/ORCL",
		"connection.user":    "orauser",
		"connection.password": "orapass",
	}

	parsed, err := ParseConnectorConfig("test-oracle", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.Type != TypeOracle {
		t.Errorf("type = %q, want oracle", parsed.Type)
	}
	if parsed.Target.Host != "orahost" {
		t.Errorf("host = %q, want orahost", parsed.Target.Host)
	}
	if parsed.Target.Port != 1521 {
		t.Errorf("port = %d, want 1521", parsed.Target.Port)
	}
	if parsed.Target.Database != "ORCL" {
		t.Errorf("database = %q, want ORCL", parsed.Target.Database)
	}
	if parsed.Target.Username != "orauser" {
		t.Errorf("username = %q, want orauser", parsed.Target.Username)
	}
	if parsed.Target.Password != "orapass" {
		t.Errorf("password = %q, want orapass", parsed.Target.Password)
	}
}

func TestExtractRedisTarget(t *testing.T) {
	tests := []struct {
		name         string
		cfg          map[string]string
		wantHost     string
		wantPort     int
		wantTLS      bool
		wantPassword string
		wantErr      bool
	}{
		{
			name: "redis:// URI",
			cfg: map[string]string{
				"connector.class": "com.redis.kafka.connect.RedisSinkConnector",
				"redis.uri":       "redis://redishost:6379",
			},
			wantHost: "redishost",
			wantPort: 6379,
			wantTLS:  false,
		},
		{
			name: "rediss:// URI with TLS",
			cfg: map[string]string{
				"connector.class": "com.redis.kafka.connect.RedisSinkConnector",
				"redis.uri":       "rediss://secure:6380",
			},
			wantHost: "secure",
			wantPort: 6380,
			wantTLS:  true,
		},
		{
			name: "redis.hosts with password",
			cfg: map[string]string{
				"connector.class": "com.redis.kafka.connect.RedisSinkConnector",
				"redis.hosts":     "host1:6379",
				"redis.password":  "secret",
			},
			wantHost:     "host1",
			wantPort:     6379,
			wantPassword: "secret",
			wantTLS:      false,
		},
		{
			name: "missing connection info",
			cfg: map[string]string{
				"connector.class": "com.redis.kafka.connect.RedisSinkConnector",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParseConnectorConfig("test-redis", tt.cfg)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if parsed.Type != TypeRedis {
				t.Errorf("type = %q, want redis", parsed.Type)
			}
			if parsed.Target.Host != tt.wantHost {
				t.Errorf("host = %q, want %q", parsed.Target.Host, tt.wantHost)
			}
			if parsed.Target.Port != tt.wantPort {
				t.Errorf("port = %d, want %d", parsed.Target.Port, tt.wantPort)
			}
			if parsed.Target.TLS != tt.wantTLS {
				t.Errorf("TLS = %v, want %v", parsed.Target.TLS, tt.wantTLS)
			}
			if tt.wantPassword != "" && parsed.Target.Password != tt.wantPassword {
				t.Errorf("password = %q, want %q", parsed.Target.Password, tt.wantPassword)
			}
		})
	}
}

func TestExtractElasticsearchTarget(t *testing.T) {
	tests := []struct {
		name         string
		cfg          map[string]string
		wantHost     string
		wantPort     int
		wantTLS      bool
		wantUsername  string
		wantPassword string
		wantErr      bool
	}{
		{
			name: "HTTPS connection.url",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
				"connection.url":  "https://eshost:9200",
			},
			wantHost: "eshost",
			wantPort: 9200,
			wantTLS:  true,
		},
		{
			name: "HTTP elasticsearch.url",
			cfg: map[string]string{
				"connector.class":  "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
				"elasticsearch.url": "http://eshost:9200",
			},
			wantHost: "eshost",
			wantPort: 9200,
			wantTLS:  false,
		},
		{
			name: "with connection.user and password",
			cfg: map[string]string{
				"connector.class":    "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
				"connection.url":     "https://eshost:9200",
				"connection.user":    "esuser",
				"connection.password": "espass",
			},
			wantHost:     "eshost",
			wantPort:     9200,
			wantTLS:      true,
			wantUsername:  "esuser",
			wantPassword: "espass",
		},
		{
			name: "URL with userinfo",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
				"connection.url":  "https://user:pass@eshost:9200",
			},
			wantHost:     "eshost",
			wantPort:     9200,
			wantTLS:      true,
			wantUsername:  "user",
			wantPassword: "pass",
		},
		{
			name: "missing URL",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParseConnectorConfig("test-es", tt.cfg)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if parsed.Type != TypeElasticsearch {
				t.Errorf("type = %q, want elasticsearch", parsed.Type)
			}
			if parsed.Target.Host != tt.wantHost {
				t.Errorf("host = %q, want %q", parsed.Target.Host, tt.wantHost)
			}
			if parsed.Target.Port != tt.wantPort {
				t.Errorf("port = %d, want %d", parsed.Target.Port, tt.wantPort)
			}
			if parsed.Target.TLS != tt.wantTLS {
				t.Errorf("TLS = %v, want %v", parsed.Target.TLS, tt.wantTLS)
			}
			if tt.wantUsername != "" && parsed.Target.Username != tt.wantUsername {
				t.Errorf("username = %q, want %q", parsed.Target.Username, tt.wantUsername)
			}
			if tt.wantPassword != "" && parsed.Target.Password != tt.wantPassword {
				t.Errorf("password = %q, want %q", parsed.Target.Password, tt.wantPassword)
			}
		})
	}
}

func TestParseConnectorConfig_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		cfg      map[string]string
		wantType ConnectorType
	}{
		{
			name: "MongoDB",
			cfg: map[string]string{
				"connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
				"connection.uri":  "mongodb://host:27017/db",
			},
			wantType: TypeMongoDB,
		},
		{
			name: "DB2",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
				"connection.url":  "jdbc:db2://host:50000/MYDB",
			},
			wantType: TypeDB2,
		},
		{
			name: "PostgreSQL",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
				"connection.url":  "jdbc:postgresql://host:5432/mydb",
			},
			wantType: TypePostgreSQL,
		},
		{
			name: "MySQL",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
				"connection.url":  "jdbc:mysql://host:3306/mydb",
			},
			wantType: TypeMySQL,
		},
		{
			name: "SQLServer",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
				"connection.url":  "jdbc:sqlserver://host:1433;databaseName=mydb",
			},
			wantType: TypeSQLServer,
		},
		{
			name: "Oracle",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
				"connection.url":  "jdbc:oracle:thin:@host:1521/ORCL",
			},
			wantType: TypeOracle,
		},
		{
			name: "Redis",
			cfg: map[string]string{
				"connector.class": "com.redis.kafka.connect.RedisSinkConnector",
				"redis.uri":       "redis://host:6379",
			},
			wantType: TypeRedis,
		},
		{
			name: "Elasticsearch",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
				"connection.url":  "https://host:9200",
			},
			wantType: TypeElasticsearch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParseConnectorConfig("test-"+tt.name, tt.cfg)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if parsed.Type != tt.wantType {
				t.Errorf("type = %q, want %q", parsed.Type, tt.wantType)
			}
			if parsed.Target.Host == "" {
				t.Error("expected non-empty target host")
			}
		})
	}
}
