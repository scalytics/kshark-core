package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scalytics/kshark-core/internal/connectapi"
)

// ---------- runConnectorProbe tests ----------

func TestRunConnectorProbe_AllEmpty(t *testing.T) {
	r := &Report{}
	runConnectorProbe(context.Background(), r, "", "", "", connectapi.ConnectAuthOpts{})
	if len(r.Rows) != 0 {
		t.Errorf("expected 0 rows for empty config, got %d", len(r.Rows))
	}
}

func TestRunConnectorProbe_LocalConfig_MongoDB(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "connector.json")

	// Flat map[string]string format as expected by LoadConnectorConfigFile
	cfg := map[string]string{
		"name":            "mongo-sink",
		"connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
		"connection.uri":  "mongodb://user:pass@mongo.example.com:27017/testdb",
	}
	data, _ := json.Marshal(cfg)
	if err := os.WriteFile(cfgPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	r := &Report{}
	runConnectorProbe(context.Background(), r, "", "", cfgPath, connectapi.ConnectAuthOpts{})

	if len(r.Rows) == 0 {
		t.Fatal("expected at least 1 row from connector probe")
	}

	// Verify component naming includes connector type
	hasConnectorRow := false
	for _, row := range r.Rows {
		if strings.HasPrefix(row.Component, "connector-") {
			hasConnectorRow = true
			break
		}
	}
	if !hasConnectorRow {
		t.Error("expected at least one row with component starting with 'connector-'")
		for _, row := range r.Rows {
			t.Logf("  row: component=%s target=%s layer=%s status=%s detail=%q",
				row.Component, row.Target, row.Layer, row.Status, row.Detail)
		}
	}
}

func TestRunConnectorProbe_LocalConfig_UnsupportedClass(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "connector.json")

	cfg := map[string]string{
		"name":            "custom-sink",
		"connector.class": "com.example.CustomConnector",
		"connection.url":  "custom://host:1234",
	}
	data, _ := json.Marshal(cfg)
	os.WriteFile(cfgPath, data, 0644)

	r := &Report{}
	runConnectorProbe(context.Background(), r, "", "", cfgPath, connectapi.ConnectAuthOpts{})

	found := false
	for _, row := range r.Rows {
		if row.Status == WARN && strings.Contains(row.Detail, "not supported for probing") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected WARN about unsupported connector class")
		for _, row := range r.Rows {
			t.Logf("  row: status=%s detail=%q", row.Status, row.Detail)
		}
	}
}

func TestRunConnectorProbe_LocalConfig_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "bad.json")
	os.WriteFile(cfgPath, []byte(`{not valid json`), 0644)

	r := &Report{}
	runConnectorProbe(context.Background(), r, "", "", cfgPath, connectapi.ConnectAuthOpts{})

	found := false
	for _, row := range r.Rows {
		if row.Status == FAIL {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected FAIL row for invalid JSON config")
		for _, row := range r.Rows {
			t.Logf("  row: status=%s detail=%q", row.Status, row.Detail)
		}
	}
}

func TestRunConnectorProbe_LocalConfig_MissingFile(t *testing.T) {
	r := &Report{}
	runConnectorProbe(context.Background(), r, "", "", "/nonexistent/connector.json", connectapi.ConnectAuthOpts{})

	found := false
	for _, row := range r.Rows {
		if row.Status == FAIL {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected FAIL row for missing config file")
	}
}

func TestRunConnectorProbe_ConnectAPI_Fallback(t *testing.T) {
	// Connect API server returns 500
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte(`{"error":"internal error"}`))
	}))
	defer server.Close()

	// Local config as fallback (flat format)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "connector.json")
	cfg := map[string]string{
		"name":                "pg-sink",
		"connector.class":     "io.confluent.connect.jdbc.JdbcSinkConnector",
		"connection.url":      "jdbc:postgresql://pg.example.com:5432/mydb",
		"connection.user":     "user",
		"connection.password": "pass",
	}
	data, _ := json.Marshal(cfg)
	os.WriteFile(cfgPath, data, 0644)

	r := &Report{}
	runConnectorProbe(context.Background(), r, server.URL, "pg-sink", cfgPath, connectapi.ConnectAuthOpts{})

	// Should have rows (either warn about fallback or probe results)
	if len(r.Rows) == 0 {
		t.Error("expected at least 1 row when Connect API fails with fallback")
	}
}

func TestRunConnectorProbe_ConnectAPI_NoFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer server.Close()

	r := &Report{}
	runConnectorProbe(context.Background(), r, server.URL, "my-connector", "", connectapi.ConnectAuthOpts{})

	found := false
	for _, row := range r.Rows {
		if row.Status == FAIL {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected FAIL row when Connect API fails without fallback")
	}
}

func TestRunConnectorProbe_PasswordRedaction(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "connector.json")

	cfg := map[string]string{
		"name":                "pg-sink",
		"connector.class":     "io.confluent.connect.jdbc.JdbcSinkConnector",
		"connection.url":      "jdbc:postgresql://pg.example.com:5432/mydb",
		"connection.user":     "admin",
		"connection.password": "super-secret-password",
	}
	data, _ := json.Marshal(cfg)
	os.WriteFile(cfgPath, data, 0644)

	r := &Report{ConfigEcho: make(map[string]string)}
	runConnectorProbe(context.Background(), r, "", "", cfgPath, connectapi.ConnectAuthOpts{})

	// Verify password not in plain text in ConfigEcho
	for k, v := range r.ConfigEcho {
		if strings.Contains(strings.ToLower(k), "password") && v == "super-secret-password" {
			t.Errorf("password not redacted in ConfigEcho: %s=%s", k, v)
		}
	}

	// Verify no row detail contains the password
	for _, row := range r.Rows {
		if strings.Contains(row.Detail, "super-secret-password") {
			t.Errorf("password leaked in row detail: %s", row.Detail)
		}
	}
}

func TestRunConnectorProbe_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "connector.json")
	cfg := map[string]string{
		"name":            "mongo-sink",
		"connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
		"connection.uri":  "mongodb://user:pass@mongo.example.com:27017/testdb",
	}
	data, _ := json.Marshal(cfg)
	os.WriteFile(cfgPath, data, 0644)

	r := &Report{}
	// Should not hang on cancelled context
	runConnectorProbe(ctx, r, "", "", cfgPath, connectapi.ConnectAuthOpts{})
}

func TestRunConnectorProbe_PostgreSQL(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "connector.json")

	cfg := map[string]string{
		"name":                "pg-sink",
		"connector.class":     "io.confluent.connect.jdbc.JdbcSinkConnector",
		"connection.url":      "jdbc:postgresql://pg.example.com:5432/mydb",
		"connection.user":     "user",
		"connection.password": "pass",
	}
	data, _ := json.Marshal(cfg)
	os.WriteFile(cfgPath, data, 0644)

	r := &Report{}
	runConnectorProbe(context.Background(), r, "", "", cfgPath, connectapi.ConnectAuthOpts{})

	// Should have attempted probe — at least DNS resolution for pg.example.com
	if len(r.Rows) == 0 {
		t.Log("No rows from PostgreSQL connector probe")
	}
	for _, row := range r.Rows {
		t.Logf("  row: component=%s status=%s detail=%q", row.Component, row.Status, row.Detail)
	}
}
