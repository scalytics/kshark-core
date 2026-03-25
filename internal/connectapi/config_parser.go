package connectapi

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/your-username/kshark/internal/probe"
)

// ConnectorType identifies the type of database connector.
type ConnectorType string

const (
	TypeMongoDB    ConnectorType = "mongodb"
	TypeDB2        ConnectorType = "db2"
	TypePostgreSQL ConnectorType = "postgresql"
	TypeUnknown    ConnectorType = "unknown"
)

// ParsedConnector holds the result of parsing a connector configuration.
type ParsedConnector struct {
	Name      string
	Class     string
	Type      ConnectorType
	Target    probe.ProbeTarget
	RawConfig map[string]string
}

// ParseConnectorConfig inspects the connector config and extracts probe target parameters.
func ParseConnectorConfig(name string, cfg map[string]string) (*ParsedConnector, error) {
	class := cfg["connector.class"]
	connType := detectConnectorType(cfg)

	result := &ParsedConnector{
		Name:      name,
		Class:     class,
		Type:      connType,
		RawConfig: RedactConnectorConfig(cfg),
	}

	var err error
	switch connType {
	case TypeMongoDB:
		result.Target, err = extractMongoTarget(cfg)
	case TypeDB2:
		result.Target, err = extractDB2Target(cfg)
	case TypePostgreSQL:
		result.Target, err = extractPostgresTarget(cfg)
	case TypeUnknown:
		// No extraction needed, will be reported as unsupported
		return result, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to extract %s target: %w", connType, err)
	}

	return result, nil
}

func detectConnectorType(cfg map[string]string) ConnectorType {
	class := cfg["connector.class"]

	switch {
	case strings.HasSuffix(class, "MongoSourceConnector"),
		strings.HasSuffix(class, "MongoSinkConnector"),
		strings.Contains(class, "Mongo"):
		return TypeMongoDB

	case strings.HasSuffix(class, "JdbcSourceConnector"),
		strings.HasSuffix(class, "JdbcSinkConnector"),
		strings.Contains(class, "Jdbc"):
		connURL := cfg["connection.url"]
		switch {
		case strings.HasPrefix(connURL, "jdbc:db2://"):
			return TypeDB2
		case strings.HasPrefix(connURL, "jdbc:postgresql://"):
			return TypePostgreSQL
		}
		return TypeUnknown

	default:
		return TypeUnknown
	}
}

func extractMongoTarget(cfg map[string]string) (probe.ProbeTarget, error) {
	uri := firstNonEmpty(cfg["connection.uri"], cfg["mongodb.connection.uri"])
	if uri == "" {
		return probe.ProbeTarget{}, fmt.Errorf("missing connection.uri in MongoDB connector config")
	}

	host, port, username, password, database, isSRV, tlsRequired, err := probe.ParseMongoURI(uri)
	if err != nil {
		return probe.ProbeTarget{}, err
	}

	// Config fields override URI values
	if db := firstNonEmpty(cfg["database"], cfg["mongodb.database"]); db != "" {
		database = db
	}
	collection := firstNonEmpty(cfg["collection"], cfg["mongodb.collection"])

	return probe.ProbeTarget{
		Host:        host,
		Port:        port,
		TLS:         tlsRequired,
		Username:    username,
		Password:    password,
		Database:    database,
		Collection:  collection,
		SRV:         isSRV,
		OriginalURI: RedactMongoURI(uri),
		ExtraProps: map[string]string{
			"connection.uri": uri, // raw URI for driver (not redacted)
		},
	}, nil
}

func extractDB2Target(cfg map[string]string) (probe.ProbeTarget, error) {
	connURL := cfg["connection.url"]
	if connURL == "" {
		return probe.ProbeTarget{}, fmt.Errorf("missing connection.url in JDBC connector config")
	}

	parsed, err := ParseJDBCURL(connURL)
	if err != nil {
		return probe.ProbeTarget{}, err
	}

	tls := strings.EqualFold(parsed.Props["sslConnection"], "true")

	extraProps := make(map[string]string)
	for k, v := range parsed.Props {
		extraProps[k] = v
	}

	return probe.ProbeTarget{
		Host:       parsed.Host,
		Port:       parsed.Port,
		TLS:        tls,
		Username:   cfg["connection.user"],
		Password:   cfg["connection.password"],
		Database:   parsed.Database,
		ExtraProps: extraProps,
	}, nil
}

func extractPostgresTarget(cfg map[string]string) (probe.ProbeTarget, error) {
	connURL := cfg["connection.url"]
	if connURL == "" {
		return probe.ProbeTarget{}, fmt.Errorf("missing connection.url in JDBC connector config")
	}

	parsed, err := ParseJDBCURL(connURL)
	if err != nil {
		return probe.ProbeTarget{}, err
	}

	sslmode := parsed.Props["sslmode"]
	if sslmode == "" {
		sslmode = "prefer"
	}

	tls := sslmode != "disable"

	extraProps := make(map[string]string)
	for k, v := range parsed.Props {
		extraProps[k] = v
	}

	return probe.ProbeTarget{
		Host:       parsed.Host,
		Port:       parsed.Port,
		TLS:        tls,
		Username:   cfg["connection.user"],
		Password:   cfg["connection.password"],
		Database:   parsed.Database,
		SSLMode:    sslmode,
		ExtraProps: extraProps,
	}, nil
}

// LoadConnectorConfigFile reads a connector configuration from a local JSON file.
func LoadConnectorConfigFile(path string) (map[string]string, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("cannot read connector config file: %w", err)
	}

	var cfg map[string]string
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, "", fmt.Errorf("invalid JSON in connector config file %s: %w", path, err)
	}

	name := cfg["name"]
	if name == "" {
		name = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}

	return cfg, name, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
