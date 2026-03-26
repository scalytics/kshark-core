package connectapi

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/scalytics/kshark-core/internal/probe"
)

// ConnectorType identifies the type of database connector.
type ConnectorType string

const (
	TypeMongoDB    ConnectorType = "mongodb"
	TypeDB2        ConnectorType = "db2"
	TypePostgreSQL ConnectorType = "postgresql"
	TypeMySQL      ConnectorType = "mysql"
	TypeSQLServer  ConnectorType = "sqlserver"
	TypeOracle         ConnectorType = "oracle"
	TypeRedis          ConnectorType = "redis"
	TypeElasticsearch  ConnectorType = "elasticsearch"
	TypeUnknown        ConnectorType = "unknown"
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
	case TypeMySQL:
		result.Target, err = extractMySQLTarget(cfg)
	case TypeSQLServer:
		result.Target, err = extractSQLServerTarget(cfg)
	case TypeOracle:
		result.Target, err = extractOracleTarget(cfg)
	case TypeRedis:
		result.Target, err = extractRedisTarget(cfg)
	case TypeElasticsearch:
		result.Target, err = extractElasticsearchTarget(cfg)
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
		case strings.HasPrefix(connURL, "jdbc:mysql://"):
			return TypeMySQL
		case strings.HasPrefix(connURL, "jdbc:sqlserver://"):
			return TypeSQLServer
		case strings.HasPrefix(connURL, "jdbc:oracle:"):
			return TypeOracle
		}
		return TypeUnknown

	case strings.Contains(class, "Redis"),
		strings.HasSuffix(class, "RedisSinkConnector"),
		strings.HasSuffix(class, "RedisSourceConnector"):
		return TypeRedis

	case strings.Contains(class, "Elasticsearch"),
		strings.HasSuffix(class, "ElasticsearchSinkConnector"):
		return TypeElasticsearch

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

func extractMySQLTarget(cfg map[string]string) (probe.ProbeTarget, error) {
	connURL := cfg["connection.url"]
	if connURL == "" {
		return probe.ProbeTarget{}, fmt.Errorf("missing connection.url in JDBC connector config")
	}

	parsed, err := ParseJDBCURL(connURL)
	if err != nil {
		return probe.ProbeTarget{}, err
	}

	tls := strings.EqualFold(parsed.Props["useSSL"], "true") ||
		strings.EqualFold(parsed.Props["requireSSL"], "true") ||
		strings.EqualFold(parsed.Props["sslMode"], "REQUIRED") ||
		strings.EqualFold(parsed.Props["sslMode"], "VERIFY_CA") ||
		strings.EqualFold(parsed.Props["sslMode"], "VERIFY_IDENTITY")

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

func extractSQLServerTarget(cfg map[string]string) (probe.ProbeTarget, error) {
	connURL := cfg["connection.url"]
	if connURL == "" {
		return probe.ProbeTarget{}, fmt.Errorf("missing connection.url in JDBC connector config")
	}

	parsed, err := ParseJDBCURL(connURL)
	if err != nil {
		return probe.ProbeTarget{}, err
	}

	tls := strings.EqualFold(parsed.Props["encrypt"], "true") ||
		strings.EqualFold(parsed.Props["encrypt"], "strict")

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

func extractOracleTarget(cfg map[string]string) (probe.ProbeTarget, error) {
	connURL := cfg["connection.url"]
	if connURL == "" {
		return probe.ProbeTarget{}, fmt.Errorf("missing connection.url in JDBC connector config")
	}

	parsed, err := ParseJDBCURL(connURL)
	if err != nil {
		return probe.ProbeTarget{}, err
	}

	// Oracle uses TCPS protocol or javax.net.ssl properties for TLS
	tls := strings.EqualFold(parsed.Props["oracle.net.ssl_server_dn_match"], "true") ||
		strings.Contains(strings.ToUpper(firstNonEmpty(parsed.Props["connectDescriptor"])), "PROTOCOL=TCPS")

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

func extractRedisTarget(cfg map[string]string) (probe.ProbeTarget, error) {
	uri := firstNonEmpty(cfg["redis.uri"], cfg["redis.hosts"], cfg["connection.uri"])
	if uri == "" {
		return probe.ProbeTarget{}, fmt.Errorf("missing redis.uri, redis.hosts, or connection.uri in Redis connector config")
	}

	host := ""
	port := 6379
	username := ""
	password := ""
	tlsRequired := false

	// Parse redis:// or rediss:// URI
	switch {
	case strings.HasPrefix(uri, "redis://") || strings.HasPrefix(uri, "rediss://"):
		if strings.HasPrefix(uri, "rediss://") {
			tlsRequired = true
		}
		u, err := url.Parse(uri)
		if err != nil {
			return probe.ProbeTarget{}, fmt.Errorf("invalid Redis URI: %w", err)
		}
		host = u.Hostname()
		if p := u.Port(); p != "" {
			parsed, err := strconv.Atoi(p)
			if err != nil {
				return probe.ProbeTarget{}, fmt.Errorf("invalid port in Redis URI: %s", p)
			}
			port = parsed
		}
		if u.User != nil {
			username = u.User.Username()
			if pw, ok := u.User.Password(); ok {
				password = pw
			}
		}
	default:
		// Treat as host:port or host,host:port (take first host)
		hosts := strings.Split(uri, ",")
		first := strings.TrimSpace(hosts[0])
		if colonIdx := strings.LastIndex(first, ":"); colonIdx >= 0 {
			host = first[:colonIdx]
			p, err := strconv.Atoi(first[colonIdx+1:])
			if err != nil {
				host = first
			} else {
				port = p
			}
		} else {
			host = first
		}
	}

	// Config fields override URI values
	if pw := firstNonEmpty(cfg["redis.password"], cfg["connection.password"]); pw != "" {
		password = pw
	}
	if usr := firstNonEmpty(cfg["redis.username"], cfg["connection.user"]); usr != "" {
		username = usr
	}

	// Check for TLS config properties
	if strings.EqualFold(cfg["redis.ssl"], "true") || strings.EqualFold(cfg["ssl.enabled"], "true") {
		tlsRequired = true
	}

	return probe.ProbeTarget{
		Host:     host,
		Port:     port,
		TLS:      tlsRequired,
		Username: username,
		Password: password,
	}, nil
}

func extractElasticsearchTarget(cfg map[string]string) (probe.ProbeTarget, error) {
	uri := firstNonEmpty(cfg["connection.url"], cfg["elasticsearch.url"], cfg["es.url"])
	if uri == "" {
		return probe.ProbeTarget{}, fmt.Errorf("missing connection.url, elasticsearch.url, or es.url in Elasticsearch connector config")
	}

	u, err := url.Parse(uri)
	if err != nil {
		return probe.ProbeTarget{}, fmt.Errorf("invalid Elasticsearch URL: %w", err)
	}

	host := u.Hostname()
	port := 9200
	if p := u.Port(); p != "" {
		parsed, err := strconv.Atoi(p)
		if err != nil {
			return probe.ProbeTarget{}, fmt.Errorf("invalid port in Elasticsearch URL: %s", p)
		}
		port = parsed
	}

	tlsRequired := strings.EqualFold(u.Scheme, "https")

	username := ""
	password := ""
	if u.User != nil {
		username = u.User.Username()
		if pw, ok := u.User.Password(); ok {
			password = pw
		}
	}

	// Config fields override URL values
	if usr := firstNonEmpty(cfg["connection.user"], cfg["elasticsearch.username"]); usr != "" {
		username = usr
	}
	if pw := firstNonEmpty(cfg["connection.password"], cfg["elasticsearch.password"]); pw != "" {
		password = pw
	}

	return probe.ProbeTarget{
		Host:     host,
		Port:     port,
		TLS:      tlsRequired,
		Username: username,
		Password: password,
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
