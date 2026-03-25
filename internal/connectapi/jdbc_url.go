package connectapi

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// JDBCParsed holds the components extracted from a JDBC URL.
type JDBCParsed struct {
	Dialect  string            // "db2", "postgresql"
	Host     string            // hostname
	Port     int               // port number
	Database string            // database name
	Props    map[string]string // sslmode, sslConnection, etc.
}

// ParseJDBCURL parses a JDBC URL and returns its components.
func ParseJDBCURL(jdbcURL string) (*JDBCParsed, error) {
	if !strings.HasPrefix(jdbcURL, "jdbc:") {
		return nil, fmt.Errorf("not a JDBC URL: %s", jdbcURL)
	}

	rest := strings.TrimPrefix(jdbcURL, "jdbc:")

	switch {
	case strings.HasPrefix(rest, "db2://"):
		return parseDB2URL(rest)
	case strings.HasPrefix(rest, "postgresql://"):
		return parsePostgresURL(rest)
	default:
		return nil, fmt.Errorf("unsupported JDBC dialect in URL: %s", jdbcURL)
	}
}

func parseDB2URL(urlPart string) (*JDBCParsed, error) {
	// Format: db2://host[:port]/DBNAME[:prop1=val1;prop2=val2;]
	rest := strings.TrimPrefix(urlPart, "db2://")

	parsed := &JDBCParsed{
		Dialect: "db2",
		Port:    50000,
		Props:   make(map[string]string),
	}

	// Split host[:port] from /DBNAME[:props]
	slashIdx := strings.Index(rest, "/")
	if slashIdx < 0 {
		return nil, fmt.Errorf("DB2 JDBC URL missing database name: %s", urlPart)
	}

	hostPort := rest[:slashIdx]
	remainder := rest[slashIdx+1:]

	// Parse host:port
	if colonIdx := strings.LastIndex(hostPort, ":"); colonIdx >= 0 {
		parsed.Host = hostPort[:colonIdx]
		p, err := strconv.Atoi(hostPort[colonIdx+1:])
		if err != nil {
			return nil, fmt.Errorf("invalid port in DB2 URL: %s", hostPort[colonIdx+1:])
		}
		parsed.Port = p
	} else {
		parsed.Host = hostPort
	}

	// Parse DBNAME and properties (separated by : in DB2 URLs)
	// e.g., "PRODDB:sslConnection=true;currentSchema=MYSCHEMA;"
	if colonIdx := strings.Index(remainder, ":"); colonIdx >= 0 {
		parsed.Database = remainder[:colonIdx]
		propsStr := remainder[colonIdx+1:]
		// Parse semicolon-delimited key=value pairs
		for _, pair := range strings.Split(propsStr, ";") {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			if eqIdx := strings.Index(pair, "="); eqIdx >= 0 {
				parsed.Props[pair[:eqIdx]] = pair[eqIdx+1:]
			}
		}
	} else {
		parsed.Database = strings.TrimRight(remainder, ";")
	}

	return parsed, nil
}

func parsePostgresURL(urlPart string) (*JDBCParsed, error) {
	// Format: postgresql://host[:port]/dbname[?params]
	u, err := url.Parse(urlPart)
	if err != nil {
		return nil, fmt.Errorf("invalid PostgreSQL JDBC URL: %w", err)
	}

	parsed := &JDBCParsed{
		Dialect: "postgresql",
		Host:    u.Hostname(),
		Port:    5432,
		Props:   make(map[string]string),
	}

	if p := u.Port(); p != "" {
		port, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("invalid port in PostgreSQL URL: %s", p)
		}
		parsed.Port = port
	}

	parsed.Database = strings.TrimPrefix(u.Path, "/")

	// Parse query params into Props
	for k, v := range u.Query() {
		if len(v) > 0 {
			parsed.Props[k] = v[0]
		}
	}

	return parsed, nil
}
