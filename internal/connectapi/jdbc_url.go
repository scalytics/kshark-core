package connectapi

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// JDBCParsed holds the components extracted from a JDBC URL.
type JDBCParsed struct {
	Dialect  string            // "db2", "postgresql", "mysql", "sqlserver", "oracle"
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
	case strings.HasPrefix(rest, "mysql://"):
		return parseMySQLURL(rest)
	case strings.HasPrefix(rest, "sqlserver://"):
		return parseSQLServerURL(rest)
	case strings.HasPrefix(rest, "oracle:"):
		return parseOracleURL(rest)
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

func parseMySQLURL(urlPart string) (*JDBCParsed, error) {
	// Format: mysql://host[:port]/dbname[?params]
	u, err := url.Parse(urlPart)
	if err != nil {
		return nil, fmt.Errorf("invalid MySQL JDBC URL: %w", err)
	}

	parsed := &JDBCParsed{
		Dialect: "mysql",
		Host:    u.Hostname(),
		Port:    3306,
		Props:   make(map[string]string),
	}

	if p := u.Port(); p != "" {
		port, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("invalid port in MySQL URL: %s", p)
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

func parseSQLServerURL(urlPart string) (*JDBCParsed, error) {
	// Format: sqlserver://host[:port][;property=value[;property=value]]
	// Or: sqlserver://host[:port];databaseName=db;encrypt=true
	rest := strings.TrimPrefix(urlPart, "sqlserver://")

	parsed := &JDBCParsed{
		Dialect: "sqlserver",
		Port:    1433,
		Props:   make(map[string]string),
	}

	// Split on first semicolon to separate host:port from properties
	parts := strings.SplitN(rest, ";", 2)
	hostPort := parts[0]

	// Parse host:port
	if colonIdx := strings.LastIndex(hostPort, ":"); colonIdx >= 0 {
		parsed.Host = hostPort[:colonIdx]
		p, err := strconv.Atoi(hostPort[colonIdx+1:])
		if err != nil {
			return nil, fmt.Errorf("invalid port in SQL Server URL: %s", hostPort[colonIdx+1:])
		}
		parsed.Port = p
	} else {
		parsed.Host = hostPort
	}

	// Parse semicolon-delimited properties
	if len(parts) > 1 {
		for _, pair := range strings.Split(parts[1], ";") {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}
			if eqIdx := strings.Index(pair, "="); eqIdx >= 0 {
				key := pair[:eqIdx]
				value := pair[eqIdx+1:]
				parsed.Props[key] = value
				// Map common JDBC property names
				if strings.EqualFold(key, "databaseName") {
					parsed.Database = value
				}
			}
		}
	}

	return parsed, nil
}

func parseOracleURL(urlPart string) (*JDBCParsed, error) {
	// Formats:
	//   oracle:thin:@host:port/service
	//   oracle:thin:@host:port:sid
	//   oracle:thin:@//host:port/service
	//   oracle:thin:@(DESCRIPTION=(...))
	rest := strings.TrimPrefix(urlPart, "oracle:")

	parsed := &JDBCParsed{
		Dialect: "oracle",
		Port:    1521,
		Props:   make(map[string]string),
	}

	// Remove driver type prefix (thin:, oci:, etc.)
	if colonIdx := strings.Index(rest, ":"); colonIdx >= 0 {
		driverType := rest[:colonIdx]
		parsed.Props["driverType"] = driverType
		rest = rest[colonIdx+1:]
	}

	// Remove leading @ if present
	rest = strings.TrimPrefix(rest, "@")

	// Handle TNS descriptor format: (DESCRIPTION=...)
	if strings.HasPrefix(rest, "(") {
		// Cannot parse TNS descriptor fully; extract what we can
		parsed.Props["connectDescriptor"] = rest
		// Try to find HOST and PORT in the descriptor
		if hostIdx := strings.Index(rest, "(HOST="); hostIdx >= 0 {
			hostEnd := strings.Index(rest[hostIdx+6:], ")")
			if hostEnd >= 0 {
				parsed.Host = rest[hostIdx+6 : hostIdx+6+hostEnd]
			}
		}
		if portIdx := strings.Index(rest, "(PORT="); portIdx >= 0 {
			portEnd := strings.Index(rest[portIdx+6:], ")")
			if portEnd >= 0 {
				p, err := strconv.Atoi(rest[portIdx+6 : portIdx+6+portEnd])
				if err == nil {
					parsed.Port = p
				}
			}
		}
		if svcIdx := strings.Index(rest, "(SERVICE_NAME="); svcIdx >= 0 {
			svcEnd := strings.Index(rest[svcIdx+14:], ")")
			if svcEnd >= 0 {
				parsed.Database = rest[svcIdx+14 : svcIdx+14+svcEnd]
			}
		}
		return parsed, nil
	}

	// Handle //host:port/service format
	rest = strings.TrimPrefix(rest, "//")

	// Split host:port from service/SID
	// Could be host:port/service or host:port:sid
	slashIdx := strings.Index(rest, "/")
	if slashIdx >= 0 {
		// host:port/service format
		hostPort := rest[:slashIdx]
		parsed.Database = rest[slashIdx+1:]

		if colonIdx := strings.LastIndex(hostPort, ":"); colonIdx >= 0 {
			parsed.Host = hostPort[:colonIdx]
			p, err := strconv.Atoi(hostPort[colonIdx+1:])
			if err != nil {
				return nil, fmt.Errorf("invalid port in Oracle URL: %s", hostPort[colonIdx+1:])
			}
			parsed.Port = p
		} else {
			parsed.Host = hostPort
		}
	} else {
		// host:port:sid format
		parts := strings.SplitN(rest, ":", 3)
		switch len(parts) {
		case 3:
			parsed.Host = parts[0]
			p, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, fmt.Errorf("invalid port in Oracle URL: %s", parts[1])
			}
			parsed.Port = p
			parsed.Database = parts[2]
		case 2:
			parsed.Host = parts[0]
			// Could be host:port or host:sid
			p, err := strconv.Atoi(parts[1])
			if err != nil {
				// Treat as host:sid
				parsed.Database = parts[1]
			} else {
				parsed.Port = p
			}
		case 1:
			parsed.Host = parts[0]
		}
	}

	return parsed, nil
}
