package probe

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// PostgresProber probes PostgreSQL targets.
type PostgresProber struct{}

func NewPostgresProber() *PostgresProber { return &PostgresProber{} }
func (p *PostgresProber) Type() string   { return "postgresql" }

func (p *PostgresProber) Probe(ctx context.Context, target ProbeTarget) []ProbeStep {
	var steps []ProbeStep

	// L3-DNS
	dnsStep := ProbeDNS(target.Host)
	steps = append(steps, dnsStep)
	if dnsStep.Status == StatusFAIL {
		return steps
	}

	// L4-TCP
	addr := fmt.Sprintf("%s:%d", target.Host, target.Port)
	conn, tcpStep := ProbeTCP(addr, 8*time.Second)
	steps = append(steps, tcpStep)
	if conn != nil {
		conn.Close()
	}
	if tcpStep.Status == StatusFAIL {
		return steps
	}

	// L5-6-TLS + L7 via pgx
	dsn := buildPostgresDSN(target)
	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	start := time.Now()
	pgConn, err := pgx.Connect(probeCtx, dsn)
	connLatency := time.Since(start)

	if err != nil {
		steps = append(steps, classifyPgError(err, target))
		return steps
	}
	defer pgConn.Close(context.Background())

	// Success — all layers passed
	if target.SSLMode == "disable" {
		steps = append(steps, ProbeStep{
			Layer:  "L5-6-TLS",
			Status: StatusSKIP,
			Detail: "TLS disabled (sslmode=disable)",
		})
	} else {
		steps = append(steps, ProbeStep{
			Layer:   "L5-6-TLS",
			Status:  StatusOK,
			Detail:  fmt.Sprintf("TLS connection established (sslmode=%s)", target.SSLMode),
			Latency: connLatency,
		})
	}

	steps = append(steps, ProbeStep{
		Layer:   "L7-Auth",
		Status:  StatusOK,
		Detail:  fmt.Sprintf("Authenticated as '%s'", target.Username),
		Latency: connLatency,
	})

	steps = append(steps, ProbeStep{
		Layer:   "L7-Ready",
		Status:  StatusOK,
		Detail:  fmt.Sprintf("Connected to database '%s', server ready", target.Database),
		Latency: connLatency,
	})

	return steps
}

// pgQuote quotes a value for use in a PostgreSQL keyword=value DSN.
// Values containing spaces, quotes, or backslashes must be single-quoted
// with internal single quotes escaped as \'.
func pgQuote(v string) string {
	if v == "" {
		return "''"
	}
	if !strings.ContainsAny(v, " '\\\t\n") {
		return v
	}
	escaped := strings.ReplaceAll(v, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return "'" + escaped + "'"
}

func buildPostgresDSN(target ProbeTarget) string {
	sslmode := target.SSLMode
	if sslmode == "" {
		sslmode = "prefer"
	}

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		pgQuote(target.Host), target.Port, pgQuote(target.Username),
		pgQuote(target.Password), pgQuote(target.Database), pgQuote(sslmode))

	if rootCert, ok := target.ExtraProps["sslrootcert"]; ok && rootCert != "" {
		dsn += fmt.Sprintf(" sslrootcert=%s", pgQuote(rootCert))
	}

	return dsn
}

func classifyPgError(err error, target ProbeTarget) ProbeStep {
	msg := ScrubCredentials(err.Error())
	lower := strings.ToLower(msg)

	switch {
	case strings.Contains(lower, "no such host"):
		return ProbeStep{
			Layer:  "L3-DNS",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("DNS resolution failed: %v", err),
			Hint:   "Check DNS resolution for the PostgreSQL host.",
		}
	case strings.Contains(lower, "connection refused"):
		return ProbeStep{
			Layer:  "L4-TCP",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Connection refused on port %d: %v", target.Port, err),
			Hint:   "PostgreSQL not running or listen_addresses not set. Check pg_isready and firewall rules.",
		}
	case strings.Contains(lower, "i/o timeout") || strings.Contains(lower, "context deadline exceeded"):
		return ProbeStep{
			Layer:  "L4-TCP",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Connection timed out: %v", err),
			Hint:   "Firewall blocking port or no route to host.",
		}
	case strings.Contains(lower, "certificate") || strings.Contains(lower, "tls") || strings.Contains(lower, "x509"):
		return ProbeStep{
			Layer:  "L5-6-TLS",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("TLS error: %v", err),
			Hint:   "Check sslmode and CA certificate. Use sslrootcert for custom CAs.",
		}
	case strings.Contains(lower, "database") && strings.Contains(lower, "does not exist"):
		return ProbeStep{
			Layer:  "L7-Startup",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Database not found: %v", err),
			Hint:   fmt.Sprintf("Database '%s' does not exist. Check with \\l in psql.", target.Database),
		}
	case strings.Contains(lower, "role") && strings.Contains(lower, "does not exist"):
		return ProbeStep{
			Layer:  "L7-Startup",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Role not found: %v", err),
			Hint:   fmt.Sprintf("Role '%s' does not exist. Check with \\du in psql or create the role.", target.Username),
		}
	case strings.Contains(lower, "password authentication failed"):
		return ProbeStep{
			Layer:  "L7-Auth",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Authentication failed: %v", err),
			Hint:   "Wrong password. Check pg_hba.conf auth method (md5 vs scram-sha-256).",
		}
	case strings.Contains(lower, "no pg_hba.conf entry"):
		return ProbeStep{
			Layer:  "L7-Auth",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Client IP not allowed: %v", err),
			Hint:   "Add an entry in pg_hba.conf for this host's IP range.",
		}
	default:
		return ProbeStep{
			Layer:  "L7-Ready",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("PostgreSQL error: %v", err),
			Hint:   "Check connection parameters and PostgreSQL server logs.",
		}
	}
}
