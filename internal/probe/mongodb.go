package probe

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoProber probes MongoDB/Atlas targets.
type MongoProber struct{}

func NewMongoProber() *MongoProber  { return &MongoProber{} }
func (p *MongoProber) Type() string { return "mongodb" }

func (p *MongoProber) Probe(ctx context.Context, target ProbeTarget) []ProbeStep {
	var steps []ProbeStep

	// Determine the URI to use — prefer the original if available
	uri := target.ExtraProps["connection.uri"]
	if uri == "" {
		uri = buildMongoURI(target)
	}

	// L3-DNS
	if target.SRV {
		hosts, step := ProbeSRV(target.Host)
		steps = append(steps, step)
		if step.Status == StatusFAIL {
			return steps
		}
		// L4-TCP: probe first resolved host
		if len(hosts) > 0 {
			conn, tcpStep := ProbeTCP(hosts[0], 8*time.Second)
			steps = append(steps, tcpStep)
			if conn != nil {
				conn.Close()
			}
			if tcpStep.Status == StatusFAIL {
				return steps
			}
		}
	} else {
		step := ProbeDNS(target.Host)
		steps = append(steps, step)
		if step.Status == StatusFAIL {
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
	}

	// L5-6-TLS + L7 via driver
	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	clientOpts := options.Client().
		ApplyURI(uri).
		SetServerSelectionTimeout(10 * time.Second).
		SetConnectTimeout(8 * time.Second)

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		steps = append(steps, classifyMongoError(err))
		return steps
	}
	defer client.Disconnect(context.Background())

	// L7-Ping
	db := target.Database
	if db == "" {
		db = "admin"
	}

	start := time.Now()
	err = client.Database(db).RunCommand(probeCtx, bson.D{bson.E{Key: "ping", Value: 1}}).Err()
	pingLatency := time.Since(start)

	if err != nil {
		// On failure, try to classify which layer failed
		steps = append(steps, classifyMongoError(err))
		return steps
	}

	// If we got here, TLS (if used) and auth both succeeded
	if target.TLS || target.SRV {
		steps = append(steps, ProbeStep{
			Layer:  "L5-6-TLS",
			Status: StatusOK,
			Detail: "TLS connection established via driver",
		})
	} else {
		steps = append(steps, ProbeStep{
			Layer:  "L5-6-TLS",
			Status: StatusSKIP,
			Detail: "Plain connection (no TLS)",
		})
	}

	if target.Username != "" {
		steps = append(steps, ProbeStep{
			Layer:  "L7-Auth",
			Status: StatusOK,
			Detail: "Authentication succeeded",
		})
	}

	steps = append(steps, ProbeStep{
		Layer:   "L7-Ping",
		Status:  StatusOK,
		Detail:  fmt.Sprintf("Database '%s' responded to ping", db),
		Latency: pingLatency,
	})

	// L7-Coll: optional collection check
	if target.Collection != "" && target.Database != "" {
		collStart := time.Now()
		cols, err := client.Database(target.Database).ListCollectionNames(probeCtx,
			bson.D{bson.E{Key: "name", Value: target.Collection}})
		collLatency := time.Since(collStart)

		if err != nil {
			steps = append(steps, ProbeStep{
				Layer:   "L7-Collection",
				Status:  StatusWARN,
				Detail:  fmt.Sprintf("Failed to check collection '%s': %v", target.Collection, err),
				Latency: collLatency,
			})
		} else if len(cols) == 0 {
			steps = append(steps, ProbeStep{
				Layer:   "L7-Collection",
				Status:  StatusWARN,
				Detail:  fmt.Sprintf("Collection '%s' not found (may be auto-created by sink)", target.Collection),
				Latency: collLatency,
			})
		} else {
			steps = append(steps, ProbeStep{
				Layer:   "L7-Collection",
				Status:  StatusOK,
				Detail:  fmt.Sprintf("Collection '%s' is accessible", target.Collection),
				Latency: collLatency,
			})
		}
	}

	return steps
}

func buildMongoURI(target ProbeTarget) string {
	scheme := "mongodb"
	if target.SRV {
		scheme = "mongodb+srv"
	}

	userinfo := ""
	if target.Username != "" {
		userinfo = url.UserPassword(target.Username, target.Password).String() + "@"
	}

	host := target.Host
	if !target.SRV && target.Port != 0 && target.Port != 27017 {
		host = fmt.Sprintf("%s:%d", target.Host, target.Port)
	}

	db := target.Database
	if db == "" {
		db = "admin"
	}

	params := ""
	if target.TLS && !target.SRV {
		params = "?tls=true"
	}

	return fmt.Sprintf("%s://%s%s/%s%s", scheme, userinfo, host, db, params)
}

func classifyMongoError(err error) ProbeStep {
	msg := ScrubCredentials(err.Error())
	lower := strings.ToLower(msg)

	switch {
	case strings.Contains(lower, "no such host") || strings.Contains(lower, "nxdomain"):
		return ProbeStep{
			Layer:  "L3-DNS",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("DNS resolution failed: %v", err),
			Hint:   "Check if hostname is resolvable. Atlas clusters require public or peered DNS.",
		}
	case strings.Contains(lower, "connection refused"):
		return ProbeStep{
			Layer:  "L4-TCP",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Connection refused: %v", err),
			Hint:   "MongoDB not listening or IP allowlist blocking. Check Atlas Network Access or security group rules.",
		}
	case strings.Contains(lower, "i/o timeout") || strings.Contains(lower, "context deadline exceeded"):
		return ProbeStep{
			Layer:  "L4-TCP",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Connection timed out: %v", err),
			Hint:   "Firewall or no route to host. Check network path and PrivateLink status.",
		}
	case strings.Contains(lower, "server selection"):
		return ProbeStep{
			Layer:  "L4-TCP",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Server selection failed: %v", err),
			Hint:   "Cannot reach any MongoDB server. Check DNS, firewall, and connection URI.",
		}
	case strings.Contains(lower, "tls") || strings.Contains(lower, "x509") || strings.Contains(lower, "certificate"):
		return ProbeStep{
			Layer:  "L5-6-TLS",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("TLS error: %v", err),
			Hint:   "Check TLS/SSL settings. Atlas uses Let's Encrypt or DigiCert CAs.",
		}
	case strings.Contains(lower, "authentication failed") || strings.Contains(lower, "auth error"):
		return ProbeStep{
			Layer:  "L7-Auth",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Authentication failed: %v", err),
			Hint:   "Wrong username/password or wrong authSource. Try authSource=admin.",
		}
	case strings.Contains(lower, "not authorized"):
		return ProbeStep{
			Layer:  "L7-Ping",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("Authorization failed: %v", err),
			Hint:   "User authenticated but lacks roles on target database. Grant readWrite role.",
		}
	default:
		return ProbeStep{
			Layer:  "L7-Ping",
			Status: StatusFAIL,
			Detail: fmt.Sprintf("MongoDB error: %v", err),
			Hint:   "Check connection URI, credentials, and database name.",
		}
	}
}

// ParseMongoURI extracts connection parameters from a MongoDB URI.
func ParseMongoURI(uri string) (host string, port int, username, password, database string, isSRV, tlsRequired bool, err error) {
	port = 27017 // default

	u, parseErr := url.Parse(uri)
	if parseErr != nil {
		err = fmt.Errorf("invalid MongoDB URI: %w", parseErr)
		return
	}

	switch u.Scheme {
	case "mongodb+srv":
		isSRV = true
		tlsRequired = true
	case "mongodb":
		// standard
	default:
		err = fmt.Errorf("unsupported MongoDB URI scheme: %s", u.Scheme)
		return
	}

	host = u.Hostname()
	if p := u.Port(); p != "" {
		fmt.Sscanf(p, "%d", &port)
	}

	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
	}

	database = strings.TrimPrefix(u.Path, "/")

	// Check query params for TLS
	q := u.Query()
	if q.Get("tls") == "true" || q.Get("ssl") == "true" {
		tlsRequired = true
	}

	return
}
