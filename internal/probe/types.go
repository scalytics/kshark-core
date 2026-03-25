package probe

import (
	"context"
	"time"
)

// ProbeTarget contains the extracted connection parameters for a database target.
type ProbeTarget struct {
	Host        string            // hostname or IP
	Port        int               // port number
	TLS         bool              // whether TLS is required
	Username    string            // auth username
	Password    string            // auth password
	Database    string            // target database name
	Collection  string            // MongoDB only: target collection
	SSLMode     string            // PostgreSQL sslmode
	SRV         bool              // MongoDB: whether this is a mongodb+srv:// URI
	OriginalURI string            // original connection URI (redacted for display)
	ExtraProps  map[string]string // additional driver-specific properties
}

// ProbeStep represents the result of one layer check.
type ProbeStep struct {
	Layer   string        // "L3-DNS", "L4-TCP", "L5-6-TLS", "L7-Auth", etc.
	Status  string        // "OK", "WARN", "FAIL", "SKIP"
	Detail  string        // human-readable detail
	Hint    string        // actionable hint on failure
	Latency time.Duration // measured latency for this step
}

// Prober is the interface that each database probe implements.
type Prober interface {
	Probe(ctx context.Context, target ProbeTarget) []ProbeStep
	Type() string
}

// Status constants.
const (
	StatusOK   = "OK"
	StatusWARN = "WARN"
	StatusFAIL = "FAIL"
	StatusSKIP = "SKIP"
)
