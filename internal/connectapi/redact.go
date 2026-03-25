package connectapi

import (
	"net/url"
	"strings"
)

// RedactConnectorConfig returns a copy of the config with sensitive fields redacted.
func RedactConnectorConfig(cfg map[string]string) map[string]string {
	out := make(map[string]string, len(cfg))
	for k, v := range cfg {
		lower := strings.ToLower(k)
		switch {
		case strings.Contains(lower, "password"),
			strings.Contains(lower, "secret"),
			strings.Contains(lower, "token"),
			strings.Contains(lower, "apikey"),
			strings.Contains(lower, "api.key"):
			out[k] = "[REDACTED]"
		case lower == "connection.uri":
			out[k] = RedactMongoURI(v)
		default:
			out[k] = v
		}
	}
	return out
}

// RedactMongoURI replaces the userinfo portion of a MongoDB URI.
func RedactMongoURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		// Fallback: regex-style replacement
		if idx := strings.Index(uri, "://"); idx >= 0 {
			rest := uri[idx+3:]
			if atIdx := strings.Index(rest, "@"); atIdx >= 0 {
				return uri[:idx+3] + "[REDACTED]@" + rest[atIdx+1:]
			}
		}
		return uri
	}

	if u.User != nil {
		u.User = url.User("[REDACTED]")
	}
	return u.String()
}
