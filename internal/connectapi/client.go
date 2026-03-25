package connectapi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// ConnectAuthOpts holds authentication options for the Kafka Connect REST API.
type ConnectAuthOpts struct {
	BasicAuth   string // "user:password" or empty
	BearerToken string // bearer token or empty
	CACertPath  string // path to CA cert PEM file or empty
}

// ConnectClient is an HTTP client for the Kafka Connect REST API.
type ConnectClient struct {
	baseURL    string
	httpClient *http.Client
	auth       ConnectAuthOpts
}

// NewConnectClient creates a new ConnectClient.
func NewConnectClient(baseURL string, auth ConnectAuthOpts) (*ConnectClient, error) {
	// Validate URL scheme
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid Connect URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("Connect URL must use http or https scheme, got: %s", u.Scheme)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()

	if auth.CACertPath != "" {
		caPEM, err := os.ReadFile(auth.CACertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read Connect CA cert %s: %w", auth.CACertPath, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("failed to parse Connect CA cert from %s", auth.CACertPath)
		}
		transport.TLSClientConfig = &tls.Config{
			RootCAs: pool,
		}
	}

	return &ConnectClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout:   15 * time.Second,
			Transport: transport,
			// Prevent redirect-based SSRF: do not follow redirects automatically.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return fmt.Errorf("Connect API redirect blocked (to %s): redirects are not followed for security", req.URL.Host)
			},
		},
		auth: auth,
	}, nil
}

// isLinkLocalIP checks if an IP is in the link-local range (169.254.0.0/16)
// which includes the cloud metadata endpoint 169.254.169.254.
func isLinkLocalIP(host string) bool {
	ips, err := net.LookupHost(host)
	if err != nil {
		return false
	}
	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}
		// Block link-local (169.254.0.0/16) — includes cloud metadata
		if ip4 := ip.To4(); ip4 != nil && ip4[0] == 169 && ip4[1] == 254 {
			return true
		}
	}
	return false
}

// GetConnectorConfig fetches the configuration for a named connector.
func (c *ConnectClient) GetConnectorConfig(ctx context.Context, name string) (map[string]string, error) {
	// SSRF protection: block link-local addresses (cloud metadata)
	u, _ := url.Parse(c.baseURL)
	if u != nil && isLinkLocalIP(u.Hostname()) {
		return nil, fmt.Errorf("Connect URL resolves to link-local address (blocked for security)")
	}

	reqURL := fmt.Sprintf("%s/connectors/%s/config", c.baseURL, url.PathEscape(name))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	c.applyAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Connect API unreachable: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // limit to 1MB

	switch {
	case resp.StatusCode == http.StatusOK:
		var cfg map[string]string
		if err := json.Unmarshal(body, &cfg); err != nil {
			return nil, fmt.Errorf("failed to decode connector config: %w", err)
		}
		return cfg, nil

	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return nil, fmt.Errorf("Connect API auth failed (HTTP %d): check --connect-basic-auth or --connect-bearer-token", resp.StatusCode)

	case resp.StatusCode == http.StatusNotFound:
		return nil, fmt.Errorf("connector '%s' not found. Use GET /connectors to list available connectors", name)

	default:
		return nil, fmt.Errorf("Connect API returned HTTP %d: %s", resp.StatusCode, string(body))
	}
}

// ListConnectors returns the names of all connectors.
func (c *ConnectClient) ListConnectors(ctx context.Context) ([]string, error) {
	reqURL := fmt.Sprintf("%s/connectors", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
	c.applyAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Connect API unreachable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Connect API returned HTTP %d", resp.StatusCode)
	}

	var names []string
	if err := json.NewDecoder(resp.Body).Decode(&names); err != nil {
		return nil, fmt.Errorf("failed to decode connector list: %w", err)
	}
	return names, nil
}

func (c *ConnectClient) applyAuth(req *http.Request) {
	if c.auth.BasicAuth != "" {
		parts := strings.SplitN(c.auth.BasicAuth, ":", 2)
		if len(parts) == 2 {
			req.SetBasicAuth(parts[0], parts[1])
		}
	}
	if c.auth.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.auth.BearerToken)
	}
}
