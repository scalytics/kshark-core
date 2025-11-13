# SSRF Vulnerability Fix Implementation Guide

**Priority:** CRITICAL
**CVSS Score:** 8.6 (High)
**Estimated Time:** 4-6 hours
**Status:** 🔴 NOT IMPLEMENTED - REQUIRES IMMEDIATE ATTENTION

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Vulnerability Description](#vulnerability-description)
3. [Affected Code Locations](#affected-code-locations)
4. [Implementation Steps](#implementation-steps)
5. [Testing Guide](#testing-guide)
6. [Validation Checklist](#validation-checklist)
7. [Rollback Plan](#rollback-plan)

---

## Executive Summary

### What is the Issue?

kshark accepts arbitrary URLs from configuration files for Schema Registry and REST Proxy without validation. This creates a Server-Side Request Forgery (SSRF) vulnerability that allows attackers to:

- Scan internal network infrastructure
- Access cloud metadata services (AWS, Azure, GCP)
- Probe internal services through the application
- Bypass firewall restrictions

### Impact

**Severity:** CRITICAL

An attacker with control over configuration files can use kshark as a proxy to:
1. Discover internal network topology
2. Access AWS EC2 metadata (169.254.169.254) to steal credentials
3. Port scan internal services
4. Exfiltrate data from internal systems

### Solution Overview

Add URL validation that:
1. Validates URL format and scheme (HTTPS only recommended)
2. Resolves hostnames to IP addresses
3. Blocks access to private/internal IP ranges
4. Prevents redirect-based bypasses

---

## Vulnerability Description

### Vulnerable Code Locations

**File:** `cmd/kshark/main.go`

**Location 1: Schema Registry Check (Lines 761-799)**
```go
func checkSchemaRegistry(ctx context.Context, r *Report, p map[string]string, tlsConf *tls.Config) {
    url := strings.TrimSpace(p["schema.registry.url"])
    if url == "" {
        return
    }
    // ⚠️ NO VALIDATION - ACCEPTS ANY URL
    client := httpClientFromTLS(tlsConf, 8*time.Second)
    req, _ := http.NewRequestWithContext(ctx, "GET",
                                        strings.TrimRight(url, "/")+"/subjects", nil)
    resp, err := client.Do(req)
    // ...
}
```

**Location 2: REST Proxy Check (Lines 1136-1159)**
```go
if rest := strings.TrimSpace(props["rest.proxy.url"]); rest != "" {
    // ⚠️ SAME VULNERABILITY
    client := httpClientFromTLS(tlsConf, 8*time.Second)
    req, _ := http.NewRequest("GET", strings.TrimRight(rest, "/")+"/topics", nil)
    resp, err := client.Do(req)
    // ...
}
```

**Location 3: HTTP Client Redirect Handling (Lines 756-759)**
```go
func httpClientFromTLS(tlsConf *tls.Config, timeout time.Duration) *http.Client {
    // ...
    return &http.Client{Transport: tr, Timeout: timeout}
    // ⚠️ NO CheckRedirect - FOLLOWS ALL REDIRECTS
}
```

### Attack Scenarios

#### Scenario 1: Internal Network Scanning
```properties
# Attacker-controlled client.properties
schema.registry.url=http://192.168.1.10:8081
rest.proxy.url=http://10.0.0.5:8082
```
**Result:** Scans internal network, reveals which hosts/ports are accessible

#### Scenario 2: Cloud Metadata Access
```properties
schema.registry.url=http://169.254.169.254/latest/meta-data/iam/security-credentials/
```
**Result:** Steals AWS IAM credentials, compromises cloud infrastructure

#### Scenario 3: Internal Service Probing
```properties
schema.registry.url=http://localhost:9200/_cluster/health
rest.proxy.url=http://127.0.0.1:6379/INFO
```
**Result:** Probes Elasticsearch, Redis, or other internal services

#### Scenario 4: Redirect-based Bypass
```properties
schema.registry.url=https://attacker.com/redirect-to-metadata
```
**Attacker's server returns:**
```http
HTTP/1.1 302 Found
Location: http://169.254.169.254/latest/meta-data/
```
**Result:** Bypasses initial domain check via redirect

---

## Implementation Steps

### Step 1: Add URL Validation Functions

**Location:** Add after line 817 (after `mtuCheck` function)

```go
// isAllowedURL validates that a URL is safe to access from SSRF perspective
func isAllowedURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	// Only allow HTTP(S) schemes
	// Consider enforcing HTTPS only for production
	if u.Scheme != "https" && u.Scheme != "http" {
		return fmt.Errorf("only HTTP(S) schemes allowed, got: %s", u.Scheme)
	}

	// Ensure hostname exists
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("no hostname in URL")
	}

	// Don't allow URLs with authentication embedded
	if u.User != nil {
		return fmt.Errorf("URLs with embedded credentials are not allowed")
	}

	// Resolve hostname to IP addresses
	ips, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("cannot resolve host %s: %w", host, err)
	}

	// Block private/internal IPs
	for _, ip := range ips {
		if isPrivateIP(ip) {
			return fmt.Errorf("private/internal IP addresses not allowed: %s resolves to %s", host, ip)
		}
	}

	return nil
}

// isPrivateIP checks if an IP address is in private or reserved ranges
func isPrivateIP(ip net.IP) bool {
	// List of private and reserved IP ranges to block
	privateRanges := []string{
		// RFC1918 - Private networks
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",

		// Loopback
		"127.0.0.0/8",

		// Link-local (including AWS metadata)
		"169.254.0.0/16",

		// IPv6 loopback
		"::1/128",

		// IPv6 private
		"fc00::/7",

		// IPv6 link-local
		"fe80::/10",

		// Current network (only valid as source)
		"0.0.0.0/8",

		// Shared address space (CGN)
		"100.64.0.0/10",

		// IETF protocol assignments
		"192.0.0.0/24",

		// Documentation/TEST-NET
		"192.0.2.0/24",
		"198.51.100.0/24",
		"203.0.113.0/24",

		// Benchmarking
		"198.18.0.0/15",

		// Multicast
		"224.0.0.0/4",

		// Reserved
		"240.0.0.0/4",

		// Broadcast
		"255.255.255.255/32",
	}

	for _, cidr := range privateRanges {
		_, subnet, err := net.ParseCIDR(cidr)
		if err != nil {
			continue // Skip malformed CIDRs
		}
		if subnet.Contains(ip) {
			return true
		}
	}

	return false
}
```

**Required imports:** (Add to import block if not present)
```go
import (
	"net"
	"net/url"
	// ... other imports
)
```

---

### Step 2: Add Redirect Protection

**Location:** Modify `httpClientFromTLS` function (Line 756)

**BEFORE:**
```go
func httpClientFromTLS(tlsConf *tls.Config, timeout time.Duration) *http.Client {
	tr := &http.Transport{
		TLSClientConfig: tlsConf,
		Proxy:           http.ProxyFromEnvironment,
		IdleConnTimeout: 10 * time.Second,
	}
	return &http.Client{Transport: tr, Timeout: timeout}
}
```

**AFTER:**
```go
func httpClientFromTLS(tlsConf *tls.Config, timeout time.Duration) *http.Client {
	tr := &http.Transport{
		TLSClientConfig: tlsConf,
		Proxy:           http.ProxyFromEnvironment,
		IdleConnTimeout: 10 * time.Second,
	}

	return &http.Client{
		Transport: tr,
		Timeout:   timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Limit redirect depth
			if len(via) >= 3 {
				return fmt.Errorf("too many redirects (maximum 3)")
			}

			// Validate each redirect URL
			if err := isAllowedURL(req.URL.String()); err != nil {
				return fmt.Errorf("redirect blocked: %w", err)
			}

			return nil
		},
	}
}
```

---

### Step 3: Apply Validation in Schema Registry Check

**Location:** Modify `checkSchemaRegistry` function (Line 761)

**Add this code immediately after the URL is extracted:**

```go
func checkSchemaRegistry(ctx context.Context, r *Report, p map[string]string, tlsConf *tls.Config) {
	url := strings.TrimSpace(p["schema.registry.url"])
	if url == "" {
		return
	}

	// ✅ ADD THIS VALIDATION
	if err := isAllowedURL(url); err != nil {
		addRow(r, Row{
			component: "schema-registry",
			target:    url,
			layer:     L7HTTP,
			status:    FAIL,
			detail:    fmt.Sprintf("URL validation failed: %v", err),
			hint:      "Schema Registry URL must be a valid HTTPS URL pointing to a public endpoint. Private IPs, localhost, and cloud metadata endpoints are blocked for security.",
		})
		return
	}

	// Rest of function continues normally...
	client := httpClientFromTLS(tlsConf, 8*time.Second)
	// ...
}
```

---

### Step 4: Apply Validation in REST Proxy Check

**Location:** Modify REST Proxy check in main scan loop (around Line 1136)

**BEFORE:**
```go
if rest := strings.TrimSpace(props["rest.proxy.url"]); rest != "" {
	client := httpClientFromTLS(tlsConf, 8*time.Second)
	req, _ := http.NewRequest("GET", strings.TrimRight(rest, "/")+"/topics", nil)
	// ...
}
```

**AFTER:**
```go
if rest := strings.TrimSpace(props["rest.proxy.url"]); rest != "" {
	// ✅ ADD THIS VALIDATION
	if err := isAllowedURL(rest); err != nil {
		addRow(report, Row{
			component: "rest-proxy",
			target:    rest,
			layer:     L7HTTP,
			status:    FAIL,
			detail:    fmt.Sprintf("URL validation failed: %v", err),
			hint:      "REST Proxy URL must be a valid HTTPS URL pointing to a public endpoint. Private IPs, localhost, and cloud metadata endpoints are blocked for security.",
		})
	} else {
		// Original code continues here
		client := httpClientFromTLS(tlsConf, 8*time.Second)
		req, _ := http.NewRequest("GET", strings.TrimRight(rest, "/")+"/topics", nil)
		// ...
	}
}
```

---

### Step 5: (Optional) Add Configuration Flag for Internal Networks

If you need to allow internal IPs in development environments, add a configuration option:

```go
// Add to properties
allowInternalURLs := strings.ToLower(p["allow.internal.urls"]) == "true"

// Modify isAllowedURL to accept a parameter
func isAllowedURL(rawURL string, allowInternal bool) error {
	// ... existing code ...

	// Block private/internal IPs (unless explicitly allowed)
	if !allowInternal {
		for _, ip := range ips {
			if isPrivateIP(ip) {
				return fmt.Errorf("private/internal IP addresses not allowed: %s resolves to %s", host, ip)
			}
		}
	}

	return nil
}
```

**Configuration:**
```properties
# client.properties
# ONLY enable in development/testing environments
# NEVER enable in production
allow.internal.urls=true
```

---

## Testing Guide

### Unit Tests

Create `cmd/kshark/ssrf_test.go`:

```go
package main

import (
	"net"
	"testing"
)

func TestIsPrivateIP(t *testing.T) {
	tests := []struct {
		name     string
		ip       string
		expected bool
	}{
		// Should block
		{"Localhost IPv4", "127.0.0.1", true},
		{"Private 10.x", "10.0.0.1", true},
		{"Private 172.16.x", "172.16.0.1", true},
		{"Private 192.168.x", "192.168.1.1", true},
		{"AWS Metadata", "169.254.169.254", true},
		{"Link-local", "169.254.1.1", true},
		{"Localhost IPv6", "::1", true},
		{"Private IPv6", "fc00::1", true},

		// Should allow
		{"Public IP", "8.8.8.8", false},
		{"Google DNS", "8.8.4.4", false},
		{"Cloudflare", "1.1.1.1", false},
		{"Public IPv6", "2001:4860:4860::8888", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP(tt.ip)
			if ip == nil {
				t.Fatalf("Invalid IP: %s", tt.ip)
			}
			result := isPrivateIP(ip)
			if result != tt.expected {
				t.Errorf("isPrivateIP(%s) = %v, expected %v", tt.ip, result, tt.expected)
			}
		})
	}
}

func TestIsAllowedURL(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		shouldErr bool
	}{
		// Should pass
		{"Valid HTTPS", "https://schema-registry.example.com:8081", false},
		{"Valid HTTP public", "http://schema-registry.example.com:8081", false},

		// Should fail
		{"Localhost", "http://localhost:8081", true},
		{"127.0.0.1", "http://127.0.0.1:8081", true},
		{"Private 10.x", "http://10.0.0.1:8081", true},
		{"Private 192.168.x", "http://192.168.1.1:8081", true},
		{"AWS Metadata", "http://169.254.169.254/latest/meta-data/", true},
		{"Invalid scheme", "ftp://example.com", true},
		{"No scheme", "example.com", true},
		{"With credentials", "http://user:pass@example.com", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := isAllowedURL(tt.url)
			if tt.shouldErr && err == nil {
				t.Errorf("isAllowedURL(%s) should have returned error", tt.url)
			}
			if !tt.shouldErr && err != nil {
				t.Errorf("isAllowedURL(%s) should not have returned error: %v", tt.url, err)
			}
		})
	}
}
```

**Run tests:**
```bash
go test ./cmd/kshark -v -run TestIsPrivateIP
go test ./cmd/kshark -v -run TestIsAllowedURL
```

---

### Manual Testing

#### Test 1: Validate Private IP Blocking

```bash
# Create test config
cat > test-ssrf-private.properties <<EOF
bootstrap.servers=broker.example.com:9092
schema.registry.url=http://192.168.1.1:8081
EOF

# Run kshark
./kshark -props test-ssrf-private.properties -y

# Expected: FAIL status with message about private IP
```

#### Test 2: Validate Localhost Blocking

```bash
cat > test-ssrf-localhost.properties <<EOF
bootstrap.servers=broker.example.com:9092
schema.registry.url=http://localhost:8081
EOF

./kshark -props test-ssrf-localhost.properties -y

# Expected: FAIL status with message about private IP
```

#### Test 3: Validate AWS Metadata Blocking

```bash
cat > test-ssrf-aws.properties <<EOF
bootstrap.servers=broker.example.com:9092
schema.registry.url=http://169.254.169.254/latest/meta-data/
EOF

./kshark -props test-ssrf-aws.properties -y

# Expected: FAIL status with message about private IP
```

#### Test 4: Validate Public URL Success

```bash
cat > test-ssrf-public.properties <<EOF
bootstrap.servers=broker.example.com:9092
schema.registry.url=https://schema-registry.example.com:8081
EOF

./kshark -props test-ssrf-public.properties -y

# Expected: Normal connection attempt (may fail due to DNS, but URL validation passes)
```

#### Test 5: Test Redirect Protection

You'll need a test server that redirects:

```bash
# On test server
cat > redirect.php <<EOF
<?php
header('Location: http://169.254.169.254/latest/meta-data/');
exit;
?>
EOF

# In test config
cat > test-ssrf-redirect.properties <<EOF
bootstrap.servers=broker.example.com:9092
schema.registry.url=http://your-test-server.com/redirect.php
EOF

./kshark -props test-ssrf-redirect.properties -y

# Expected: FAIL with "redirect blocked" message
```

---

## Validation Checklist

Use this checklist to confirm the fix is properly implemented:

### Code Implementation
- [ ] `isAllowedURL()` function added to main.go
- [ ] `isPrivateIP()` function added to main.go
- [ ] All private IP ranges included (see implementation)
- [ ] Redirect validation added to `httpClientFromTLS()`
- [ ] Maximum redirect depth enforced (3 redirects)
- [ ] Validation applied in `checkSchemaRegistry()`
- [ ] Validation applied in REST Proxy check
- [ ] Appropriate error messages added
- [ ] Required imports added (net, net/url)

### Testing
- [ ] Unit tests written and passing
- [ ] Private IP blocking tested (10.x, 172.16.x, 192.168.x)
- [ ] Localhost blocking tested (127.0.0.1, ::1)
- [ ] AWS metadata blocking tested (169.254.169.254)
- [ ] Link-local blocking tested
- [ ] Public IP acceptance tested
- [ ] Redirect protection tested
- [ ] Invalid scheme rejection tested
- [ ] Embedded credentials rejection tested

### Security Validation
- [ ] Cannot access http://localhost:8081
- [ ] Cannot access http://127.0.0.1:8081
- [ ] Cannot access http://192.168.1.1:8081
- [ ] Cannot access http://10.0.0.1:8081
- [ ] Cannot access http://169.254.169.254/
- [ ] Can access https://legitimate-public-domain.com
- [ ] Redirects to private IPs are blocked
- [ ] Redirects to metadata services are blocked

### Documentation
- [ ] Code comments added to validation functions
- [ ] Error messages are clear and helpful
- [ ] User documentation updated (if needed)
- [ ] Security documentation updated

### Deployment
- [ ] Changes tested in development environment
- [ ] Changes tested in staging environment
- [ ] No regression in normal functionality
- [ ] Performance impact measured (should be minimal)
- [ ] Ready for production deployment

---

## Rollback Plan

If issues arise after deployment:

### Immediate Rollback (Emergency)

1. **Revert the commit:**
   ```bash
   git revert <commit-hash>
   git push origin main
   ```

2. **Or temporarily disable validation:**
   ```go
   // In isAllowedURL function, temporarily return nil
   func isAllowedURL(rawURL string) error {
       // TODO: Re-enable after fixing issue
       return nil
   }
   ```

3. **Rebuild and redeploy:**
   ```bash
   go build -o kshark ./cmd/kshark
   ```

### Debugging Failed Rollout

If legitimate URLs are being blocked:

1. **Check DNS resolution:**
   ```bash
   nslookup your-registry-url.com
   ```

2. **Verify IP addresses:**
   ```bash
   dig +short your-registry-url.com
   ```

3. **Test URL validation manually:**
   ```go
   // Add temporary logging
   func isAllowedURL(rawURL string) error {
       u, err := url.Parse(rawURL)
       fmt.Printf("DEBUG: Parsing URL: %s\n", rawURL)

       ips, err := net.LookupIP(u.Hostname())
       fmt.Printf("DEBUG: Resolved IPs: %v\n", ips)

       // Continue with validation...
   }
   ```

4. **Add allowlist for specific domains (emergency fix):**
   ```go
   func isAllowedURL(rawURL string) error {
       // Emergency allowlist
       allowedDomains := []string{
           "schema-registry.production.company.com",
           "rest-proxy.production.company.com",
       }

       u, _ := url.Parse(rawURL)
       for _, domain := range allowedDomains {
           if strings.HasSuffix(u.Hostname(), domain) {
               return nil // Skip validation for allowed domains
           }
       }

       // Continue with normal validation...
   }
   ```

---

## Additional Considerations

### For Enterprise Deployments

If your organization requires access to internal Schema Registries:

**Option 1: Use Configuration Flag**
```properties
allow.internal.urls=true  # Only in dev/staging
```

**Option 2: Use Allowlist**
```properties
trusted.domains=schema-registry.internal.company.com,rest-proxy.internal.company.com
```

**Option 3: Use Network Segmentation**
- Deploy kshark in a DMZ with no access to internal networks
- Use VPN or bastion host for internal diagnostics

### Security Monitoring

After deployment, monitor for:

```bash
# Look for validation failures in logs
grep "URL validation failed" /var/log/kshark.log

# Look for blocked attempts
grep "private/internal IP addresses not allowed" /var/log/kshark.log
```

Set up alerts for repeated validation failures (could indicate attack attempts).

---

## Timeline

**Estimated Implementation Time:**

| Task | Time | Responsible |
|------|------|-------------|
| Code implementation | 2 hours | Developer |
| Unit testing | 1 hour | Developer |
| Integration testing | 1 hour | QA |
| Security validation | 1 hour | Security Team |
| Documentation | 30 min | Developer |
| Code review | 30 min | Senior Dev |
| **Total** | **6 hours** | |

**Deployment Schedule:**

1. **Day 1:** Implement fix in development branch
2. **Day 1:** Code review and testing
3. **Day 2:** Deploy to staging environment
4. **Day 2:** Security team validation
5. **Day 3:** Production deployment (low-traffic window)
6. **Day 3-7:** Monitor for issues

---

## Success Criteria

The fix is considered successful when:

1. ✅ All unit tests pass
2. ✅ All manual tests pass
3. ✅ Private IPs are blocked in all scenarios
4. ✅ Public URLs continue to work normally
5. ✅ No regression in existing functionality
6. ✅ Performance impact < 50ms per URL check
7. ✅ Security team approves implementation
8. ✅ Production deployment successful with no rollback

---

## Questions & Support

**For implementation questions:**
- Slack: #security-team
- Email: security@your-org.com

**For emergency security issues:**
- On-call: security-oncall@your-org.com
- Phone: +1-XXX-XXX-XXXX

---

## References

- **OWASP SSRF Prevention Cheat Sheet:** https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html
- **CWE-918: Server-Side Request Forgery (SSRF):** https://cwe.mitre.org/data/definitions/918.html
- **AWS SSRF Mitigation:** https://aws.amazon.com/blogs/security/defense-in-depth-open-firewalls-reverse-proxies-ssrf-vulnerabilities-ec2-instance-metadata-service/

---

**Document Version:** 1.0
**Created:** 2025-11-13
**Last Updated:** 2025-11-13
**Author:** Security Team
**Status:** 🔴 ACTION REQUIRED
