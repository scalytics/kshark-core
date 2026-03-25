package main

import (
	"net"
	"testing"
)

func TestClassifyIP(t *testing.T) {
	tests := []struct {
		name string
		ip   string
		want ssrfVerdict
	}{
		// DENY (always blocked)
		{"loopback v4", "127.0.0.1", ssrfDeny},
		{"loopback v4 high", "127.255.255.254", ssrfDeny},
		{"loopback v6", "::1", ssrfDeny},
		{"link-local", "169.254.1.1", ssrfDeny},
		{"cloud metadata", "169.254.169.254", ssrfDeny},
		{"link-local v6", "fe80::1", ssrfDeny},
		{"CGN", "100.64.0.1", ssrfDeny},
		{"TEST-NET-1", "192.0.2.1", ssrfDeny},
		{"multicast", "224.0.0.1", ssrfDeny},
		{"reserved", "240.0.0.1", ssrfDeny},
		{"broadcast", "255.255.255.255", ssrfDeny},

		// WARN (RFC1918 — allowed for PrivateLink)
		{"10/8", "10.0.0.1", ssrfWarn},
		{"172.16/12", "172.16.0.1", ssrfWarn},
		{"192.168/16", "192.168.1.1", ssrfWarn},
		{"ULA v6", "fc00::1", ssrfWarn},

		// ALLOW (public)
		{"google dns", "8.8.8.8", ssrfAllow},
		{"cloudflare", "1.1.1.1", ssrfAllow},
		{"public v6", "2001:4860:4860::8888", ssrfAllow},
		{"172.32 outside /12", "172.32.0.1", ssrfAllow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP(tt.ip)
			if ip == nil {
				t.Fatalf("bad test IP: %s", tt.ip)
			}
			got := classifyIP(ip)
			if got != tt.want {
				t.Errorf("classifyIP(%s) = %d, want %d", tt.ip, got, tt.want)
			}
		})
	}
}

func TestIsAllowedURL_Blocked(t *testing.T) {
	blocked := []struct {
		name string
		url  string
	}{
		{"loopback", "http://127.0.0.1:8081"},
		{"cloud metadata", "http://169.254.169.254/latest/meta-data/"},
		{"ftp scheme", "ftp://example.com/subjects"},
		{"embedded creds", "http://user:pass@registry.example.com:8081"},
	}

	for _, tt := range blocked {
		t.Run(tt.name, func(t *testing.T) {
			_, err := isAllowedURL(tt.url)
			if err == nil {
				t.Errorf("isAllowedURL(%q) should have been blocked", tt.url)
			}
		})
	}
}

func TestIsAllowedURL_RFC1918_Warns(t *testing.T) {
	warned := []string{
		"https://10.0.0.5:8081/subjects",
		"https://172.16.0.1:8081/subjects",
		"https://192.168.1.100:8081/subjects",
	}

	for _, u := range warned {
		t.Run(u, func(t *testing.T) {
			warning, err := isAllowedURL(u)
			if err != nil {
				t.Errorf("isAllowedURL(%q) unexpected error: %v", u, err)
			}
			if warning == "" {
				t.Errorf("isAllowedURL(%q) should have returned a warning", u)
			}
		})
	}
}
