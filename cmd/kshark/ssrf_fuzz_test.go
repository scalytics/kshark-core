package main

import "testing"

func FuzzIsAllowedURL(f *testing.F) {
	f.Add("https://example.com:8081/subjects")
	f.Add("http://127.0.0.1:8081")
	f.Add("ftp://evil.com")
	f.Add("http://169.254.169.254/latest/meta-data/")
	f.Add("https://10.0.0.5:8081/subjects")
	f.Add("")
	f.Add("://missing-scheme")
	f.Add("http://user:pass@host:8081")
	f.Add("http://[::1]:8081")
	f.Fuzz(func(t *testing.T, input string) {
		isAllowedURL(input) // must not panic
	})
}
