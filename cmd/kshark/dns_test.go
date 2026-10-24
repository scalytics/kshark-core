package main

import (
	"context"
	"net"
	"strings"
)

func init() {
	resolve := lookupHost
	lookupHost = func(ctx context.Context, host string) ([]string, error) {
		if strings.HasSuffix(strings.TrimSuffix(host, "."), ".invalid") {
			return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
		}
		return resolve(ctx, host)
	}
}
