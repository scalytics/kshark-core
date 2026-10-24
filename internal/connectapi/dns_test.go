package connectapi

import (
	"net"
	"strings"
)

func init() {
	resolve := lookupHost
	lookupHost = func(host string) ([]string, error) {
		if strings.HasSuffix(strings.TrimSuffix(host, "."), ".invalid") {
			return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
		}
		return resolve(host)
	}
}
