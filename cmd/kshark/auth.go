// Copyright 2024-2026 Scalytics GmbH and kshark Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// ---------- Auth builders ----------

type KafkaAuthKind int

const (
	AuthNone KafkaAuthKind = iota
	AuthPLAIN
	AuthSCRAM256
	AuthSCRAM512
	AuthGSSAPI // optional
)

func parseJaasConfig(jaas string) (username, password string) {
	// Parse: ...PlainLoginModule required username='...' password='...';
	for _, part := range strings.Fields(jaas) {
		if strings.HasPrefix(part, "username=") {
			username = strings.Trim(strings.TrimPrefix(part, "username="), "\"';")
		} else if strings.HasPrefix(part, "password=") {
			password = strings.Trim(strings.TrimPrefix(part, "password="), "\"';")
		}
	}
	return
}

func saslCreds(p map[string]string) (string, string) {
	user, pass := p["sasl.username"], p["sasl.password"]
	if user == "" && pass == "" {
		if jaas := p["sasl.jaas.config"]; jaas != "" {
			user, pass = parseJaasConfig(jaas)
		}
	}
	return user, pass
}

func saslFromProps(p map[string]string) (KafkaAuthKind, map[string]string, error) {
	secProto := strings.ToUpper(p["security.protocol"])
	mech := strings.ToUpper(p["sasl.mechanism"])

	switch mech {
	case "PLAIN":
		user, pass := saslCreds(p)
		return AuthPLAIN, map[string]string{
			"username": user,
			"password": pass,
		}, nil
	case "SCRAM-SHA-256":
		user, pass := saslCreds(p)
		return AuthSCRAM256, map[string]string{
			"username": user,
			"password": pass,
		}, nil
	case "SCRAM-SHA-512":
		user, pass := saslCreds(p)
		return AuthSCRAM512, map[string]string{
			"username": user,
			"password": pass,
		}, nil
	case "GSSAPI", "KERBEROS":
		return AuthGSSAPI, map[string]string{
			"service.name": p["sasl.kerberos.service.name"], // defaults "kafka"
			"principal":    p["sasl.kerberos.principal"],    // optional
			"realm":        p["sasl.kerberos.realm"],        // optional
		}, nil
	case "":
		if secProto == "SSL" || secProto == "PLAINTEXT" || secProto == "" {
			return AuthNone, nil, nil
		}
		return AuthNone, nil, fmt.Errorf("missing sasl.mechanism for security.protocol=%s", secProto)
	default:
		return AuthNone, nil, fmt.Errorf("unsupported sasl.mechanism: %s", mech)
	}
}

func dialerFromProps(p map[string]string, hostForSNI string) (*kafka.Dialer, string, error) {
	tlsConf, tlsDesc, err := tlsConfigFromProps(p, hostForSNI)
	if err != nil {
		return nil, "", err
	}

	kind, kv, err := saslFromProps(p)
	if err != nil {
		return nil, "", err
	}

	var mech sasl.Mechanism
	switch kind {
	case AuthPLAIN:
		mech = plain.Mechanism{Username: kv["username"], Password: kv["password"]}
	case AuthSCRAM256:
		m, e := scram.Mechanism(scram.SHA256, kv["username"], kv["password"])
		if e != nil {
			return nil, "", e
		}
		mech = m
	case AuthSCRAM512:
		m, e := scram.Mechanism(scram.SHA512, kv["username"], kv["password"])
		if e != nil {
			return nil, "", e
		}
		mech = m
	case AuthGSSAPI:
		// Build with -tags kerberos and wire gssapi mechanism here.
		// If not available, we mark SKIP later during probe and provide hints.
	}

	d := &kafka.Dialer{
		Timeout:       8 * time.Second,
		DualStack:     true,
		TLS:           tlsConf,
		SASLMechanism: mech,
	}
	return d, tlsDesc, nil
}

// transportFromProps builds a kafka.Transport with TLS and SASL configured
// directly. Unlike dialerFromProps (which returns a kafka.Dialer suitable for
// kafka.Conn), this creates a Transport where TLS ServerName is auto-derived
// per broker, so connections to leader brokers use the correct SNI.
func transportFromProps(p map[string]string, timeout time.Duration) (*kafka.Transport, error) {
	// Empty ServerName — the Transport auto-fills it from the target broker
	// address, ensuring correct SNI for each broker on Confluent Cloud.
	tlsConf, _, err := tlsConfigFromProps(p, "")
	if err != nil {
		return nil, fmt.Errorf("tls config: %w", err)
	}

	kind, kv, err := saslFromProps(p)
	if err != nil {
		return nil, fmt.Errorf("sasl config: %w", err)
	}

	var mech sasl.Mechanism
	switch kind {
	case AuthPLAIN:
		mech = plain.Mechanism{Username: kv["username"], Password: kv["password"]}
	case AuthSCRAM256:
		m, e := scram.Mechanism(scram.SHA256, kv["username"], kv["password"])
		if e != nil {
			return nil, e
		}
		mech = m
	case AuthSCRAM512:
		m, e := scram.Mechanism(scram.SHA512, kv["username"], kv["password"])
		if e != nil {
			return nil, e
		}
		mech = m
	}

	return &kafka.Transport{
		TLS:         tlsConf,
		SASL:        mech,
		DialTimeout: timeout,
	}, nil
}
