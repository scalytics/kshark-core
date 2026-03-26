package main

import (
	"testing"
)

func TestParseJaasConfig(t *testing.T) {
	tests := []struct {
		name     string
		jaas     string
		wantUser string
		wantPass string
	}{
		{
			name:     "standard PlainLoginModule format",
			jaas:     "org.apache.kafka.common.security.plain.PlainLoginModule required username='myuser' password='mypass';",
			wantUser: "myuser",
			wantPass: "mypass",
		},
		{
			name:     "double-quoted values",
			jaas:     `org.apache.kafka.common.security.plain.PlainLoginModule required username="myuser" password="mypass";`,
			wantUser: "myuser",
			wantPass: "mypass",
		},
		{
			name:     "unquoted values",
			jaas:     "org.apache.kafka.common.security.plain.PlainLoginModule required username=myuser password=mypass;",
			wantUser: "myuser",
			wantPass: "mypass",
		},
		{
			name:     "empty string",
			jaas:     "",
			wantUser: "",
			wantPass: "",
		},
		{
			name:     "missing fields",
			jaas:     "org.apache.kafka.common.security.plain.PlainLoginModule required;",
			wantUser: "",
			wantPass: "",
		},
		{
			name:     "only username present",
			jaas:     "org.apache.kafka.common.security.plain.PlainLoginModule required username='onlyuser';",
			wantUser: "onlyuser",
			wantPass: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotUser, gotPass := parseJaasConfig(tt.jaas)
			if gotUser != tt.wantUser {
				t.Errorf("parseJaasConfig() username = %q, want %q", gotUser, tt.wantUser)
			}
			if gotPass != tt.wantPass {
				t.Errorf("parseJaasConfig() password = %q, want %q", gotPass, tt.wantPass)
			}
		})
	}
}

func TestSaslCreds(t *testing.T) {
	tests := []struct {
		name     string
		props    map[string]string
		wantUser string
		wantPass string
	}{
		{
			name: "direct username and password",
			props: map[string]string{
				"sasl.username": "directuser",
				"sasl.password": "directpass",
			},
			wantUser: "directuser",
			wantPass: "directpass",
		},
		{
			name: "fallback to jaas.config",
			props: map[string]string{
				"sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username='jaasuser' password='jaaspass';",
			},
			wantUser: "jaasuser",
			wantPass: "jaaspass",
		},
		{
			name:     "empty everything",
			props:    map[string]string{},
			wantUser: "",
			wantPass: "",
		},
		{
			name: "direct creds take precedence over jaas",
			props: map[string]string{
				"sasl.username":    "directuser",
				"sasl.password":    "directpass",
				"sasl.jaas.config": "PlainLoginModule required username='jaasuser' password='jaaspass';",
			},
			wantUser: "directuser",
			wantPass: "directpass",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotUser, gotPass := saslCreds(tt.props)
			if gotUser != tt.wantUser {
				t.Errorf("saslCreds() user = %q, want %q", gotUser, tt.wantUser)
			}
			if gotPass != tt.wantPass {
				t.Errorf("saslCreds() pass = %q, want %q", gotPass, tt.wantPass)
			}
		})
	}
}

func TestSaslFromProps(t *testing.T) {
	tests := []struct {
		name     string
		props    map[string]string
		wantKind KafkaAuthKind
		wantErr  bool
	}{
		{
			name: "PLAIN mechanism",
			props: map[string]string{
				"security.protocol": "SASL_SSL",
				"sasl.mechanism":    "PLAIN",
				"sasl.username":     "user",
				"sasl.password":     "pass",
			},
			wantKind: AuthPLAIN,
		},
		{
			name: "SCRAM-SHA-256 mechanism",
			props: map[string]string{
				"security.protocol": "SASL_SSL",
				"sasl.mechanism":    "SCRAM-SHA-256",
				"sasl.username":     "user",
				"sasl.password":     "pass",
			},
			wantKind: AuthSCRAM256,
		},
		{
			name: "SCRAM-SHA-512 mechanism",
			props: map[string]string{
				"security.protocol": "SASL_SSL",
				"sasl.mechanism":    "SCRAM-SHA-512",
				"sasl.username":     "user",
				"sasl.password":     "pass",
			},
			wantKind: AuthSCRAM512,
		},
		{
			name: "GSSAPI mechanism",
			props: map[string]string{
				"security.protocol":          "SASL_SSL",
				"sasl.mechanism":             "GSSAPI",
				"sasl.kerberos.service.name": "kafka",
			},
			wantKind: AuthGSSAPI,
		},
		{
			name: "KERBEROS alias",
			props: map[string]string{
				"security.protocol": "SASL_SSL",
				"sasl.mechanism":    "KERBEROS",
			},
			wantKind: AuthGSSAPI,
		},
		{
			name: "empty mechanism with PLAINTEXT protocol",
			props: map[string]string{
				"security.protocol": "PLAINTEXT",
			},
			wantKind: AuthNone,
		},
		{
			name: "empty mechanism with SSL protocol",
			props: map[string]string{
				"security.protocol": "SSL",
			},
			wantKind: AuthNone,
		},
		{
			name:     "empty mechanism with empty protocol",
			props:    map[string]string{},
			wantKind: AuthNone,
		},
		{
			name: "missing mechanism with SASL_SSL returns error",
			props: map[string]string{
				"security.protocol": "SASL_SSL",
			},
			wantKind: AuthNone,
			wantErr:  true,
		},
		{
			name: "unsupported mechanism",
			props: map[string]string{
				"sasl.mechanism": "OAUTHBEARER",
			},
			wantKind: AuthNone,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKind, _, err := saslFromProps(tt.props)
			if (err != nil) != tt.wantErr {
				t.Fatalf("saslFromProps() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if gotKind != tt.wantKind {
				t.Errorf("saslFromProps() kind = %v, want %v", gotKind, tt.wantKind)
			}
		})
	}
}

func TestDialerFromProps_PLAINTEXT(t *testing.T) {
	props := map[string]string{
		"security.protocol": "PLAINTEXT",
	}
	d, desc, err := dialerFromProps(props, "broker.example.com")
	if err != nil {
		t.Fatalf("dialerFromProps() error = %v", err)
	}
	if d == nil {
		t.Fatal("dialer is nil")
	}
	if d.TLS != nil {
		t.Error("expected nil TLS for PLAINTEXT")
	}
	if d.SASLMechanism != nil {
		t.Error("expected nil SASL for PLAINTEXT")
	}
	_ = desc
}

func TestDialerFromProps_SASL_SSL_PLAIN(t *testing.T) {
	props := map[string]string{
		"security.protocol": "SASL_SSL",
		"sasl.mechanism":    "PLAIN",
		"sasl.username":     "user",
		"sasl.password":     "pass",
	}
	d, _, err := dialerFromProps(props, "broker.example.com")
	if err != nil {
		t.Fatalf("dialerFromProps() error = %v", err)
	}
	if d.TLS == nil {
		t.Error("expected non-nil TLS for SASL_SSL")
	}
	if d.SASLMechanism == nil {
		t.Error("expected non-nil SASL mechanism for PLAIN")
	}
}

func TestDialerFromProps_SASL_SSL_SCRAM256(t *testing.T) {
	props := map[string]string{
		"security.protocol": "SASL_SSL",
		"sasl.mechanism":    "SCRAM-SHA-256",
		"sasl.username":     "user",
		"sasl.password":     "pass",
	}
	d, _, err := dialerFromProps(props, "broker.example.com")
	if err != nil {
		t.Fatalf("dialerFromProps() error = %v", err)
	}
	if d.TLS == nil {
		t.Error("expected non-nil TLS")
	}
	if d.SASLMechanism == nil {
		t.Error("expected non-nil SASL mechanism for SCRAM-SHA-256")
	}
}

func TestTransportFromProps_PLAINTEXT(t *testing.T) {
	props := map[string]string{
		"security.protocol": "PLAINTEXT",
	}
	tr, err := transportFromProps(props, 10000000000)
	if err != nil {
		t.Fatalf("transportFromProps() error = %v", err)
	}
	if tr == nil {
		t.Fatal("transport is nil")
	}
	if tr.TLS != nil {
		t.Error("expected nil TLS for PLAINTEXT")
	}
}

func TestTransportFromProps_SASL_SSL(t *testing.T) {
	props := map[string]string{
		"security.protocol": "SASL_SSL",
		"sasl.mechanism":    "PLAIN",
		"sasl.username":     "user",
		"sasl.password":     "pass",
	}
	tr, err := transportFromProps(props, 10000000000)
	if err != nil {
		t.Fatalf("transportFromProps() error = %v", err)
	}
	if tr == nil {
		t.Fatal("transport is nil")
	}
	if tr.TLS == nil {
		t.Error("expected non-nil TLS for SASL_SSL")
	}
	if tr.SASL == nil {
		t.Error("expected non-nil SASL for SASL_SSL/PLAIN")
	}
}
