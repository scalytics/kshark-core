package main

import "testing"

func FuzzParseJaasConfig(f *testing.F) {
	f.Add("org.apache.kafka.common.security.plain.PlainLoginModule required username='user' password='pass';")
	f.Add("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"pass\";")
	f.Add("")
	f.Add("username='only-user'")
	f.Add("password='only-pass'")
	f.Add("random garbage input")
	f.Add("username=unquoted password=unquoted")
	f.Fuzz(func(t *testing.T, input string) {
		parseJaasConfig(input) // must not panic
	})
}
