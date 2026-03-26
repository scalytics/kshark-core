package connectapi

import "testing"

func FuzzParseJDBCURL(f *testing.F) {
	f.Add("jdbc:db2://host:50000/DB")
	f.Add("jdbc:postgresql://host:5432/db?sslmode=require")
	f.Add("jdbc:mysql://host/db")
	f.Add("jdbc:postgresql://host:5432/db?user=u&password=p")
	f.Add("")
	f.Add("not-a-jdbc-url")
	f.Add("jdbc:")
	f.Add("jdbc:db2://")
	f.Add("jdbc:postgresql://host:notaport/db")
	f.Fuzz(func(t *testing.T, input string) {
		ParseJDBCURL(input) // must not panic
	})
}
