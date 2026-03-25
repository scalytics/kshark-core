package connectapi

import (
	"testing"
)

func TestParseJDBCURL_DB2(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		wantHost string
		wantPort int
		wantDB   string
		wantProp string
		wantVal  string
		wantErr  bool
	}{
		{
			name:     "basic DB2 URL",
			url:      "jdbc:db2://db2host.internal:50000/PRODDB",
			wantHost: "db2host.internal",
			wantPort: 50000,
			wantDB:   "PRODDB",
		},
		{
			name:     "DB2 default port",
			url:      "jdbc:db2://db2host/MYDB",
			wantHost: "db2host",
			wantPort: 50000,
			wantDB:   "MYDB",
		},
		{
			name:     "DB2 with SSL properties",
			url:      "jdbc:db2://db2host.internal:50000/PRODDB:sslConnection=true;currentSchema=MYSCHEMA;",
			wantHost: "db2host.internal",
			wantPort: 50000,
			wantDB:   "PRODDB",
			wantProp: "sslConnection",
			wantVal:  "true",
		},
		{
			name:    "missing database",
			url:     "jdbc:db2://host",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParseJDBCURL(tt.url)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if parsed.Dialect != "db2" {
				t.Errorf("dialect = %q, want db2", parsed.Dialect)
			}
			if parsed.Host != tt.wantHost {
				t.Errorf("host = %q, want %q", parsed.Host, tt.wantHost)
			}
			if parsed.Port != tt.wantPort {
				t.Errorf("port = %d, want %d", parsed.Port, tt.wantPort)
			}
			if parsed.Database != tt.wantDB {
				t.Errorf("database = %q, want %q", parsed.Database, tt.wantDB)
			}
			if tt.wantProp != "" {
				if parsed.Props[tt.wantProp] != tt.wantVal {
					t.Errorf("prop %q = %q, want %q", tt.wantProp, parsed.Props[tt.wantProp], tt.wantVal)
				}
			}
		})
	}
}

func TestParseJDBCURL_PostgreSQL(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		wantHost string
		wantPort int
		wantDB   string
		wantProp string
		wantVal  string
	}{
		{
			name:     "basic PostgreSQL URL",
			url:      "jdbc:postgresql://pghost:5432/appdb",
			wantHost: "pghost",
			wantPort: 5432,
			wantDB:   "appdb",
		},
		{
			name:     "PostgreSQL default port",
			url:      "jdbc:postgresql://pghost/mydb",
			wantHost: "pghost",
			wantPort: 5432,
			wantDB:   "mydb",
		},
		{
			name:     "PostgreSQL with sslmode",
			url:      "jdbc:postgresql://pghost:5432/appdb?sslmode=require",
			wantHost: "pghost",
			wantPort: 5432,
			wantDB:   "appdb",
			wantProp: "sslmode",
			wantVal:  "require",
		},
		{
			name:     "PostgreSQL with multiple params",
			url:      "jdbc:postgresql://pghost:5432/appdb?sslmode=verify-full&sslrootcert=/path/ca.pem",
			wantHost: "pghost",
			wantPort: 5432,
			wantDB:   "appdb",
			wantProp: "sslrootcert",
			wantVal:  "/path/ca.pem",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParseJDBCURL(tt.url)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if parsed.Dialect != "postgresql" {
				t.Errorf("dialect = %q, want postgresql", parsed.Dialect)
			}
			if parsed.Host != tt.wantHost {
				t.Errorf("host = %q, want %q", parsed.Host, tt.wantHost)
			}
			if parsed.Port != tt.wantPort {
				t.Errorf("port = %d, want %d", parsed.Port, tt.wantPort)
			}
			if parsed.Database != tt.wantDB {
				t.Errorf("database = %q, want %q", parsed.Database, tt.wantDB)
			}
			if tt.wantProp != "" {
				if parsed.Props[tt.wantProp] != tt.wantVal {
					t.Errorf("prop %q = %q, want %q", tt.wantProp, parsed.Props[tt.wantProp], tt.wantVal)
				}
			}
		})
	}
}

func TestParseJDBCURL_Unsupported(t *testing.T) {
	_, err := ParseJDBCURL("jdbc:mysql://host/db")
	if err == nil {
		t.Fatal("expected error for unsupported dialect")
	}
}

func TestParseJDBCURL_NotJDBC(t *testing.T) {
	_, err := ParseJDBCURL("http://host/db")
	if err == nil {
		t.Fatal("expected error for non-JDBC URL")
	}
}
