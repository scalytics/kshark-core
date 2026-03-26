package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRedactArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "no sensitive flags",
			args: []string{"-bootstrap", "broker:9092", "-topic", "test"},
			want: []string{"-bootstrap", "broker:9092", "-topic", "test"},
		},
		{
			name: "connect-basic-auth redacted",
			args: []string{"-connect-basic-auth", "user:pass", "-topic", "test"},
			want: []string{"-connect-basic-auth", "[REDACTED]", "-topic", "test"},
		},
		{
			name: "connect-bearer-token redacted",
			args: []string{"-connect-bearer-token", "eyJhbGciOi...", "-topic", "test"},
			want: []string{"-connect-bearer-token", "[REDACTED]", "-topic", "test"},
		},
		{
			name: "both sensitive flags redacted",
			args: []string{"-connect-basic-auth", "creds", "-connect-bearer-token", "token"},
			want: []string{"-connect-basic-auth", "[REDACTED]", "-connect-bearer-token", "[REDACTED]"},
		},
		{
			name: "flag at end without value is not touched",
			args: []string{"-topic", "test", "-connect-basic-auth"},
			want: []string{"-topic", "test", "-connect-basic-auth"},
		},
		{
			name: "empty args",
			args: []string{},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := redactArgs(tt.args)
			if len(got) != len(tt.want) {
				t.Fatalf("redactArgs() length = %d, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("redactArgs()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestRedactArgs_DoesNotMutateOriginal(t *testing.T) {
	original := []string{"-connect-basic-auth", "secret"}
	originalCopy := make([]string, len(original))
	copy(originalCopy, original)

	redacted := redactArgs(original)

	// Verify original is not mutated
	for i := range original {
		if original[i] != originalCopy[i] {
			t.Errorf("original[%d] was mutated: got %q, want %q", i, original[i], originalCopy[i])
		}
	}

	// Verify redacted version is correct
	if redacted[1] != "[REDACTED]" {
		t.Errorf("redacted[1] = %q, want [REDACTED]", redacted[1])
	}
}

func TestInitScanLog_EmptyPath(t *testing.T) {
	f, err := initScanLog("", "text")
	if err != nil {
		t.Fatalf("initScanLog(\"\") error = %v", err)
	}
	if f != nil {
		f.Close()
		t.Error("initScanLog(\"\") returned non-nil file, want nil")
	}
}

func TestInitScanLog_ValidPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	f, err := initScanLog(path, "text")
	if err != nil {
		t.Fatalf("initScanLog(%q) error = %v", path, err)
	}
	if f == nil {
		t.Fatal("initScanLog() returned nil file")
	}
	defer f.Close()

	// Verify the file was created
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("log file was not created at %s", path)
	}
}

func TestInitScanLog_JSONFormat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	f, err := initScanLog(path, "json")
	if err != nil {
		t.Fatalf("initScanLog(%q, json) error = %v", path, err)
	}
	if f == nil {
		t.Fatal("initScanLog() returned nil file")
	}
	f.Close()
}

func TestInitScanLog_InvalidPath(t *testing.T) {
	// Use an impossible path
	_, err := initScanLog("/nonexistent/deeply/nested/dir/test.log", "text")
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
}

func TestParamMeta_Provided(t *testing.T) {
	pm := paramMeta("myvalue", "default", true)
	if pm.Value != "myvalue" {
		t.Errorf("Value = %q, want %q", pm.Value, "myvalue")
	}
	if pm.Default != "default" {
		t.Errorf("Default = %q, want %q", pm.Default, "default")
	}
	if pm.IsDefault != false {
		t.Errorf("IsDefault = %v, want false", pm.IsDefault)
	}
	if pm.Provided != true {
		t.Errorf("Provided = %v, want true", pm.Provided)
	}
}

func TestParamMeta_NotProvided(t *testing.T) {
	pm := paramMeta("default", "default", false)
	if pm.Value != "default" {
		t.Errorf("Value = %q, want %q", pm.Value, "default")
	}
	if pm.IsDefault != true {
		t.Errorf("IsDefault = %v, want true", pm.IsDefault)
	}
	if pm.Provided != false {
		t.Errorf("Provided = %v, want false", pm.Provided)
	}
}

func TestFileSHA256_KnownContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")
	content := []byte("hello world")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}

	got, err := fileSHA256(path)
	if err != nil {
		t.Fatalf("fileSHA256() error = %v", err)
	}

	expected := sha256.Sum256(content)
	want := hex.EncodeToString(expected[:])
	if got != want {
		t.Errorf("fileSHA256() = %q, want %q", got, want)
	}
}

func TestFileSHA256_MissingFile(t *testing.T) {
	_, err := fileSHA256("/nonexistent/file.txt")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestReadFileContent_Success(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")
	content := "hello, kshark!"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	got, err := readFileContent(path)
	if err != nil {
		t.Fatalf("readFileContent() error = %v", err)
	}
	if got != content {
		t.Errorf("readFileContent() = %q, want %q", got, content)
	}
}

func TestReadFileContent_MissingFile(t *testing.T) {
	_, err := readFileContent("/nonexistent/file.txt")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestWriteReportChecksum_Success(t *testing.T) {
	dir := t.TempDir()
	reportPath := filepath.Join(dir, "report.json")
	reportContent := `{"rows":[]}`
	if err := os.WriteFile(reportPath, []byte(reportContent), 0644); err != nil {
		t.Fatal(err)
	}

	checksumPath, hash, err := writeReportChecksum(reportPath)
	if err != nil {
		t.Fatalf("writeReportChecksum() error = %v", err)
	}
	if checksumPath == "" {
		t.Fatal("checksumPath is empty")
	}
	if hash == "" {
		t.Fatal("hash is empty")
	}

	// Verify checksum file exists
	data, err := os.ReadFile(checksumPath)
	if err != nil {
		t.Fatalf("could not read checksum file: %v", err)
	}
	checksumContent := string(data)
	if !strings.Contains(checksumContent, hash) {
		t.Errorf("checksum file does not contain hash %q", hash)
	}
	if !strings.Contains(checksumContent, reportPath) {
		t.Errorf("checksum file does not contain report path %q", reportPath)
	}

	// Verify the hash matches the actual file hash
	expectedHash, _ := fileSHA256(reportPath)
	if hash != expectedHash {
		t.Errorf("hash = %q, want %q", hash, expectedHash)
	}
}

func TestWriteReportChecksum_EmptyPath(t *testing.T) {
	_, _, err := writeReportChecksum("")
	if err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestWriteReportChecksum_MissingFile(t *testing.T) {
	_, _, err := writeReportChecksum("/nonexistent/report.json")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}
