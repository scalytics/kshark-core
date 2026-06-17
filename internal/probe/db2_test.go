package probe

import (
	"encoding/binary"
	"testing"
)

func TestBuildEXCSAT(t *testing.T) {
	msg := buildEXCSAT()

	if len(msg) < 10 {
		t.Fatalf("EXCSAT message too short: %d bytes", len(msg))
	}

	// Check DSS header
	totalLen := binary.BigEndian.Uint16(msg[0:2])
	if int(totalLen) != len(msg) {
		t.Errorf("DSS length = %d, but message is %d bytes", totalLen, len(msg))
	}

	// Magic byte
	if msg[2] != 0xD0 {
		t.Errorf("magic byte = 0x%02X, want 0xD0", msg[2])
	}

	// Check EXCSAT code point in body
	cpLen := binary.BigEndian.Uint16(msg[6:8])
	cp := binary.BigEndian.Uint16(msg[8:10])
	if cp != cpEXCSAT {
		t.Errorf("code point = 0x%04X, want 0x%04X (EXCSAT)", cp, cpEXCSAT)
	}
	if int(cpLen) != len(msg)-6 {
		t.Errorf("code point length = %d, want %d", cpLen, len(msg)-6)
	}
}

func TestBuildACCSEC(t *testing.T) {
	msg := buildACCSEC("TESTDB")

	if len(msg) < 10 {
		t.Fatalf("ACCSEC message too short: %d bytes", len(msg))
	}

	// Check DSS header
	totalLen := binary.BigEndian.Uint16(msg[0:2])
	if int(totalLen) != len(msg) {
		t.Errorf("DSS length = %d, but message is %d bytes", totalLen, len(msg))
	}

	// Check ACCSEC code point
	cp := binary.BigEndian.Uint16(msg[8:10])
	if cp != cpACCSEC {
		t.Errorf("code point = 0x%04X, want 0x%04X (ACCSEC)", cp, cpACCSEC)
	}
}

func TestBuildSECCHK(t *testing.T) {
	msg := buildSECCHK("TESTDB", "admin", "password123")

	if len(msg) < 10 {
		t.Fatalf("SECCHK message too short: %d bytes", len(msg))
	}

	// Check DSS header
	totalLen := binary.BigEndian.Uint16(msg[0:2])
	if int(totalLen) != len(msg) {
		t.Errorf("DSS length = %d, but message is %d bytes", totalLen, len(msg))
	}

	// Check SECCHK code point
	cp := binary.BigEndian.Uint16(msg[8:10])
	if cp != cpSECCHK {
		t.Errorf("code point = 0x%04X, want 0x%04X (SECCHK)", cp, cpSECCHK)
	}
}

func TestBuildACCRDB(t *testing.T) {
	msg := buildACCRDB("PRODDB")

	if len(msg) < 10 {
		t.Fatalf("ACCRDB message too short: %d bytes", len(msg))
	}

	// Check DSS header
	totalLen := binary.BigEndian.Uint16(msg[0:2])
	if int(totalLen) != len(msg) {
		t.Errorf("DSS length = %d, but message is %d bytes", totalLen, len(msg))
	}

	// Check ACCRDB code point
	cp := binary.BigEndian.Uint16(msg[8:10])
	if cp != cpACCRDB {
		t.Errorf("code point = 0x%04X, want 0x%04X (ACCRDB)", cp, cpACCRDB)
	}
}

func TestEncodeRDBName(t *testing.T) {
	result := encodeRDBName("TESTDB")

	if len(result) != 18 {
		t.Errorf("RDBNAM length = %d, want 18", len(result))
	}

	// First 6 bytes should be EBCDIC for "TESTDB"
	expected := toEBCDIC([]byte("TESTDB"))
	for i := 0; i < 6; i++ {
		if result[i] != expected[i] {
			t.Errorf("byte %d = 0x%02X, want 0x%02X", i, result[i], expected[i])
		}
	}

	// Remaining bytes should be EBCDIC space (0x40)
	for i := 6; i < 18; i++ {
		if result[i] != 0x40 {
			t.Errorf("padding byte %d = 0x%02X, want 0x40 (EBCDIC space)", i, result[i])
		}
	}
}

func TestEBCDICRoundTrip(t *testing.T) {
	input := "HELLO WORLD 123"
	encoded := toEBCDIC([]byte(input))
	decoded := string(fromEBCDIC(encoded))

	if decoded != input {
		t.Errorf("EBCDIC round trip failed: got %q, want %q", decoded, input)
	}
}

func TestParseSecurityCheckCode(t *testing.T) {
	// Build a mock SECCHKRD response data with SECCHKCD = 0x0000 (success)
	data := make([]byte, 6)
	binary.BigEndian.PutUint16(data[0:2], 6)          // length
	binary.BigEndian.PutUint16(data[2:4], cpSECCHKCD) // code point
	binary.BigEndian.PutUint16(data[4:6], 0x0000)     // success

	code, err := parseSecurityCheckCode(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != 0x0000 {
		t.Errorf("code = 0x%04X, want 0x0000", code)
	}
}

func TestParseSecurityCheckCode_Failure(t *testing.T) {
	data := make([]byte, 6)
	binary.BigEndian.PutUint16(data[0:2], 6)
	binary.BigEndian.PutUint16(data[2:4], cpSECCHKCD)
	binary.BigEndian.PutUint16(data[4:6], 0x000F) // invalid credentials

	code, err := parseSecurityCheckCode(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != 0x000F {
		t.Errorf("code = 0x%04X, want 0x000F", code)
	}
}

func TestBuildCodePoint(t *testing.T) {
	data := []byte{0x01, 0x02}
	cp := buildCodePoint(0x1234, data)

	if len(cp) != 6 {
		t.Fatalf("code point length = %d, want 6", len(cp))
	}

	// Length field
	length := binary.BigEndian.Uint16(cp[0:2])
	if length != 6 {
		t.Errorf("length = %d, want 6", length)
	}

	// Code point value
	cpVal := binary.BigEndian.Uint16(cp[2:4])
	if cpVal != 0x1234 {
		t.Errorf("code point = 0x%04X, want 0x1234", cpVal)
	}

	// Data
	if cp[4] != 0x01 || cp[5] != 0x02 {
		t.Errorf("data = %v, want [0x01, 0x02]", cp[4:6])
	}
}
