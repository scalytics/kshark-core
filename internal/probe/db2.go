package probe

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

// DB2Prober probes DB2 targets using the DRDA wire protocol.
type DB2Prober struct{}

func NewDB2Prober() *DB2Prober    { return &DB2Prober{} }
func (p *DB2Prober) Type() string { return "db2" }

// DRDA code points
const (
	cpEXCSAT   uint16 = 0x1041 // Exchange Server Attributes
	cpEXCSATRD uint16 = 0x1443 // Exchange Server Attributes Reply
	cpACCSEC   uint16 = 0x106D // Access Security
	cpACCSECRD uint16 = 0x14AC // Access Security Reply
	cpSECCHK   uint16 = 0x106E // Security Check
	cpSECCHKRD uint16 = 0x1219 // Security Check Reply
	cpACCRDB   uint16 = 0x2001 // Access RDB
	cpACCRDBRM uint16 = 0x2201 // Access RDB Reply
	cpRDBNFNRM uint16 = 0x2211 // RDB Not Found

	// Parameter code points
	cpSRVNAM    uint16 = 0x116D // Server Name
	cpSRVRLSLV  uint16 = 0x115A // Server Release Level
	cpEXTNAM    uint16 = 0x115E // External Name
	cpMGRLVLLS  uint16 = 0x1404 // Manager Level List
	cpSECMEC    uint16 = 0x11A2 // Security Mechanism
	cpSECCHKCD  uint16 = 0x11A4 // Security Check Code
	cpUSRID     uint16 = 0x11A0 // User ID
	cpPASSWORD  uint16 = 0x11A1 // Password
	cpRDBNAM    uint16 = 0x2110 // RDB Name
	cpPRDID     uint16 = 0x112E // Product ID
	cpTYPDEFNAM uint16 = 0x002F // Type Definition Name
	cpCRRTKN    uint16 = 0x2135 // Correlation Token
	cpRDBACSCLS uint16 = 0x210F // RDB Access Manager Class

	secUSRIDPWD uint16 = 0x0003 // User ID + Password mechanism
)

func (p *DB2Prober) Probe(ctx context.Context, target ProbeTarget) []ProbeStep {
	var steps []ProbeStep

	// L3-DNS
	dnsStep := ProbeDNS(target.Host)
	steps = append(steps, dnsStep)
	if dnsStep.Status == StatusFAIL {
		return steps
	}

	// L4-TCP
	addr := fmt.Sprintf("%s:%d", target.Host, target.Port)
	conn, tcpStep := ProbeTCP(addr, 8*time.Second)
	steps = append(steps, tcpStep)
	if conn == nil {
		return steps
	}
	defer conn.Close()

	// L5-6-TLS (optional)
	var rwConn net.Conn = conn
	if target.TLS {
		tlsCfg, err := BuildTLSConfig(target.Host, target.ExtraProps["sslTrustStoreLocation"], false)
		if err != nil {
			steps = append(steps, ProbeStep{
				Layer:  "L5-6-TLS",
				Status: StatusFAIL,
				Detail: fmt.Sprintf("TLS config error: %v", err),
				Hint:   "Check SSL configuration for DB2.",
			})
			return steps
		}
		tlsConn, tlsStep := ProbeTLS(conn, target.Host, tlsCfg)
		steps = append(steps, tlsStep)
		if tlsConn == nil {
			return steps
		}
		rwConn = tlsConn
	} else {
		steps = append(steps, ProbeStep{
			Layer:  "L5-6-TLS",
			Status: StatusSKIP,
			Detail: "SSL not configured",
		})
	}

	timeout := 10 * time.Second

	// L7-DRDA: EXCSAT
	start := time.Now()
	excsat := buildEXCSAT()
	resp, err := sendAndReceive(rwConn, excsat, timeout)
	excsatLatency := time.Since(start)

	if err != nil {
		steps = append(steps, ProbeStep{
			Layer:   "L7-DRDA",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("DRDA EXCSAT failed: %v", err),
			Hint:    "Server may not be a DB2 instance or port is wrong.",
			Latency: excsatLatency,
		})
		return steps
	}

	if resp.CodePoint != cpEXCSATRD {
		steps = append(steps, ProbeStep{
			Layer:   "L7-DRDA",
			Status:  StatusFAIL,
			Detail:  fmt.Sprintf("Unexpected DRDA response: 0x%04X (expected EXCSATRD 0x%04X)", resp.CodePoint, cpEXCSATRD),
			Hint:    "Server did not respond with expected DRDA protocol. Check if this is a DB2 port.",
			Latency: excsatLatency,
		})
		return steps
	}

	serverInfo := parseServerInfo(resp.Data)
	steps = append(steps, ProbeStep{
		Layer:   "L7-DRDA",
		Status:  StatusOK,
		Detail:  fmt.Sprintf("DB2 server responded: %s", serverInfo),
		Latency: excsatLatency,
	})

	// L7-Auth: ACCSEC + SECCHK
	if target.Username != "" {
		rdbName := strings.ToUpper(target.Database)

		// ACCSEC
		accsec := buildACCSEC(rdbName)
		accsecResp, err := sendAndReceive(rwConn, accsec, timeout)
		if err != nil {
			steps = append(steps, ProbeStep{
				Layer:  "L7-Auth",
				Status: StatusFAIL,
				Detail: fmt.Sprintf("DRDA ACCSEC failed: %v", err),
				Hint:   "Security negotiation failed.",
			})
			return steps
		}
		if accsecResp.CodePoint != cpACCSECRD {
			steps = append(steps, ProbeStep{
				Layer:  "L7-Auth",
				Status: StatusFAIL,
				Detail: fmt.Sprintf("Unexpected ACCSEC response: 0x%04X", accsecResp.CodePoint),
			})
			return steps
		}

		// SECCHK
		secchk := buildSECCHK(rdbName, target.Username, target.Password)
		secchkResp, err := sendAndReceive(rwConn, secchk, timeout)
		if err != nil {
			steps = append(steps, ProbeStep{
				Layer:  "L7-Auth",
				Status: StatusFAIL,
				Detail: fmt.Sprintf("DRDA SECCHK failed: %v", err),
				Hint:   "Authentication request failed.",
			})
			return steps
		}
		if secchkResp.CodePoint != cpSECCHKRD {
			steps = append(steps, ProbeStep{
				Layer:  "L7-Auth",
				Status: StatusFAIL,
				Detail: fmt.Sprintf("Unexpected SECCHK response: 0x%04X", secchkResp.CodePoint),
			})
			return steps
		}

		code, codeErr := parseSecurityCheckCode(secchkResp.Data)
		if codeErr != nil || code != 0x0000 {
			hint := "Verify connection.user and connection.password in connector config."
			detail := fmt.Sprintf("Authentication failed (SECCHKCD=0x%04X)", code)
			if code == 0x000F {
				hint = "Invalid credentials. Check if the DB2 account is locked."
			} else if code == 0x0010 {
				hint = "Password expired. User must change password before connecting."
			}
			steps = append(steps, ProbeStep{
				Layer:  "L7-Auth",
				Status: StatusFAIL,
				Detail: detail,
				Hint:   hint,
			})
			return steps
		}

		steps = append(steps, ProbeStep{
			Layer:  "L7-Auth",
			Status: StatusOK,
			Detail: fmt.Sprintf("Authenticated as '%s'", target.Username),
		})
	}

	// L7-DB: ACCRDB
	if target.Database != "" {
		rdbName := strings.ToUpper(target.Database)
		accrdb := buildACCRDB(rdbName)
		accrdbResp, err := sendAndReceive(rwConn, accrdb, timeout)
		if err != nil {
			steps = append(steps, ProbeStep{
				Layer:  "L7-DB",
				Status: StatusFAIL,
				Detail: fmt.Sprintf("DRDA ACCRDB failed: %v", err),
				Hint:   "Database access request failed.",
			})
			return steps
		}

		if accrdbResp.CodePoint == cpRDBNFNRM {
			steps = append(steps, ProbeStep{
				Layer:  "L7-DB",
				Status: StatusFAIL,
				Detail: fmt.Sprintf("Database '%s' not found", target.Database),
				Hint:   "Verify database name matches 'list db directory'.",
			})
			return steps
		}

		if accrdbResp.CodePoint == cpACCRDBRM {
			steps = append(steps, ProbeStep{
				Layer:  "L7-DB",
				Status: StatusOK,
				Detail: fmt.Sprintf("Database '%s' is accessible", target.Database),
			})
		} else {
			steps = append(steps, ProbeStep{
				Layer:  "L7-DB",
				Status: StatusFAIL,
				Detail: fmt.Sprintf("Unexpected ACCRDB response: 0x%04X", accrdbResp.CodePoint),
				Hint:   "Check database authorization. Run GRANT CONNECT ON DATABASE TO USER.",
			})
		}
	}

	return steps
}

// --- DRDA message construction ---

// buildDSSHeader constructs a 6-byte DSS header.
func buildDSSHeader(length uint16, chainBit bool, correlationID uint16) []byte {
	buf := make([]byte, 6)
	binary.BigEndian.PutUint16(buf[0:2], length) // total length including header
	buf[2] = 0xD0                                // magic byte
	if chainBit {
		buf[3] = 0x41 // request, chained
	} else {
		buf[3] = 0x01 // request, end of chain
	}
	binary.BigEndian.PutUint16(buf[4:6], correlationID)
	return buf
}

// buildCodePoint constructs a length-prefixed DRDA code point.
func buildCodePoint(cp uint16, data []byte) []byte {
	length := uint16(4 + len(data)) // 2 bytes length + 2 bytes codepoint + data
	buf := make([]byte, length)
	binary.BigEndian.PutUint16(buf[0:2], length)
	binary.BigEndian.PutUint16(buf[2:4], cp)
	copy(buf[4:], data)
	return buf
}

func buildEXCSAT() []byte {
	// Server name
	srvNam := buildCodePoint(cpSRVNAM, []byte("kshark"))
	// External name
	extNam := buildCodePoint(cpEXTNAM, []byte("kshark-probe"))
	// Manager level list (AGENT=7, SQLAM=7, RDB=7)
	mgrLvl := buildManagerLevelList()

	// EXCSAT object
	var payload bytes.Buffer
	payload.Write(srvNam)
	payload.Write(extNam)
	payload.Write(mgrLvl)

	excsat := buildCodePoint(cpEXCSAT, payload.Bytes())

	// DSS header
	totalLen := uint16(6 + len(excsat))
	header := buildDSSHeader(totalLen, false, 1)

	var msg bytes.Buffer
	msg.Write(header)
	msg.Write(excsat)
	return msg.Bytes()
}

func buildManagerLevelList() []byte {
	// Manager level list: pairs of (manager codepoint[2], level[2])
	data := make([]byte, 12)
	// AGENT manager (0x1403) level 7
	binary.BigEndian.PutUint16(data[0:2], 0x1403)
	binary.BigEndian.PutUint16(data[2:4], 7)
	// SQLAM manager (0x2407) level 7
	binary.BigEndian.PutUint16(data[4:6], 0x2407)
	binary.BigEndian.PutUint16(data[6:8], 7)
	// RDB manager (0x240F) level 7
	binary.BigEndian.PutUint16(data[8:10], 0x240F)
	binary.BigEndian.PutUint16(data[10:12], 7)
	return buildCodePoint(cpMGRLVLLS, data)
}

func buildACCSEC(rdbName string) []byte {
	// Security mechanism: USRIDPWD
	secMecData := make([]byte, 2)
	binary.BigEndian.PutUint16(secMecData, secUSRIDPWD)
	secMec := buildCodePoint(cpSECMEC, secMecData)

	// RDB name (EBCDIC, padded to 18 bytes)
	rdbNam := buildCodePoint(cpRDBNAM, encodeRDBName(rdbName))

	var payload bytes.Buffer
	payload.Write(secMec)
	payload.Write(rdbNam)

	accsec := buildCodePoint(cpACCSEC, payload.Bytes())

	totalLen := uint16(6 + len(accsec))
	header := buildDSSHeader(totalLen, false, 2)

	var msg bytes.Buffer
	msg.Write(header)
	msg.Write(accsec)
	return msg.Bytes()
}

func buildSECCHK(rdbName, username, password string) []byte {
	// Security mechanism
	secMecData := make([]byte, 2)
	binary.BigEndian.PutUint16(secMecData, secUSRIDPWD)
	secMec := buildCodePoint(cpSECMEC, secMecData)

	// RDB name
	rdbNam := buildCodePoint(cpRDBNAM, encodeRDBName(rdbName))

	// User ID
	usrID := buildCodePoint(cpUSRID, toEBCDIC([]byte(username)))

	// Password
	passwd := buildCodePoint(cpPASSWORD, toEBCDIC([]byte(password)))

	var payload bytes.Buffer
	payload.Write(secMec)
	payload.Write(rdbNam)
	payload.Write(usrID)
	payload.Write(passwd)

	secchk := buildCodePoint(cpSECCHK, payload.Bytes())

	totalLen := uint16(6 + len(secchk))
	header := buildDSSHeader(totalLen, false, 3)

	var msg bytes.Buffer
	msg.Write(header)
	msg.Write(secchk)
	return msg.Bytes()
}

func buildACCRDB(rdbName string) []byte {
	// RDB name
	rdbNam := buildCodePoint(cpRDBNAM, encodeRDBName(rdbName))

	// Product ID
	prdID := buildCodePoint(cpPRDID, []byte("KSH00010"))

	// Type definition name (for SQLAM)
	typDefNam := buildCodePoint(cpTYPDEFNAM, toEBCDIC([]byte("QTDSQLASC")))

	// Correlation token
	crrTkn := buildCodePoint(cpCRRTKN, []byte("kshark-probe"))

	// RDB access manager class
	rdbAcsCls := buildCodePoint(cpRDBACSCLS, []byte{0x00, 0x01})

	var payload bytes.Buffer
	payload.Write(rdbNam)
	payload.Write(prdID)
	payload.Write(typDefNam)
	payload.Write(crrTkn)
	payload.Write(rdbAcsCls)

	accrdb := buildCodePoint(cpACCRDB, payload.Bytes())

	totalLen := uint16(6 + len(accrdb))
	header := buildDSSHeader(totalLen, false, 4)

	var msg bytes.Buffer
	msg.Write(header)
	msg.Write(accrdb)
	return msg.Bytes()
}

// --- DRDA response parsing ---

type drdaResponse struct {
	CodePoint uint16
	Data      []byte
}

func sendAndReceive(conn net.Conn, cmd []byte, timeout time.Duration) (*drdaResponse, error) {
	conn.SetWriteDeadline(time.Now().Add(timeout))
	if _, err := conn.Write(cmd); err != nil {
		return nil, fmt.Errorf("write failed: %w", err)
	}

	conn.SetReadDeadline(time.Now().Add(timeout))
	return readDRDAResponse(conn)
}

func readDRDAResponse(conn net.Conn) (*drdaResponse, error) {
	// Read DSS header (6 bytes)
	header := make([]byte, 6)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, fmt.Errorf("failed to read DSS header: %w", err)
	}

	totalLen := binary.BigEndian.Uint16(header[0:2])
	if totalLen < 10 { // 6 header + 4 minimum code point
		return nil, fmt.Errorf("DRDA response too short: %d bytes", totalLen)
	}

	// Read the rest of the DSS
	remaining := make([]byte, totalLen-6)
	if _, err := io.ReadFull(conn, remaining); err != nil {
		return nil, fmt.Errorf("failed to read DSS body: %w", err)
	}

	// Parse the first code point in the response
	if len(remaining) < 4 {
		return nil, fmt.Errorf("response body too short")
	}

	cpLen := binary.BigEndian.Uint16(remaining[0:2])
	cp := binary.BigEndian.Uint16(remaining[2:4])

	var data []byte
	if cpLen > 4 && int(cpLen)-4 <= len(remaining)-4 {
		data = remaining[4 : cpLen-4+4]
	}

	return &drdaResponse{
		CodePoint: cp,
		Data:      data,
	}, nil
}

func parseServerInfo(data []byte) string {
	info := []string{}
	offset := 0
	for offset+4 <= len(data) {
		paramLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		paramCP := binary.BigEndian.Uint16(data[offset+2 : offset+4])

		if paramLen < 4 || offset+paramLen > len(data) {
			break
		}

		paramData := data[offset+4 : offset+paramLen]

		switch paramCP {
		case cpSRVNAM:
			info = append(info, fmt.Sprintf("server=%s", string(fromEBCDIC(paramData))))
		case cpSRVRLSLV:
			info = append(info, fmt.Sprintf("version=%s", string(fromEBCDIC(paramData))))
		}

		offset += paramLen
	}

	if len(info) == 0 {
		return "DB2 (details unavailable)"
	}
	return strings.Join(info, ", ")
}

func parseSecurityCheckCode(data []byte) (uint16, error) {
	offset := 0
	for offset+4 <= len(data) {
		paramLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		paramCP := binary.BigEndian.Uint16(data[offset+2 : offset+4])

		if paramLen < 4 || offset+paramLen > len(data) {
			break
		}

		if paramCP == cpSECCHKCD {
			if paramLen >= 6 {
				return binary.BigEndian.Uint16(data[offset+4 : offset+6]), nil
			}
		}

		offset += paramLen
	}
	return 0xFFFF, fmt.Errorf("SECCHKCD not found in response")
}

// --- EBCDIC conversion ---

// ASCII to EBCDIC conversion table (US CECP 500 / code page 37 for common chars)
var asciiToEBCDIC [256]byte

func init() {
	// Initialize with identity mapping, then override for printable ASCII
	for i := range asciiToEBCDIC {
		asciiToEBCDIC[i] = 0x3F // EBCDIC '?' as default
	}
	// Space and common punctuation
	asciiToEBCDIC[' '] = 0x40
	asciiToEBCDIC['!'] = 0x5A
	asciiToEBCDIC['"'] = 0x7F
	asciiToEBCDIC['#'] = 0x7B
	asciiToEBCDIC['$'] = 0x5B
	asciiToEBCDIC['%'] = 0x6C
	asciiToEBCDIC['&'] = 0x50
	asciiToEBCDIC['\''] = 0x7D
	asciiToEBCDIC['('] = 0x4D
	asciiToEBCDIC[')'] = 0x5D
	asciiToEBCDIC['*'] = 0x5C
	asciiToEBCDIC['+'] = 0x4E
	asciiToEBCDIC[','] = 0x6B
	asciiToEBCDIC['-'] = 0x60
	asciiToEBCDIC['.'] = 0x4B
	asciiToEBCDIC['/'] = 0x61
	// Digits 0-9
	for i := byte('0'); i <= '9'; i++ {
		asciiToEBCDIC[i] = 0xF0 + (i - '0')
	}
	asciiToEBCDIC[':'] = 0x7A
	asciiToEBCDIC[';'] = 0x5E
	asciiToEBCDIC['<'] = 0x4C
	asciiToEBCDIC['='] = 0x7E
	asciiToEBCDIC['>'] = 0x6E
	asciiToEBCDIC['?'] = 0x6F
	asciiToEBCDIC['@'] = 0x7C
	// Uppercase A-Z
	for i := byte('A'); i <= 'I'; i++ {
		asciiToEBCDIC[i] = 0xC1 + (i - 'A')
	}
	for i := byte('J'); i <= 'R'; i++ {
		asciiToEBCDIC[i] = 0xD1 + (i - 'J')
	}
	for i := byte('S'); i <= 'Z'; i++ {
		asciiToEBCDIC[i] = 0xE2 + (i - 'S')
	}
	asciiToEBCDIC['_'] = 0x6D
	// Lowercase a-z
	for i := byte('a'); i <= 'i'; i++ {
		asciiToEBCDIC[i] = 0x81 + (i - 'a')
	}
	for i := byte('j'); i <= 'r'; i++ {
		asciiToEBCDIC[i] = 0x91 + (i - 'j')
	}
	for i := byte('s'); i <= 'z'; i++ {
		asciiToEBCDIC[i] = 0xA2 + (i - 's')
	}
}

func toEBCDIC(ascii []byte) []byte {
	ebcdic := make([]byte, len(ascii))
	for i, b := range ascii {
		ebcdic[i] = asciiToEBCDIC[b]
	}
	return ebcdic
}

func fromEBCDIC(ebcdic []byte) []byte {
	// Build reverse table
	var ebcdicToASCII [256]byte
	for i := range ebcdicToASCII {
		ebcdicToASCII[i] = '?'
	}
	for ascii, ebc := range asciiToEBCDIC {
		if ebc != 0x3F || byte(ascii) == '?' {
			ebcdicToASCII[ebc] = byte(ascii)
		}
	}

	result := make([]byte, len(ebcdic))
	for i, b := range ebcdic {
		result[i] = ebcdicToASCII[b]
	}
	return result
}

// encodeRDBName encodes a database name to EBCDIC, padded to 18 bytes.
func encodeRDBName(name string) []byte {
	padded := make([]byte, 18)
	for i := range padded {
		padded[i] = ' ' // ASCII space
	}
	copy(padded, []byte(name))
	return toEBCDIC(padded)
}
