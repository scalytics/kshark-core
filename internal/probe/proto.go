package probe

import (
	"encoding/binary"
	"fmt"
	"io"
)

// safeRead reads exactly n bytes from r, returning an error on short read.
func safeRead(r io.Reader, n int) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("negative read size: %d", n)
	}
	if n == 0 {
		return nil, nil
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

// readUint16BE reads a 2-byte big-endian uint16 from r.
func readUint16BE(r io.Reader) (uint16, error) {
	var buf [2]byte
	_, err := io.ReadFull(r, buf[:])
	return binary.BigEndian.Uint16(buf[:]), err
}

// readUint16LE reads a 2-byte little-endian uint16 from r.
func readUint16LE(r io.Reader) (uint16, error) {
	var buf [2]byte
	_, err := io.ReadFull(r, buf[:])
	return binary.LittleEndian.Uint16(buf[:]), err
}

// readUint32LE reads a 4-byte little-endian uint32 from r.
func readUint32LE(r io.Reader) (uint32, error) {
	var buf [4]byte
	_, err := io.ReadFull(r, buf[:])
	return binary.LittleEndian.Uint32(buf[:]), err
}
