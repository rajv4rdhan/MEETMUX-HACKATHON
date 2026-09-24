package wal

import (
	"encoding/binary"
	"hash/crc32"
)

// A record on disk looks like:
//
//	length(4) | crc32(4) | term(8) | index(8) | data
//
// length counts every byte after the length field. The checksum covers the
// term, index and data.
const crcSize = 4
const headerSize = 8 + 8 // term + index

// encodeRecord returns the on-disk bytes for one entry.
func encodeRecord(term, index uint64, data []byte) []byte {
	payload := make([]byte, headerSize+len(data))
	binary.BigEndian.PutUint64(payload[0:8], term)
	binary.BigEndian.PutUint64(payload[8:16], index)
	copy(payload[16:], data)

	body := make([]byte, crcSize+len(payload))
	binary.BigEndian.PutUint32(body[0:4], crc32.ChecksumIEEE(payload))
	copy(body[4:], payload)

	buf := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(body)))
	copy(buf[4:], body)
	return buf
}
