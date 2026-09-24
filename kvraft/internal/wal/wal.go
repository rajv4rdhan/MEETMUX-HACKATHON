// Package wal is a small append-only write-ahead log for raft entries.
package wal

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type Entry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

type WAL struct {
	mu   sync.Mutex
	path string
	f    *os.File
	w    *bufio.Writer
}

func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{path: path, f: f, w: bufio.NewWriter(f)}, nil
}

func (w *WAL) Dir() string {
	return filepath.Dir(w.path)
}

func (w *WAL) Append(entries []Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range entries {
		if _, err := w.w.Write(encodeRecord(e.Term, e.Index, e.Data)); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *WAL) ReadAll() ([]Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return nil, err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return readRecords(w.f)
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}

// readRecords stops at the first damaged record, because a crash can leave a
// half-written tail.
func readRecords(r io.Reader) ([]Entry, error) {
	br := bufio.NewReader(r)
	var entries []Entry
	for {
		var lengthBuf [4]byte
		if _, err := io.ReadFull(br, lengthBuf[:]); err != nil {
			return entries, nil
		}
		length := binary.BigEndian.Uint32(lengthBuf[:])
		if length < crcSize+headerSize {
			return entries, nil
		}

		body := make([]byte, length)
		if _, err := io.ReadFull(br, body); err != nil {
			return entries, nil
		}

		crc := binary.BigEndian.Uint32(body[0:4])
		payload := body[4:]
		if crc32.ChecksumIEEE(payload) != crc {
			return entries, nil
		}

		entries = append(entries, Entry{
			Term:  binary.BigEndian.Uint64(payload[0:8]),
			Index: binary.BigEndian.Uint64(payload[8:16]),
			Data:  append([]byte(nil), payload[16:]...),
		})
	}
}
