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

// Entry is one record stored in the log.
type Entry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

// WAL is an append-only file of raft log entries.
type WAL struct {
	mu   sync.Mutex
	path string
	f    *os.File
	w    *bufio.Writer
}

// Open opens or creates the log file at path.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{path: path, f: f, w: bufio.NewWriter(f)}, nil
}

// Dir returns the directory that holds the log file.
func (w *WAL) Dir() string {
	return filepath.Dir(w.path)
}

// Append buffers entries for writing.
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

// Sync flushes buffered records and forces them to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// Rewrite replaces the whole log with entries. It is used when a conflicting
// suffix is removed from the log, since an append-only file cannot truncate.
func (w *WAL) Rewrite(entries []Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w.w.Reset(w.f)
	for _, e := range entries {
		if _, err := w.w.Write(encodeRecord(e.Term, e.Index, e.Data)); err != nil {
			return err
		}
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// ReadAll reads every valid record from the start of the file. It stops at
// the first damaged record, because a crash can leave a half-written tail.
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

// Close flushes and closes the log file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}

// readRecords decodes records until the file ends or a record is broken.
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
