package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendReadBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	want := []Entry{
		{Term: 1, Index: 1, Data: []byte("set a 1")},
		{Term: 1, Index: 2, Data: []byte("set b 2")},
		{Term: 2, Index: 3, Data: []byte("del a")},
	}

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(want); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	got, err := w2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("ReadAll returned %d entries; want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Term != want[i].Term || got[i].Index != want[i].Index || string(got[i].Data) != string(want[i].Data) {
			t.Fatalf("entry %d = %+v; want %+v", i, got[i], want[i])
		}
	}
}

func TestCorruptedTailIsIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]Entry{{Term: 1, Index: 1, Data: []byte("good")}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Append garbage to pretend a write was cut off during a crash.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x00, 0x00, 0x00, 0x10, 0xde, 0xad}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	got, err := w2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0].Data) != "good" {
		t.Fatalf("ReadAll = %+v; want only the good entry", got)
	}
}

func TestReplayKeepsLastRecordForIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	records := []Entry{
		{Term: 1, Index: 1, Data: []byte("one")},
		{Term: 1, Index: 2, Data: []byte("old two")},
		{Term: 2, Index: 2, Data: []byte("new two")},
	}
	if err := w.Append(records); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	got, err := w2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("ReadAll returned %d records; want all 3", len(got))
	}

	// Replay the same way raft does: a later record at an index replaces the
	// older one. Index 0 is the sentinel entry.
	log := []Entry{{Term: 0, Index: 0}}
	for _, e := range got {
		if e.Index < uint64(len(log)) {
			log = log[:e.Index]
		}
		log = append(log, e)
	}
	if len(log) != 3 {
		t.Fatalf("replayed log has %d entries; want 3", len(log))
	}
	if log[2].Term != 2 || string(log[2].Data) != "new two" {
		t.Fatalf("index 2 = %+v; want the new term", log[2])
	}
}
