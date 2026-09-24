package raft

import (
	"log"
	"time"

	"kvraft/internal/wal"
)

// proposal is a command waiting to be added to the log.
type proposal struct {
	cmd    []byte
	result chan proposalResult
}

// proposalResult is the index and term assigned to a command.
type proposalResult struct {
	index uint64
	term  uint64
	ok    bool
}

// Propose adds a command to the log. It returns the index and term the
// command was assigned, and whether this node is the leader. The command is
// queued so several proposals can share one WAL sync and one AppendEntries.
func (rf *Raft) Propose(cmd []byte) (uint64, uint64, bool) {
	rf.mu.Lock()
	isLeader := rf.state == leader
	rf.mu.Unlock()
	if !isLeader {
		return 0, 0, false
	}

	p := proposal{cmd: cmd, result: make(chan proposalResult, 1)}
	rf.proposeCh <- p
	res := <-p.result
	return res.index, res.term, res.ok
}

// batchLoop collects proposals for a short window, then appends them to the
// log with a single WAL write and replicates them together.
func (rf *Raft) batchLoop() {
	for first := range rf.proposeCh {
		batch := []proposal{first}

		timer := time.NewTimer(batchWindow)
	collect:
		for len(batch) < maxBatch {
			select {
			case p := <-rf.proposeCh:
				batch = append(batch, p)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()

		rf.commitBatch(batch)
	}
}

// commitBatch appends one batch to the log and WAL, answers the callers and
// starts replication.
func (rf *Raft) commitBatch(batch []proposal) {
	rf.mu.Lock()
	if rf.state != leader {
		rf.mu.Unlock()
		for _, p := range batch {
			p.result <- proposalResult{ok: false}
		}
		return
	}

	term := rf.currentTerm
	startIndex := rf.lastIndex() + 1
	entries := make([]wal.Entry, 0, len(batch))
	for i, p := range batch {
		index := startIndex + uint64(i)
		rf.log = append(rf.log, LogEntry{Term: term, Index: index, Data: p.cmd})
		entries = append(entries, wal.Entry{Term: term, Index: index, Data: p.cmd})
	}
	if err := rf.appendToWAL(entries); err != nil {
		log.Printf("raft %d: wal append: %v", rf.id, err)
	}
	rf.mu.Unlock()

	for i, p := range batch {
		p.result <- proposalResult{index: startIndex + uint64(i), term: term, ok: true}
	}
	go rf.broadcastAppendEntries()
}

// appendToWAL writes entries and forces them to disk. The caller must hold
// rf.mu so the log and the file stay in the same order.
func (rf *Raft) appendToWAL(entries []wal.Entry) error {
	if rf.wal == nil || len(entries) == 0 {
		return nil
	}
	if err := rf.wal.Append(entries); err != nil {
		return err
	}
	return rf.wal.Sync()
}
