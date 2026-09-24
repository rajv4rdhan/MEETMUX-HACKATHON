package raft

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
)

// persistedState is the part of raft state that must survive a restart.
type persistedState struct {
	CurrentTerm int `json:"current_term"`
	VotedFor    int `json:"voted_for"`
}

// statePath is where the term and vote are stored, next to the WAL.
func (rf *Raft) statePath() string {
	if rf.wal == nil {
		return ""
	}
	return filepath.Join(rf.wal.Dir(), "state.json")
}

// recover reloads the log and the saved term and vote.
func (rf *Raft) recover() {
	if rf.wal == nil {
		return
	}
	if data, err := os.ReadFile(rf.statePath()); err == nil {
		var st persistedState
		if err := json.Unmarshal(data, &st); err == nil {
			rf.currentTerm = st.CurrentTerm
			rf.votedFor = st.VotedFor
		}
	}

	entries, err := rf.wal.ReadAll()
	if err != nil {
		log.Printf("raft %d: reading wal: %v", rf.id, err)
	}
	for _, e := range entries {
		// A later record at the same index replaces an older one, which is
		// how a follower that overwrote a suffix is replayed.
		if int(e.Index) < len(rf.log) {
			rf.log = rf.log[:e.Index]
		}
		rf.log = append(rf.log, LogEntry{Term: int(e.Term), Index: int(e.Index), Data: e.Data})
	}
	rf.syncedIndex = rf.lastIndex()
}

// persistState writes the term and vote so a restart cannot double-vote.
// The caller must hold rf.mu.
func (rf *Raft) persistState() {
	path := rf.statePath()
	if path == "" {
		return
	}
	data, err := json.Marshal(persistedState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
	})
	if err != nil {
		return
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		log.Printf("raft %d: saving state: %v", rf.id, err)
	}
}
