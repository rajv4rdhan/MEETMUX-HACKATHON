package raft

import (
	"log"
	"sync"

	"kvraft/internal/wal"
	"kvraft/proto/raftpb"
)

// toWALEntries converts raft log entries for storage.
func toWALEntries(entries []LogEntry) []wal.Entry {
	out := make([]wal.Entry, 0, len(entries))
	for _, e := range entries {
		out = append(out, wal.Entry{Term: e.Term, Index: e.Index, Data: e.Data})
	}
	return out
}

// broadcastAppendEntries replicates the log to every peer in parallel.
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	rf.mu.Unlock()

	var wg sync.WaitGroup
	for id, addr := range rf.peers {
		if id == rf.id {
			continue
		}
		wg.Add(1)
		go func(id uint32, addr string) {
			defer wg.Done()
			rf.replicateTo(id, addr, term)
		}(id, addr)
	}
}

// replicateTo sends whatever a peer is missing, backing up one entry at a
// time when the peer rejects the request.
func (rf *Raft) replicateTo(id uint32, addr string, term uint64) {
	for {
		rf.mu.Lock()
		if rf.state != leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		next := rf.nextIndex[id]
		if next < 1 {
			next = 1
		}
		if next > rf.lastIndex()+1 {
			next = rf.lastIndex() + 1
		}
		prevIndex := next - 1
		prevTerm := rf.log[prevIndex].Term

		entries := make([]*raftpb.LogEntry, 0, rf.lastIndex()-next+1)
		for i := next; i <= rf.lastIndex(); i++ {
			e := rf.log[i]
			entries = append(entries, &raftpb.LogEntry{Term: e.Term, Index: e.Index, Data: e.Data})
		}
		req := &raftpb.AppendEntriesRequest{
			Term:         term,
			LeaderId:     rf.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		resp, err := rf.sendAppendEntries(addr, req)
		if err != nil {
			return
		}

		rf.mu.Lock()
		if resp.Term > rf.currentTerm {
			// step down: a higher term means we are out of date
			rf.becomeFollower(resp.Term)
			rf.mu.Unlock()
			return
		}
		if rf.state != leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		if resp.Success {
			rf.matchIndex[id] = req.PrevLogIndex + uint64(len(req.Entries))
			rf.nextIndex[id] = rf.matchIndex[id] + 1
			rf.advanceCommitIndex()
			rf.mu.Unlock()
			return
		}

		if rf.nextIndex[id] > 1 {
			rf.nextIndex[id]--
		}
		rf.mu.Unlock()
	}
}

// advanceCommitIndex moves commitIndex forward once a majority has stored an
// entry. The caller must hold rf.mu.
func (rf *Raft) advanceCommitIndex() {
	for n := rf.lastIndex(); n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			// Only entries from the current term may be committed by counting.
			continue
		}
		count := 1
		for id := range rf.peers {
			if id != rf.id && rf.matchIndex[id] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.persistState()
			rf.signalApply()
			return
		}
	}
}

// HandleAppendEntries accepts a heartbeat or new entries from the leader.
func (rf *Raft) HandleAppendEntries(req *raftpb.AppendEntriesRequest) *raftpb.AppendEntriesResponse {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if req.Term < rf.currentTerm {
		return &raftpb.AppendEntriesResponse{Term: rf.currentTerm, Success: false}
	}
	if req.Term > rf.currentTerm {
		rf.becomeFollower(req.Term)
	}
	// A valid leader exists, so restart the election countdown.
	rf.state = follower
	rf.leaderID = req.LeaderId
	rf.resetElectionTimer()

	// The log must already contain the entry just before the new ones.
	if req.PrevLogIndex > rf.lastIndex() {
		return &raftpb.AppendEntriesResponse{Term: rf.currentTerm, Success: false}
	}
	if rf.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		return &raftpb.AppendEntriesResponse{Term: rf.currentTerm, Success: false}
	}

	// Append the new entries, dropping any conflicting suffix first.
	truncated := false
	var fresh []wal.Entry
	for i, e := range req.Entries {
		index := req.PrevLogIndex + 1 + uint64(i)
		if index <= rf.lastIndex() {
			if rf.log[index].Term == e.Term {
				continue
			}
			rf.log = rf.log[:index]
			truncated = true
		}
		rf.log = append(rf.log, LogEntry{Term: e.Term, Index: index, Data: e.Data})
		fresh = append(fresh, wal.Entry{Term: e.Term, Index: index, Data: e.Data})
	}

	if truncated {
		// The file is append-only, so a shorter log must be rewritten.
		if err := rf.wal.Rewrite(toWALEntries(rf.log[1:])); err != nil {
			log.Printf("raft %d: wal rewrite: %v", rf.id, err)
		}
	} else if err := rf.appendToWAL(fresh); err != nil {
		log.Printf("raft %d: wal append: %v", rf.id, err)
	}

	// Followers commit everything the leader has committed.
	if req.LeaderCommit > rf.commitIndex {
		last := rf.lastIndex()
		if req.LeaderCommit < last {
			rf.commitIndex = req.LeaderCommit
		} else {
			rf.commitIndex = last
		}
		rf.persistState()
		rf.signalApply()
	}

	return &raftpb.AppendEntriesResponse{
		Term:       rf.currentTerm,
		Success:    true,
		MatchIndex: rf.lastIndex(),
	}
}
