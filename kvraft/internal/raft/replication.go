package raft

import (
	"log"
	"time"

	"kvraft/internal/wal"
	"kvraft/proto/raftpb"
)

func (rf *Raft) peerLoop(id int, addr string, term int) {
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
			entries = append(entries, &raftpb.LogEntry{Term: uint64(e.Term), Index: uint64(e.Index), Data: e.Data})
		}
		req := &raftpb.AppendEntriesRequest{
			Term:         uint64(term),
			LeaderId:     uint32(rf.id),
			PrevLogIndex: uint64(prevIndex),
			PrevLogTerm:  uint64(prevTerm),
			Entries:      entries,
			LeaderCommit: uint64(rf.commitIndex),
		}
		rf.mu.Unlock()

		resp, err := rf.sendAppendEntries(addr, req)
		if err != nil {
			time.Sleep(tickInterval)
			continue
		}

		rf.mu.Lock()
		if int(resp.Term) > rf.currentTerm {
			// step down: a higher term means we are out of date
			rf.becomeFollower(int(resp.Term))
			rf.mu.Unlock()
			return
		}
		if rf.state != leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		if resp.Success {
			match := int(resp.MatchIndex)
			if match > rf.matchIndex[id] {
				rf.matchIndex[id] = match
			}
			if rf.matchIndex[id]+1 > rf.nextIndex[id] {
				rf.nextIndex[id] = rf.matchIndex[id] + 1
			}
			rf.advanceCommitIndex()
		} else if rf.nextIndex[id] > 1 {
			rf.nextIndex[id]--
		}
		rf.mu.Unlock()

		time.Sleep(tickInterval)
	}
}

func (rf *Raft) advanceCommitIndex() {
	for n := rf.lastIndex(); n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			// only entries from the current term may be committed by counting
			continue
		}
		count := 0
		if n <= rf.syncedIndex {
			count++ // our own copy counts once it is on disk
		}
		for id := 1; id < len(rf.peers); id++ {
			if id != rf.id && rf.matchIndex[id] >= n {
				count++
			}
		}
		if count >= rf.quorum {
			rf.commitIndex = n
			return
		}
	}
}

func (rf *Raft) appendToWAL(entries []wal.Entry) error {
	if rf.wal == nil || len(entries) == 0 {
		return nil
	}
	if err := rf.wal.Append(entries); err != nil {
		return err
	}
	return rf.wal.Sync()
}

func (rf *Raft) HandleAppendEntries(req *raftpb.AppendEntriesRequest) *raftpb.AppendEntriesResponse {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if int(req.Term) < rf.currentTerm {
		return &raftpb.AppendEntriesResponse{Term: uint64(rf.currentTerm), Success: false}
	}
	if int(req.Term) > rf.currentTerm {
		rf.becomeFollower(int(req.Term))
	}
	rf.state = follower
	rf.leaderID = int(req.LeaderId)
	rf.resetElectionTimer()

	prevIndex := int(req.PrevLogIndex)
	if prevIndex > rf.lastIndex() {
		return &raftpb.AppendEntriesResponse{Term: uint64(rf.currentTerm), Success: false}
	}
	if rf.log[prevIndex].Term != int(req.PrevLogTerm) {
		return &raftpb.AppendEntriesResponse{Term: uint64(rf.currentTerm), Success: false}
	}

	// The WAL is append-only, so a conflict just adds records; replay keeps
	// the last record for an index.
	var fresh []wal.Entry
	for i, e := range req.Entries {
		index := prevIndex + 1 + i
		if index <= rf.lastIndex() {
			if rf.log[index].Term == int(e.Term) {
				continue
			}
			rf.log = rf.log[:index]
		}
		rf.log = append(rf.log, LogEntry{Term: int(e.Term), Index: index, Data: e.Data})
		fresh = append(fresh, wal.Entry{Term: e.Term, Index: uint64(index), Data: e.Data})
	}
	if err := rf.wal.Append(fresh); err != nil {
		log.Printf("raft %d: wal append: %v", rf.id, err)
	}

	if int(req.LeaderCommit) > rf.commitIndex {
		last := rf.lastIndex()
		if int(req.LeaderCommit) < last {
			rf.commitIndex = int(req.LeaderCommit)
		} else {
			rf.commitIndex = last
		}
	}

	// only count entries that are already on disk
	return &raftpb.AppendEntriesResponse{
		Term:       uint64(rf.currentTerm),
		Success:    true,
		MatchIndex: uint64(rf.syncedIndex),
	}
}
