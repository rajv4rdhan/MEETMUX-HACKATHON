package raft

import (
	"sync"

	"kvraft/proto/raftpb"
)

// broadcastAppendEntries sends a heartbeat to every peer so followers know a
// leader is alive.
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	prevIndex := rf.lastIndex()
	prevTerm := rf.log[prevIndex].Term
	commit := rf.commitIndex
	rf.mu.Unlock()

	var wg sync.WaitGroup
	for id, addr := range rf.peers {
		if id == rf.id {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			req := &raftpb.AppendEntriesRequest{
				Term:         term,
				LeaderId:     rf.id,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				LeaderCommit: commit,
			}
			resp, err := rf.sendAppendEntries(addr, req)
			if err != nil {
				return
			}
			rf.mu.Lock()
			if resp.Term > rf.currentTerm {
				// step down: a higher term means we are out of date
				rf.becomeFollower(resp.Term)
			}
			rf.mu.Unlock()
		}(addr)
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

	return &raftpb.AppendEntriesResponse{Term: rf.currentTerm, Success: true}
}
