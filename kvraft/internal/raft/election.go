package raft

import (
	"log"
	"sync"

	"kvraft/proto/raftpb"
)

// startElection turns this node into a candidate and asks every peer for a
// vote in a new term.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = candidate
	rf.currentTerm++
	term := rf.currentTerm
	rf.votedFor = rf.id
	rf.resetElectionTimer()
	rf.persistState()
	lastIndex, lastTerm := rf.lastLogInfo()
	rf.mu.Unlock()

	log.Printf("raft %d: starting election for term %d", rf.id, term)

	var (
		voteMu sync.Mutex
		votes  = 1 // vote for self
		wg     sync.WaitGroup
	)

	for id, addr := range rf.peers {
		if id == rf.id {
			continue
		}
		wg.Add(1)
		go func(id uint32, addr string) {
			defer wg.Done()
			req := &raftpb.RequestVoteRequest{
				Term:         term,
				CandidateId:  rf.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			resp, err := rf.sendRequestVote(addr, req)
			if err != nil {
				return
			}

			rf.mu.Lock()
			if resp.Term > rf.currentTerm {
				// A higher term means we are out of date.
				rf.becomeFollower(resp.Term)
				rf.mu.Unlock()
				return
			}
			stillCandidate := rf.state == candidate && rf.currentTerm == term
			rf.mu.Unlock()
			if !stillCandidate || !resp.VoteGranted {
				return
			}

			voteMu.Lock()
			votes++
			won := votes > len(rf.peers)/2
			voteMu.Unlock()
			if won {
				rf.mu.Lock()
				became := rf.state == candidate && rf.currentTerm == term
				if became {
					rf.becomeLeader()
				}
				rf.mu.Unlock()
				if became {
					go rf.broadcastAppendEntries()
				}
			}
		}(id, addr)
	}
}

// HandleRequestVote decides whether to grant a vote to a candidate.
func (rf *Raft) HandleRequestVote(req *raftpb.RequestVoteRequest) *raftpb.RequestVoteResponse {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if req.Term < rf.currentTerm {
		return &raftpb.RequestVoteResponse{Term: rf.currentTerm, VoteGranted: false}
	}
	if req.Term > rf.currentTerm {
		rf.becomeFollower(req.Term)
	}

	lastIndex, lastTerm := rf.lastLogInfo()
	upToDate := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)

	granted := false
	if (rf.votedFor == 0 || rf.votedFor == req.CandidateId) && upToDate {
		rf.votedFor = req.CandidateId
		rf.resetElectionTimer()
		rf.persistState()
		granted = true
	}

	return &raftpb.RequestVoteResponse{Term: rf.currentTerm, VoteGranted: granted}
}
