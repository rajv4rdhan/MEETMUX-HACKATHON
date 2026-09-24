package raft

import (
	"log"
	"sync"

	"kvraft/proto/raftpb"
)

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
		if id == 0 || id == rf.id {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			req := &raftpb.RequestVoteRequest{
				Term:         uint64(term),
				CandidateId:  uint32(rf.id),
				LastLogIndex: uint64(lastIndex),
				LastLogTerm:  uint64(lastTerm),
			}
			resp, err := rf.sendRequestVote(addr, req)
			if err != nil {
				return
			}

			rf.mu.Lock()
			if int(resp.Term) > rf.currentTerm {
				// A higher term means we are out of date.
				rf.becomeFollower(int(resp.Term))
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
			won := votes >= rf.quorum
			voteMu.Unlock()
			if won {
				rf.mu.Lock()
				if rf.state == candidate && rf.currentTerm == term {
					rf.becomeLeader()
				}
				rf.mu.Unlock()
			}
		}(addr)
	}
}

func (rf *Raft) HandleRequestVote(req *raftpb.RequestVoteRequest) *raftpb.RequestVoteResponse {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if int(req.Term) < rf.currentTerm {
		return &raftpb.RequestVoteResponse{Term: uint64(rf.currentTerm), VoteGranted: false}
	}
	if int(req.Term) > rf.currentTerm {
		rf.becomeFollower(int(req.Term))
	}

	lastIndex, lastTerm := rf.lastLogInfo()
	upToDate := int(req.LastLogTerm) > lastTerm ||
		(int(req.LastLogTerm) == lastTerm && int(req.LastLogIndex) >= lastIndex)

	granted := false
	if (rf.votedFor == 0 || rf.votedFor == int(req.CandidateId)) && upToDate {
		rf.votedFor = int(req.CandidateId)
		rf.resetElectionTimer()
		rf.persistState()
		granted = true
	}

	return &raftpb.RequestVoteResponse{Term: uint64(rf.currentTerm), VoteGranted: granted}
}
