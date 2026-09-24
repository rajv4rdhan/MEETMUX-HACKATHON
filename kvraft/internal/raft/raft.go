// Package raft implements the Raft consensus algorithm for one node.
//
// Raft knows nothing about keys and values: it only replicates opaque
// commands and hands committed ones to the apply channel.
package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"kvraft/proto/raftpb"
)

const (
	heartbeatInterval  = 50 * time.Millisecond
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	rpcTimeout         = 100 * time.Millisecond
)

// state is the role a node plays at a given time.
type state int

const (
	follower state = iota
	candidate
	leader
)

func (s state) String() string {
	switch s {
	case follower:
		return "follower"
	case candidate:
		return "candidate"
	case leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry is one replicated command.
type LogEntry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

// ApplyMsg is sent on the apply channel after an entry is committed.
type ApplyMsg struct {
	Index uint64
	Data  []byte
}

// Raft is a single member of a Raft cluster.
type Raft struct {
	mu sync.Mutex

	id    uint32
	peers map[uint32]string

	// Persistent state.
	currentTerm uint64
	votedFor    uint32 // 0 means no vote yet
	log         []LogEntry

	// Volatile state.
	commitIndex uint64
	lastApplied uint64

	// Leader state, rebuilt after every election.
	nextIndex  map[uint32]uint64
	matchIndex map[uint32]uint64

	state    state
	leaderID uint32

	applyCh  chan ApplyMsg
	notifyCh chan struct{}

	// Election timer, kept as a deadline instead of a real timer so the
	// ticker loop stays simple.
	lastContact     time.Time
	electionTimeout time.Duration

	connMu sync.Mutex
	conns  map[string]raftpb.RaftClient

	forwardHandler func([]byte) ([]byte, error)
}

// New creates a raft node. Committed entries are sent on applyCh in order.
func New(id uint32, peers map[uint32]string, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		id:          id,
		peers:       peers,
		log:         []LogEntry{{Term: 0, Index: 0}},
		applyCh:     applyCh,
		notifyCh:    make(chan struct{}, 1),
		nextIndex:   make(map[uint32]uint64),
		matchIndex:  make(map[uint32]uint64),
		state:       follower,
		lastContact: time.Now(),
		conns:       make(map[string]raftpb.RaftClient),
	}
	rf.electionTimeout = randomElectionTimeout()
	return rf
}

// Start launches the background loops.
func (rf *Raft) Start() {
	go rf.ticker()
	go rf.applyLoop()
}

// Propose appends a command to the log. It returns the index and term the
// command was assigned, and whether this node is the leader.
func (rf *Raft) Propose(cmd []byte) (uint64, uint64, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		return 0, 0, false
	}
	entry := LogEntry{
		Term:  rf.currentTerm,
		Index: rf.lastIndex() + 1,
		Data:  cmd,
	}
	rf.log = append(rf.log, entry)

	go rf.broadcastAppendEntries()
	return entry.Index, entry.Term, true
}

// signalApply wakes the apply loop without blocking.
func (rf *Raft) signalApply() {
	select {
	case rf.notifyCh <- struct{}{}:
	default:
	}
}

// applyLoop sends committed entries to the apply channel in order.
func (rf *Raft) applyLoop() {
	for range rf.notifyCh {
		for {
			rf.mu.Lock()
			if rf.lastApplied >= rf.commitIndex {
				rf.mu.Unlock()
				break
			}
			rf.lastApplied++
			entry := rf.log[rf.lastApplied]
			rf.mu.Unlock()

			rf.applyCh <- ApplyMsg{Index: entry.Index, Data: entry.Data}
		}
	}
}

// ticker drives heartbeats on the leader and election timeouts elsewhere.
func (rf *Raft) ticker() {
	for {
		rf.mu.Lock()
		isLeader := rf.state == leader
		rf.mu.Unlock()

		if isLeader {
			rf.broadcastAppendEntries()
		} else if rf.electionTimedOut() {
			rf.startElection()
		}
		time.Sleep(heartbeatInterval)
	}
}

// IsLeader reports whether this node is currently the leader.
func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == leader
}

// LeaderAddr returns the raft address of the current leader, if known.
func (rf *Raft) LeaderAddr() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	addr, ok := rf.peers[rf.leaderID]
	if !ok {
		return ""
	}
	return addr
}

// electionTimedOut reports whether the node has not heard from a leader in time.
func (rf *Raft) electionTimedOut() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastContact) >= rf.electionTimeout
}

// resetElectionTimer restarts the countdown with a fresh random timeout.
// The caller must hold rf.mu.
func (rf *Raft) resetElectionTimer() {
	rf.lastContact = time.Now()
	rf.electionTimeout = randomElectionTimeout()
}

// randomElectionTimeout picks a timeout in [min, max) so nodes do not all
// start elections at the same moment.
func randomElectionTimeout() time.Duration {
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
}

// lastIndex is the index of the newest log entry. The caller must hold rf.mu.
func (rf *Raft) lastIndex() uint64 {
	return rf.log[len(rf.log)-1].Index
}

// lastLogInfo returns the index and term of the newest log entry.
// The caller must hold rf.mu.
func (rf *Raft) lastLogInfo() (uint64, uint64) {
	last := rf.log[len(rf.log)-1]
	return last.Index, last.Term
}

// becomeFollower steps down to follower at the given term. The caller must
// hold rf.mu.
func (rf *Raft) becomeFollower(term uint64) {
	rf.state = follower
	rf.currentTerm = term
	rf.votedFor = 0
	rf.resetElectionTimer()
}

// becomeLeader promotes a candidate that won the election. The caller must
// hold rf.mu.
func (rf *Raft) becomeLeader() {
	rf.state = leader
	rf.leaderID = rf.id
	next := rf.lastIndex() + 1
	for id := range rf.peers {
		rf.nextIndex[id] = next
		rf.matchIndex[id] = 0
	}
	log.Printf("raft %d: became leader for term %d", rf.id, rf.currentTerm)
}
