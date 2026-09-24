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

	"kvraft/internal/wal"
	"kvraft/proto/raftpb"
)

const (
	tickInterval       = 10 * time.Millisecond
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

// LogEntry is one replicated command.
type LogEntry struct {
	Term  int
	Index int
	Data  []byte
}

// ApplyMsg is sent on the apply channel after an entry is committed.
type ApplyMsg struct {
	Index int
	Term  int
	Data  []byte
}

// Raft is a single member of a Raft cluster.
type Raft struct {
	mu sync.Mutex

	id     int
	peers  []string // indexed by id, index 0 is unused
	quorum int
	wal    *wal.WAL

	// Persistent state.
	currentTerm int
	votedFor    int // 0 means no vote yet
	log         []LogEntry

	// Volatile state.
	commitIndex int
	lastApplied int
	syncedIndex int // last index known to be on disk

	// Leader state, rebuilt after every election.
	nextIndex  []int
	matchIndex []int

	state    state
	leaderID int

	applyCh chan ApplyMsg

	// Election timer, kept as a deadline instead of a real timer so the
	// ticker loop stays simple.
	lastContact     time.Time
	electionTimeout time.Duration

	connMu sync.Mutex
	conns  map[string]raftpb.RaftClient
}

// New creates a raft node, recovering any log and term already on disk.
// Committed entries are sent on applyCh in order.
func New(id int, peers []string, w *wal.WAL, applyCh chan ApplyMsg) *Raft {
	count := 0
	for i := 1; i < len(peers); i++ {
		if peers[i] != "" {
			count++
		}
	}

	rf := &Raft{
		id:          id,
		peers:       peers,
		quorum:      count/2 + 1,
		wal:         w,
		log:         []LogEntry{{Term: 0, Index: 0}},
		applyCh:     applyCh,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		state:       follower,
		lastContact: time.Now(),
		conns:       make(map[string]raftpb.RaftClient),
	}
	rf.electionTimeout = randomElectionTimeout()
	rf.recover()
	return rf
}

// Start launches the background loop.
func (rf *Raft) Start() {
	go rf.ticker()
}

// ticker syncs the log, watches for election timeouts and applies commits.
func (rf *Raft) ticker() {
	for {
		rf.syncWAL()

		rf.mu.Lock()
		isLeader := rf.state == leader
		rf.mu.Unlock()

		if !isLeader && rf.electionTimedOut() {
			rf.startElection()
		}
		rf.applyCommitted()
		time.Sleep(tickInterval)
	}
}

// Propose adds a command to the log. It returns the index and term the
// command was assigned, and whether this node is the leader.
func (rf *Raft) Propose(cmd []byte) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		return 0, 0, false
	}
	entry := LogEntry{Term: rf.currentTerm, Index: rf.lastIndex() + 1, Data: cmd}
	rf.log = append(rf.log, entry)
	if err := rf.wal.Append([]wal.Entry{{Term: uint64(entry.Term), Index: uint64(entry.Index), Data: entry.Data}}); err != nil {
		log.Printf("raft %d: wal append: %v", rf.id, err)
	}
	return entry.Index, entry.Term, true
}

// syncWAL forces the log file to disk and records how far this node has
// safely stored entries. It runs outside rf.mu so writes are not blocked by
// the fsync.
func (rf *Raft) syncWAL() {
	rf.mu.Lock()
	idx := rf.lastIndex()
	rf.mu.Unlock()

	if err := rf.wal.Sync(); err != nil {
		log.Printf("raft %d: wal sync: %v", rf.id, err)
		return
	}

	rf.mu.Lock()
	if idx > rf.syncedIndex {
		rf.syncedIndex = idx
	}
	rf.mu.Unlock()
}

// LeaderID returns the id of the current leader, if known.
func (rf *Raft) LeaderID() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderID
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
func (rf *Raft) lastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// lastLogInfo returns the index and term of the newest log entry.
// The caller must hold rf.mu.
func (rf *Raft) lastLogInfo() (int, int) {
	last := rf.log[len(rf.log)-1]
	return last.Index, last.Term
}

// applyCommitted sends committed entries to the apply channel in order.
func (rf *Raft) applyCommitted() {
	for {
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			return
		}
		rf.lastApplied++
		entry := rf.log[rf.lastApplied]
		rf.mu.Unlock()

		rf.applyCh <- ApplyMsg{Index: entry.Index, Term: entry.Term, Data: entry.Data}
	}
}

// becomeFollower steps down to follower at the given term. The caller must
// hold rf.mu.
func (rf *Raft) becomeFollower(term int) {
	rf.state = follower
	rf.currentTerm = term
	rf.votedFor = 0
	rf.resetElectionTimer()
	rf.persistState()
}

// becomeLeader promotes a candidate that won the election. The caller must
// hold rf.mu.
func (rf *Raft) becomeLeader() {
	rf.state = leader
	rf.leaderID = rf.id
	for id := range rf.peers {
		rf.nextIndex[id] = rf.lastIndex() + 1
		rf.matchIndex[id] = 0
	}

	// Append an empty entry. Committing it also commits every entry left
	// over from earlier terms, so the store is rebuilt after a restart.
	entry := LogEntry{Term: rf.currentTerm, Index: rf.lastIndex() + 1}
	rf.log = append(rf.log, entry)
	if err := rf.appendToWAL([]wal.Entry{{Term: uint64(entry.Term), Index: uint64(entry.Index)}}); err != nil {
		log.Printf("raft %d: wal append noop: %v", rf.id, err)
	}

	for id, addr := range rf.peers {
		if id == 0 || id == rf.id {
			continue
		}
		go rf.peerLoop(id, addr, rf.currentTerm)
	}

	log.Printf("raft %d: became leader for term %d", rf.id, rf.currentTerm)
}
