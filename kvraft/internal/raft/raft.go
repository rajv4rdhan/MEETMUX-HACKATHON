// Package raft implements the Raft consensus algorithm for one node.
//
// Raft knows nothing about keys and values: it only replicates opaque
// commands and hands committed ones to the apply channel.
//
// functions starting with a lowercase letter expect rf.mu to be held.
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

type state int

const (
	follower state = iota
	candidate
	leader
)

type LogEntry struct {
	Term  int
	Index int
	Data  []byte
}

type ApplyMsg struct {
	Index int
	Term  int
	Data  []byte
}

type Raft struct {
	mu sync.Mutex

	id     int
	peers  []string // indexed by id, index 0 is unused
	quorum int
	wal    *wal.WAL

	currentTerm int
	votedFor    int // 0 means no vote yet
	log         []LogEntry

	commitIndex int
	lastApplied int
	syncedIndex int // last index known to be on disk

	nextIndex  []int
	matchIndex []int

	state    state
	leaderID int

	applyCh chan ApplyMsg

	// A deadline instead of a real timer keeps the ticker loop simple.
	lastContact     time.Time
	electionTimeout time.Duration

	connMu sync.Mutex
	conns  map[string]raftpb.RaftClient
}

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

func (rf *Raft) Start() {
	go rf.ticker()
}

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

// syncWAL runs outside rf.mu so writes are not blocked by the fsync.
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

func (rf *Raft) LeaderID() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderID
}

func (rf *Raft) electionTimedOut() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastContact) >= rf.electionTimeout
}

func (rf *Raft) resetElectionTimer() {
	rf.lastContact = time.Now()
	rf.electionTimeout = randomElectionTimeout()
}

// randomElectionTimeout keeps nodes from starting elections at the same moment.
func randomElectionTimeout() time.Duration {
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
}

func (rf *Raft) lastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) lastLogInfo() (int, int) {
	last := rf.log[len(rf.log)-1]
	return last.Index, last.Term
}

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

func (rf *Raft) becomeFollower(term int) {
	rf.state = follower
	rf.currentTerm = term
	rf.votedFor = 0
	rf.resetElectionTimer()
	rf.persistState()
}

func (rf *Raft) becomeLeader() {
	rf.state = leader
	rf.leaderID = rf.id
	for id := range rf.peers {
		rf.nextIndex[id] = rf.lastIndex() + 1
		rf.matchIndex[id] = 0
	}

	// The empty entry lets the new leader commit everything left over from
	// earlier terms, so a restarted node rebuilds its store.
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
