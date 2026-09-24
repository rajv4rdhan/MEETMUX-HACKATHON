package node

import (
	"bufio"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"

	"kvraft/internal/config"
	"kvraft/internal/raft"
	"kvraft/internal/resp"
	"kvraft/internal/store"
	"kvraft/internal/wal"
)

type Node struct {
	cfg     config.Config
	store   *store.Store
	server  *resp.Server
	raft    *raft.Raft
	wal     *wal.WAL
	applyCh chan raft.ApplyMsg

	// appliedTerm lets a client check whether its write survived.
	mu           sync.Mutex
	lastApplied  int
	appliedTerm  map[int]int
	appliedCount int

	connMu       sync.Mutex
	leaderConn   net.Conn
	leaderReader *bufio.Reader
	leaderAddr   string
}

func New(cfg config.Config) (*Node, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	w, err := wal.Open(filepath.Join(cfg.DataDir, "raft.wal"))
	if err != nil {
		return nil, err
	}

	n := &Node{
		cfg:         cfg,
		store:       store.New(),
		wal:         w,
		applyCh:     make(chan raft.ApplyMsg, 256),
		appliedTerm: make(map[int]int),
	}

	raftPeers := make([]string, len(cfg.Peers))
	for id, peer := range cfg.Peers {
		raftPeers[id] = peer.RaftAddr
	}
	n.raft = raft.New(cfg.ID, raftPeers, w, n.applyCh)
	return n, nil
}

func (n *Node) Run() error {
	go func() {
		if err := n.raft.Serve(n.cfg.RaftAddr); err != nil {
			log.Fatalf("node %d raft server stopped: %v", n.cfg.ID, err)
		}
	}()
	n.raft.Start()
	go n.applyLoop()

	n.server = resp.NewServer(n.cfg.RespAddr, n)
	log.Printf("node %d listening: resp=%s raft=%s", n.cfg.ID, n.cfg.RespAddr, n.cfg.RaftAddr)
	return n.server.ListenAndServe()
}
