// Package node wires the store, the raft log and the RESP server together.
package node

import (
	"log"
	"os"
	"path/filepath"
	"sync"

	"kvraft/internal/config"
	"kvraft/internal/raft"
	"kvraft/internal/resp"
	"kvraft/internal/store"
	"kvraft/internal/wal"
	"kvraft/proto/raftpb"
)

// Node is one member of the key-value cache.
type Node struct {
	cfg     config.Config
	store   *store.Store
	server  *resp.Server
	raft    *raft.Raft
	wal     *wal.WAL
	applyCh chan raft.ApplyMsg

	// pending maps a log index to the clients waiting for it to be applied.
	mu          sync.Mutex
	pending     map[uint64]chan struct{}
	lastApplied uint64

	// fwdConns caches gRPC clients to the leader for forwarded commands.
	connMu   sync.Mutex
	fwdConns map[string]raftpb.RaftClient
}

// New creates a node from config and opens its write-ahead log.
func New(cfg config.Config) (*Node, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	w, err := wal.Open(filepath.Join(cfg.DataDir, "raft.wal"))
	if err != nil {
		return nil, err
	}

	n := &Node{
		cfg:      cfg,
		store:    store.New(),
		wal:      w,
		applyCh:  make(chan raft.ApplyMsg, 256),
		pending:  make(map[uint64]chan struct{}),
		fwdConns: make(map[string]raftpb.RaftClient),
	}
	n.raft = raft.New(cfg.ID, cfg.Peers, w, n.applyCh)
	n.raft.SetForwardHandler(n.runForwarded)
	return n, nil
}

// Run starts the raft and RESP servers and blocks until the RESP server stops.
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
