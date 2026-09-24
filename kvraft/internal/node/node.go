// Package node wires the store, the raft log and the RESP server together.
package node

import (
	"log"

	"kvraft/internal/config"
	"kvraft/internal/raft"
	"kvraft/internal/resp"
	"kvraft/internal/store"
)

// Node is one member of the key-value cache.
type Node struct {
	cfg     config.Config
	store   *store.Store
	server  *resp.Server
	raft    *raft.Raft
	applyCh chan raft.ApplyMsg
}

// New creates a node from config.
func New(cfg config.Config) *Node {
	n := &Node{
		cfg:     cfg,
		store:   store.New(),
		applyCh: make(chan raft.ApplyMsg, 256),
	}
	n.raft = raft.New(cfg.ID, cfg.Peers, n.applyCh)
	return n
}

// Run starts the raft and RESP servers and blocks until the RESP server stops.
func (n *Node) Run() error {
	go func() {
		if err := n.raft.Serve(n.cfg.RaftAddr); err != nil {
			log.Fatalf("node %d raft server stopped: %v", n.cfg.ID, err)
		}
	}()
	n.raft.Start()

	n.server = resp.NewServer(n.cfg.RespAddr, n)
	log.Printf("node %d listening: resp=%s raft=%s", n.cfg.ID, n.cfg.RespAddr, n.cfg.RaftAddr)
	return n.server.ListenAndServe()
}
