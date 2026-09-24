// Package node wires the store, the raft log and the RESP server together.
package node

import (
	"log"

	"kvraft/internal/config"
	"kvraft/internal/resp"
	"kvraft/internal/store"
)

// Node is one member of the key-value cache.
type Node struct {
	cfg    config.Config
	store  *store.Store
	server *resp.Server
}

// New creates a node from config.
func New(cfg config.Config) *Node {
	return &Node{
		cfg:   cfg,
		store: store.New(),
	}
}

// Run starts the RESP server and blocks until it stops.
func (n *Node) Run() error {
	n.server = resp.NewServer(n.cfg.RespAddr, n)
	log.Printf("node %d resp listening on %s", n.cfg.ID, n.cfg.RespAddr)
	return n.server.ListenAndServe()
}
