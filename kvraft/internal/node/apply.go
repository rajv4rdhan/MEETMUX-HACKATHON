package node

import (
	"log"
	"time"
)

// applyLoop applies committed commands to the store and wakes any client
// waiting for that log index.
func (n *Node) applyLoop() {
	for msg := range n.applyCh {
		cmd, err := Decode(msg.Data)
		if err != nil {
			log.Printf("node %d: dropping bad command: %v", n.cfg.ID, err)
			continue
		}
		n.applyCommand(cmd)

		n.mu.Lock()
		n.lastApplied = msg.Index
		if ch, ok := n.pending[msg.Index]; ok {
			delete(n.pending, msg.Index)
			close(ch)
		}
		n.mu.Unlock()
	}
}

// applyCommand runs one command against the local store.
func (n *Node) applyCommand(cmd Command) {
	switch cmd.Op {
	case OpSet:
		n.store.Set(cmd.Key, cmd.Value)
	case OpDel:
		n.store.Del(cmd.Key)
	}
}

// waitApplied blocks until the entry at index has been applied to the store.
func (n *Node) waitApplied(index uint64) bool {
	n.mu.Lock()
	if n.lastApplied >= index {
		n.mu.Unlock()
		return true
	}
	ch := make(chan struct{})
	n.pending[index] = ch
	n.mu.Unlock()

	select {
	case <-ch:
		return true
	case <-time.After(2 * time.Second):
		n.mu.Lock()
		delete(n.pending, index)
		n.mu.Unlock()
		return false
	}
}
