package node

import (
	"log"
	"time"
)

func (n *Node) applyLoop() {
	for msg := range n.applyCh {
		// An empty command is the no-op a new leader commits to advance the
		// commit index; there is nothing to apply.
		if len(msg.Data) > 0 {
			cmd, err := Decode(msg.Data)
			if err != nil {
				log.Printf("node %d: dropping bad command: %v", n.cfg.ID, err)
			} else {
				n.applyCommand(cmd)
			}
		}

		n.mu.Lock()
		n.lastApplied = msg.Index
		n.appliedTerm[msg.Index] = msg.Term
		n.appliedCount++
		if n.appliedCount%1000 == 0 {
			for index := range n.appliedTerm {
				if index+1000 < msg.Index {
					delete(n.appliedTerm, index)
				}
			}
		}
		n.mu.Unlock()
	}
}

func (n *Node) applyCommand(cmd Command) {
	switch cmd.Op {
	case OpSet:
		n.store.Set(cmd.Key, cmd.Value)
	case OpDel:
		n.store.Del(cmd.Key)
	}
}

// waitApplied returns false if a different leader overwrote the entry.
func (n *Node) waitApplied(index, term int) bool {
	for i := 0; i < 2000; i++ {
		n.mu.Lock()
		if n.lastApplied >= index {
			applied, ok := n.appliedTerm[index]
			n.mu.Unlock()
			return ok && applied == term
		}
		n.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	return false
}
