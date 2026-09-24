package node

import (
	"errors"
	"strings"
	"time"

	"kvraft/internal/resp"
)

var (
	errNotLeader = errors.New("not leader")
	errTimeout   = errors.New("timed out waiting for commit")
)

// apply proposes a command through raft and waits until it is applied. If
// this node is not the leader, the command is forwarded to the leader. It
// retries a few times because the leader may change during a failover.
func (n *Node) apply(cmd Command) error {
	data, err := Encode(cmd)
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		index, _, isLeader := n.raft.Propose(data)
		if isLeader {
			if n.waitApplied(index) {
				return nil
			}
			lastErr = errTimeout
		} else if err := n.forward(data); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return lastErr
}

// Handle runs one client command against the store.
func (n *Node) Handle(args []string) resp.Reply {
	switch strings.ToUpper(args[0]) {
	case "PING":
		return resp.Bulk("PONG")
	case "GET":
		return n.handleGet(args)
	case "SET":
		return n.handleSet(args)
	case "DEL":
		return n.handleDel(args)
	default:
		return resp.Error("unknown command '" + args[0] + "'")
	}
}

func (n *Node) handleGet(args []string) resp.Reply {
	if len(args) != 2 {
		return resp.Error("wrong number of arguments for 'get'")
	}
	value, ok := n.store.Get(args[1])
	if !ok {
		return resp.Nil()
	}
	return resp.Bulk(value)
}

func (n *Node) handleSet(args []string) resp.Reply {
	if len(args) != 3 {
		return resp.Error("wrong number of arguments for 'set'")
	}
	if err := n.apply(Command{Op: OpSet, Key: args[1], Value: args[2]}); err != nil {
		return resp.Error(err.Error())
	}
	return resp.OK()
}

func (n *Node) handleDel(args []string) resp.Reply {
	if len(args) != 2 {
		return resp.Error("wrong number of arguments for 'del'")
	}
	if err := n.apply(Command{Op: OpDel, Key: args[1]}); err != nil {
		return resp.Error(err.Error())
	}
	return resp.Int(1)
}
