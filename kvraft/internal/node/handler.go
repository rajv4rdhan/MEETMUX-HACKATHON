package node

import (
	"errors"
	"strings"

	"kvraft/internal/resp"
)

var (
	errNotLeader = errors.New("not leader")
	errTimeout   = errors.New("timed out waiting for commit")
)

// apply proposes a command through raft and waits until it is applied.
func (n *Node) apply(cmd Command) error {
	data, err := Encode(cmd)
	if err != nil {
		return err
	}
	index, _, isLeader := n.raft.Propose(data)
	if !isLeader {
		return errNotLeader
	}
	if !n.waitApplied(index) {
		return errTimeout
	}
	return nil
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
