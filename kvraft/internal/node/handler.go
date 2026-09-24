package node

import (
	"encoding/json"
	"strings"
	"time"

	"kvraft/internal/resp"
)

// Command is a store operation that gets replicated through raft.
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

const (
	OpSet = "set"
	OpDel = "del"
)

func Encode(c Command) ([]byte, error) {
	return json.Marshal(c)
}

func Decode(data []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(data, &c)
	return c, err
}

// forwardOrPropose forwards to the leader when this node is not the leader.
// done is false only when the command committed locally, so the caller can
// build the success reply.
func (n *Node) forwardOrPropose(args []string, cmd Command) (resp.Reply, bool) {
	data, err := Encode(cmd)
	if err != nil {
		return resp.Error(err.Error()), true
	}

	for attempt := 0; attempt < 3; attempt++ {
		index, term, isLeader := n.raft.Propose(data)
		if isLeader {
			if n.waitApplied(index, term) {
				return nil, false
			}
		} else if reply, ok := n.forward(args); ok {
			return reply, true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return resp.Error("not leader"), true
}

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
	if reply, done := n.forwardOrPropose(args, Command{Op: OpSet, Key: args[1], Value: args[2]}); done {
		return reply
	}
	return resp.OK()
}

func (n *Node) handleDel(args []string) resp.Reply {
	if len(args) != 2 {
		return resp.Error("wrong number of arguments for 'del'")
	}
	_, existed := n.store.Get(args[1])
	if reply, done := n.forwardOrPropose(args, Command{Op: OpDel, Key: args[1]}); done {
		return reply
	}
	if existed {
		return resp.Int(1)
	}
	return resp.Int(0)
}
