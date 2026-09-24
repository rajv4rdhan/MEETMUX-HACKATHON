package node

import (
	"strings"

	"kvraft/internal/resp"
)

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
	n.store.Set(args[1], args[2])
	return resp.OK()
}

func (n *Node) handleDel(args []string) resp.Reply {
	if len(args) != 2 {
		return resp.Error("wrong number of arguments for 'del'")
	}
	if n.store.Del(args[1]) {
		return resp.Int(1)
	}
	return resp.Int(0)
}
