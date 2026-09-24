package node

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"time"

	"kvraft/internal/resp"
)

func (n *Node) forward(args []string) (resp.Reply, bool) {
	leaderID := n.raft.LeaderID()
	if leaderID <= 0 || leaderID >= len(n.cfg.Peers) {
		return nil, false
	}
	peer := n.cfg.Peers[leaderID]
	if peer.RespAddr == "" {
		return nil, false
	}

	n.connMu.Lock()
	defer n.connMu.Unlock()

	if n.leaderConn == nil || n.leaderAddr != peer.RespAddr {
		if n.leaderConn != nil {
			n.leaderConn.Close()
			n.leaderConn = nil
		}
		conn, err := net.DialTimeout("tcp", peer.RespAddr, time.Second)
		if err != nil {
			return nil, false
		}
		n.leaderConn = conn
		n.leaderReader = bufio.NewReader(conn)
		n.leaderAddr = peer.RespAddr
	}

	if _, err := n.leaderConn.Write(encodeCommand(args)); err != nil {
		n.leaderConn.Close()
		n.leaderConn = nil
		return nil, false
	}
	line, err := n.leaderReader.ReadBytes('\n')
	if err != nil {
		n.leaderConn.Close()
		n.leaderConn = nil
		return nil, false
	}
	return resp.Reply(line), true
}

func encodeCommand(args []string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(arg), arg)
	}
	return b.Bytes()
}
