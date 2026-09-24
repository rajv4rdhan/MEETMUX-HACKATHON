package node

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"kvraft/proto/raftpb"
)

const forwardTimeout = 500 * time.Millisecond

var errNoLeader = errors.New("no leader known")

// forward sends a command to the current leader and waits for its reply.
func (n *Node) forward(cmd []byte) error {
	addr := n.raft.LeaderAddr()
	if addr == "" {
		return errNoLeader
	}
	return n.forwardTo(addr, cmd)
}

// forwardTo calls the leader's Forward RPC once.
func (n *Node) forwardTo(addr string, cmd []byte) error {
	client, err := n.leaderClient(addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), forwardTimeout)
	defer cancel()

	resp, err := client.Forward(ctx, &raftpb.ForwardRequest{Command: cmd})
	if err != nil {
		n.dropClient(addr)
		return err
	}
	if resp.Error != "" {
		return errors.New(resp.Error)
	}
	return nil
}

// leaderClient returns a cached gRPC client for the leader address.
func (n *Node) leaderClient(addr string) (raftpb.RaftClient, error) {
	n.connMu.Lock()
	defer n.connMu.Unlock()

	if client, ok := n.fwdConns[addr]; ok {
		return client, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := raftpb.NewRaftClient(conn)
	n.fwdConns[addr] = client
	return client, nil
}

// dropClient forgets a cached connection so the next call dials again.
func (n *Node) dropClient(addr string) {
	n.connMu.Lock()
	defer n.connMu.Unlock()
	delete(n.fwdConns, addr)
}

// runForwarded executes a command that a follower forwarded to this leader.
func (n *Node) runForwarded(data []byte) ([]byte, error) {
	index, term, isLeader := n.raft.Propose(data)
	if !isLeader {
		return nil, errNotLeader
	}
	if !n.waitApplied(index, term) {
		return nil, errTimeout
	}
	return []byte("OK"), nil
}
