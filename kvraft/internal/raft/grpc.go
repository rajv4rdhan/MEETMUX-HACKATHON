package raft

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"kvraft/proto/raftpb"
)

// rpcServer adapts Raft to the generated gRPC service.
type rpcServer struct {
	raftpb.UnimplementedRaftServer
	rf *Raft
}

// RequestVote handles a vote request from a candidate.
func (s *rpcServer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return s.rf.HandleRequestVote(req), nil
}

// AppendEntries handles a heartbeat or log entries from the leader.
func (s *rpcServer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	return s.rf.HandleAppendEntries(req), nil
}

// Forward handles a client command sent to a follower.
func (s *rpcServer) Forward(ctx context.Context, req *raftpb.ForwardRequest) (*raftpb.ForwardResponse, error) {
	return s.rf.handleForward(req), nil
}

// Serve runs the gRPC server on addr and blocks until it stops.
func (rf *Raft) Serve(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	raftpb.RegisterRaftServer(server, &rpcServer{rf: rf})
	return server.Serve(ln)
}

// SetForwardHandler registers the function that runs a client command on the
// leader. The node package sets this so raft stays free of key-value logic.
func (rf *Raft) SetForwardHandler(fn func([]byte) ([]byte, error)) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.forwardHandler = fn
}

// handleForward runs a forwarded command if this node is the leader.
func (rf *Raft) handleForward(req *raftpb.ForwardRequest) *raftpb.ForwardResponse {
	rf.mu.Lock()
	handler := rf.forwardHandler
	rf.mu.Unlock()

	if handler == nil {
		return &raftpb.ForwardResponse{Error: "no forward handler"}
	}
	result, err := handler(req.Command)
	if err != nil {
		return &raftpb.ForwardResponse{Error: err.Error()}
	}
	return &raftpb.ForwardResponse{Result: result}
}

// peerClient returns a cached gRPC client for a peer address.
func (rf *Raft) peerClient(addr string) (raftpb.RaftClient, error) {
	rf.connMu.Lock()
	defer rf.connMu.Unlock()

	if client, ok := rf.conns[addr]; ok {
		return client, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := raftpb.NewRaftClient(conn)
	rf.conns[addr] = client
	return client, nil
}

// sendRequestVote asks one peer for a vote.
func (rf *Raft) sendRequestVote(addr string, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	client, err := rf.peerClient(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	return client.RequestVote(ctx, req)
}

// sendAppendEntries sends a heartbeat or log entries to one peer.
func (rf *Raft) sendAppendEntries(addr string, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	client, err := rf.peerClient(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	return client.AppendEntries(ctx, req)
}
