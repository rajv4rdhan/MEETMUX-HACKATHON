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
