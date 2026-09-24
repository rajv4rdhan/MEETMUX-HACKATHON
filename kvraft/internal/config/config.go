// Package config loads the per-node settings from command line flags.
package config

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
)

// Config holds everything one node needs to start.
type Config struct {
	ID       uint32
	RespAddr string
	RaftAddr string
	DataDir  string
	Peers    map[uint32]string
}

// Load reads the node settings from the command line.
func Load() (Config, error) {
	id := flag.Uint("id", 1, "node id (1-3)")
	respAddr := flag.String("resp", ":6380", "address for the RESP server")
	raftAddr := flag.String("raft", ":5001", "address for the raft gRPC server")
	dataDir := flag.String("data", "data", "directory for the write-ahead log")
	peersFlag := flag.String("peers", "1=localhost:5001,2=localhost:5002,3=localhost:5003", "comma separated id=addr list")
	flag.Parse()

	peers, err := parsePeers(*peersFlag)
	if err != nil {
		return Config{}, err
	}

	return Config{
		ID:       uint32(*id),
		RespAddr: *respAddr,
		RaftAddr: *raftAddr,
		DataDir:  *dataDir,
		Peers:    peers,
	}, nil
}

// parsePeers turns "1=host:port,2=host:port" into a map of id to address.
func parsePeers(s string) (map[uint32]string, error) {
	peers := make(map[uint32]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idStr, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("bad peer %q, want id=addr", part)
		}
		id, err := strconv.ParseUint(idStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("bad peer id %q: %w", idStr, err)
		}
		peers[uint32(id)] = addr
	}
	return peers, nil
}
