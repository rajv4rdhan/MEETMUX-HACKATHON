package config

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
)

// Peer is one other node: where to reach its raft server and its RESP server.
type Peer struct {
	ID       int
	RaftAddr string
	RespAddr string
}

type Config struct {
	ID       int
	RespAddr string
	RaftAddr string
	DataDir  string
	Peers    []Peer // indexed by id, index 0 is unused
}

func Load() (Config, error) {
	id := flag.Int("id", 1, "node id (1-3)")
	respAddr := flag.String("resp", ":6380", "address for the RESP server")
	raftAddr := flag.String("raft", ":5001", "address for the raft gRPC server")
	dataDir := flag.String("data", "data", "directory for the write-ahead log")
	peersFlag := flag.String("peers", "1=localhost:5001:6380,2=localhost:5002:6381,3=localhost:5003:6382", "comma separated id=host:raftport:respport list")
	flag.Parse()

	peers, err := parsePeers(*peersFlag)
	if err != nil {
		return Config{}, err
	}

	return Config{
		ID:       *id,
		RespAddr: *respAddr,
		RaftAddr: *raftAddr,
		DataDir:  *dataDir,
		Peers:    peers,
	}, nil
}

func parsePeers(s string) ([]Peer, error) {
	var parsed []Peer
	maxID := 0
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idStr, rest, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("bad peer %q, want id=host:raftport:respport", part)
		}
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, fmt.Errorf("bad peer id %q: %w", idStr, err)
		}
		fields := strings.Split(rest, ":")
		if len(fields) != 3 {
			return nil, fmt.Errorf("bad peer %q, want id=host:raftport:respport", part)
		}
		parsed = append(parsed, Peer{
			ID:       id,
			RaftAddr: fields[0] + ":" + fields[1],
			RespAddr: fields[0] + ":" + fields[2],
		})
		if id > maxID {
			maxID = id
		}
	}

	peers := make([]Peer, maxID+1)
	for _, p := range parsed {
		peers[p.ID] = p
	}
	return peers, nil
}
