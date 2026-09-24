# kvraft

A small distributed in-memory key-value cache with Raft consensus and a
write-ahead log. It speaks the Redis protocol (RESP), so `redis-cli` and
`redis-benchmark` work against it. Writes on a follower are forwarded to the
leader, and the cluster survives a node dying.

## How to run

Docker (recommended):

```sh
make up            # docker compose up --build
redis-cli -h localhost -p 6380 set a 1
redis-cli -h localhost -p 6381 get a
make down
```

Local (needs Go 1.23+, `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc`):

```sh
make proto
go run ./cmd/server -id=1 -resp=:6380 -raft=:5001 -data=data/1
go run ./cmd/server -id=2 -resp=:6381 -raft=:5002 -data=data/2
go run ./cmd/server -id=3 -resp=:6382 -raft=:5003 -data=data/3
```

Each node takes `-id`, `-resp`, `-raft`, `-data` and
`-peers=id:raftport:respport,...`.

## Manual test checklist

1. Start 3 nodes. Exactly one log line says `became leader`.
2. `redis-cli -p 6380 set a 1`, then `get a` on 6380, 6381 and 6382 returns `1`.
3. `set` on a follower's port returns `OK` (it forwards to the leader).
4. Stop the leader (Ctrl+C). A new leader appears within about 1 second and
   `set` still works.
5. Restart the stopped node. `get a` on its port returns `1` (WAL replay).
6. `go run ./cmd/client` and stop the leader: the printed max gap is under 1 s.

## Benchmarks

`redis-benchmark -c 50 -d 64` on Docker Desktop. Full table and notes in
[`bench/results.md`](bench/results.md).

| Setup | SET ops/s | GET ops/s |
|---|---|---|
| Redis (no persistence), P=16 | 694,445 | 699,301 |
| Redis (`appendfsync always`), P=16 | 76,336 | 552,486 |
| Ours, Raft + WAL, P=16 | 1,146 | 558,659 |
| Ours, Raft + WAL, P=1, c=200 | 3,983 | - |

GET matches Redis because it comes from memory. SET is much slower: it needs a
fsync on the leader and the followers plus gRPC round trips. Here fsync is the
bottleneck (about 16 ms on this Docker volume), and more clients make bigger
batches, which is why `-c 200` is faster.

## Known limitations

- No snapshots or cluster membership changes; the log grows without bound.
- GET is served from the local store, so a follower's read can be slightly
  stale during a leader change.
- Every write waits for the 10 ms sync tick, so single-writer latency is
  higher than it needs to be.
