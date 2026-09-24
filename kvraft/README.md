# kvraft

A small distributed in-memory key-value cache with Raft consensus, a
write-ahead log and client failover. It speaks the Redis protocol (RESP), so
`redis-cli` and `redis-benchmark` work against it out of the box.

## What it does

- 3 nodes, one leader, replicated log.
- Leader election with randomized timeouts.
- Log replication following Figure 2 of the Raft paper.
- Write-ahead log with CRC checks and recovery after a restart.
- Group commit: many proposals share one WAL `fsync` and one `AppendEntries`.
- Clients may talk to any node; writes on a follower are forwarded to the leader.
- A small failover client that keeps writing while a node dies.

## Architecture

```
 redis-benchmark / redis-cli / failover client
            |   RESP over TCP (ports 6380, 6381, 6382)
            v
 +--------------------------- one node ---------------------------+
 |                                                                 |
 |   resp.Server  --->  node.Node  --->  raft.Raft  --->  wal.WAL  |
 |   (parse cmds)       (glue)           (consensus)     (disk)    |
 |                        |    ^             |                     |
 |                        v    | applyCh     | gRPC (5001-5003)    |
 |                   store.Store  <----------+---> other nodes     |
 |                   (sharded map)                                 |
 +-----------------------------------------------------------------+
```

- Clients speak **RESP**; nodes speak **gRPC** to each other.
- `raft` knows nothing about keys and values. It replicates `[]byte` commands
  and sends committed ones on an apply channel. `node` decodes them and updates
  `store`, which keeps the Raft code reusable and easy to test.

## Layout

```
kvraft/
├── cmd/server          # one node
├── cmd/client          # failover test client
├── internal/config     # flags -> Config
├── internal/store      # sharded map: Get, Set, Del
├── internal/resp       # RESP reader, writer and TCP server
├── internal/raft       # election, replication, batching, persistence, gRPC
├── internal/wal        # append-only log with CRC records
├── internal/node       # wires store + raft + resp, forwarding, apply
├── proto/raft.proto    # Raft service: RequestVote, AppendEntries, Forward
├── deploy/             # Dockerfile and docker-compose.yml
└── bench/              # run.sh and results.md
```

Package dependencies (no import cycles):

```
cmd/server --> config, node
node       --> store, raft, resp, wal, proto/raftpb
raft       --> wal, proto/raftpb
resp, store, wal --> standard library only
```

The only interface is `resp.Handler`, implemented by `node`, so `resp` never
needs to import `node`.

## Quick start

Requires Go 1.23+, `protoc` with `protoc-gen-go` and `protoc-gen-go-grpc`, and
Docker.

```sh
make proto          # generate proto/raftpb from proto/raft.proto
make build          # go build ./...
make test           # go test ./...
make up             # docker compose up --build
```

Then talk to any node:

```sh
redis-cli -h localhost -p 6380 set foo bar
redis-cli -h localhost -p 6381 get foo     # follower: forwards the write / reads local
redis-cli -h localhost -p 6382 get foo
```

`make down` stops the cluster.

### Node flags

| Flag | Default | Meaning |
|---|---|---|
| `-id` | `1` | node id |
| `-resp` | `:6380` | RESP listen address |
| `-raft` | `:5001` | gRPC listen address |
| `-data` | `data` | directory for `raft.wal` and `state.json` |
| `-peers` | `1=localhost:5001,...` | comma separated `id=addr` list |

## How it works

- **Election** (`internal/raft/election.go`): a follower that hears nothing for
  a random 150-300 ms becomes a candidate, bumps the term and asks for votes. A
  vote is granted only if the candidate's log is at least as up to date.
- **Replication** (`internal/raft/replication.go`): the leader sends heartbeats
  every 50 ms and backtracks `nextIndex` when a follower rejects. An entry is
  committed once a majority has it **and** it is from the current term.
- **Batching** (`internal/raft/batch.go`): `Propose` queues commands; a loop
  collects them for about 1 ms (up to 256) and writes the whole batch with one
  WAL `fsync`.
- **WAL** (`internal/wal`): records are `length | crc32 | term | index | data`.
  `ReadAll` stops at the first bad CRC, because a crash can cut a write short.
- **Persistence** (`internal/raft/persist.go`): term and vote are saved to
  `state.json`; log entries live in the WAL. A new leader commits an empty
  no-op entry so entries from earlier terms are applied after a restart.
- **Forwarding** (`internal/node/forward.go`): a follower sends the command to
  the leader's `Forward` RPC and returns the result.

## Benchmarks

Full table and notes in [`bench/results.md`](bench/results.md). Summary
(Windows, Docker Desktop, 3 nodes, `redis-benchmark -c 50 -d 64`):

| Setup | SET ops/s | GET ops/s |
|---|---|---|
| Redis (no persistence), P=16 | 588,235 | 641,026 |
| Redis (`appendfsync always`), P=16 | 76,864 | 518,135 |
| Ours, Raft + WAL, P=16 | 2,738 | 458,716 |

GET is competitive because reads come from memory using all cores. SET is much
slower than a single in-memory Redis, which is expected: every write goes
through an election, one fsync on the leader, one on each follower and gRPC
round trips. Here fsync is the bottleneck (about 16 ms per call on this Docker
volume), so throughput is roughly `batch size / fsync time`; with more
concurrent clients it reached about 6,658 SET/s at `-c 200`. On Linux/WSL2,
where fsync is far cheaper, it should be much faster.

Run them yourself:

```sh
sh bench/run.sh
```

## Failover

```sh
go run ./cmd/client -addrs localhost:6380,localhost:6381,localhost:6382
# in another shell:
docker compose -f deploy/docker-compose.yml kill node1
```

The client switches nodes on error and prints the longest gap between
successful writes. In testing a leader kill caused a **608 ms** gap, under the
1 second target.

## Design notes and trade-offs

- **Reads are served from the local store.** This is fast and uses every core,
  but a follower's read can be slightly stale during a leader change. This is
  the trade-off we accepted; a linearizable read would need to go through the
  leader or use a read index.
- **Only `SET` and `DEL` go through Raft.** `GET` and `PING` never touch the log.
- **Durability first.** The WAL is fsynced before a leader replies, so an
  acknowledged write survives a crash of a majority.
- **No snapshots or membership changes.** The log grows without bound and the
  peer set is fixed at startup.

## Future work

- Snapshotting to compact the log.
- Dynamic cluster membership.
- Linearizable reads.
- Batch follower fsyncs to cut write latency further.

## Tests

```sh
make test    # unit tests for store, resp and wal
make race    # go test -race ./...
```

The race detector needs a C toolchain; on Windows it is easiest to run it in
Docker:

```sh
docker run --rm -v "$PWD:/src" -w /src golang:1.23 go test -race ./...
```
