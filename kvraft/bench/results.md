# Benchmark results

Measured on Windows with Docker Desktop (WSL2 backend), 3 nodes plus a Redis
container on the same machine. `redis-benchmark -n 100000 -c 50 -d 64`, with
and without `-P 16` pipelining. All commands were sent to one node; if it was a
follower the write was forwarded to the leader.

| Setup | SET ops/s | GET ops/s | SET p99 | GET p99 |
|---|---|---|---|---|
| Redis (default, no persistence), P=16 | 588,235 | 641,026 | 4.0 ms | 2.1 ms |
| Redis (default, no persistence), P=1 | 45,579 | 50,429 | 1.8 ms | 1.5 ms |
| Redis (`appendfsync always`), P=16 | 76,864 | 518,135 | 20.5 ms | 2.9 ms |
| Redis (`appendfsync always`), P=1 | 6,321 | 33,146 | 21.1 ms | 2.6 ms |
| Ours, 3 nodes, Raft + WAL, P=16 | 2,738 | 458,716 | 412.7 ms | 2.4 ms |
| Ours, 3 nodes, Raft + WAL, P=1 | 2,804 | 29,516 | 29.8 ms | 2.9 ms |

## What the numbers say

- **GET is competitive.** Reads come straight from the in-memory store and the
  Go server uses all cores, so GET lands in the same range as Redis.
- **SET is much slower than in-memory Redis, and slower than Redis with
  `appendfsync always`.** Every write goes through leader election, one fsync
  on the leader, one fsync on each follower and gRPC round trips. That is the
  price of a replicated, durable log.
- **fsync is the bottleneck here.** 100 fsyncs on a Docker Desktop volume take
  about 1.6 s (~16 ms each). Each batch of proposals needs one fsync, so
  throughput is roughly `batch size / fsync time`.
- **Batching works.** More concurrent clients make bigger batches and raise
  throughput: the same SET test with `-c 200` instead of `-c 50` reached about
  **6,658 ops/s**. On Linux/WSL2, where fsync is far cheaper, this should be
  much higher.

## Failover test

`cmd/client` writes as fast as it can and switches nodes on error.

```
connected to localhost:6382
switched to localhost:6380 after error: EOF
1000 writes ok (node localhost:6380, max gap 608.3487ms)
```

Killing the leader while the client was connected to it caused a **608 ms**
gap, under the 1 second target. The write path retries after a short timeout,
by which point a new leader has usually been elected.

## Reproduce

```sh
sh bench/run.sh
```

Set `N`, `CLIENTS` and `PIPE` to change the load, for example
`N=1000000 PIPE=1 sh bench/run.sh`.
