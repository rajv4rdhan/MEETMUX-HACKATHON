# Benchmark results

Measured with Docker Desktop on Windows, 3 nodes plus a Redis container on the
same machine. `redis-benchmark -n 100000 -c 50 -d 64`, with and without
`-P 16` pipelining. Commands go to the leader; a follower would forward them.

| Setup | SET ops/s | GET ops/s | SET p99 | GET p99 |
|---|---|---|---|---|
| Redis (default, no persistence), P=16 | 694,445 | 699,301 | 2.1 ms | 2.1 ms |
| Redis (default, no persistence), P=1 | 53,333 | 53,050 | 1.5 ms | 1.5 ms |
| Redis (`appendfsync always`), P=16 | 76,336 | 552,486 | 23.2 ms | 2.9 ms |
| Redis (`appendfsync always`), P=1 | 6,297 | 42,699 | 19.4 ms | 1.8 ms |
| Ours, 3 nodes, Raft + WAL, P=16 | 1,146 | 558,659 | 782.8 ms | 3.9 ms |
| Ours, 3 nodes, Raft + WAL, P=1 | 1,161 | 38,052 | 56.6 ms | 1.7 ms |
| Ours, 3 nodes, Raft + WAL, P=1, c=200 | 3,983 | - | 68.7 ms | - |

## What the numbers say

- **GET is competitive.** Reads come from the in-memory store using all cores,
  so GET lands in the same range as Redis.
- **SET is much slower.** Every write needs a fsync on the leader and on each
  follower, plus gRPC round trips. On this Docker Desktop volume one fsync
  takes about 16 ms, and the log is synced on a 10 ms tick, so writes wait for
  those ticks. That is the price of a replicated, durable log.
- **Batching works.** More concurrent clients make bigger batches and raise
  throughput: at `-c 200` the same SET test reached **3,983 ops/s**.

## Failover test

`cmd/client` writes as fast as it can and switches nodes on error.

```
connected to localhost:6382
switched to localhost:6380 after error: EOF
switched to localhost:6381 after error: server replied "-ERR not leader"
1000 writes ok (node localhost:6381, max gap 652.6729ms)
```

Killing the leader while the client was connected to it caused a **652 ms**
gap, under the 1 second target.

## Note on the environment

The plan asks for WSL2 numbers, but this machine only has the `docker-desktop`
WSL distro, so the run happened in Docker Desktop. fsync is far more expensive
there than on a normal Linux filesystem, which is exactly the cost SET pays.
On a Linux host or a full WSL2 distro the SET numbers should be much higher.

## Reproduce

```sh
sh bench/run.sh
```

Set `N`, `CLIENTS` and `PIPE` to change the load, for example
`N=1000000 PIPE=1 sh bench/run.sh`.
