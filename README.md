# MEETMUX-HACKATHON

This project is called **kvraft**. It is inside the `kvraft/` folder.

## What it is

kvraft is a small key-value store (like a tiny Redis) that runs on 3 nodes at the same time.

- You can `set` a key and `get` it back, just like Redis.
- It speaks the **Redis protocol**, so normal tools like `redis-cli` and
  `redis-benchmark` work with it.
- All 3 nodes hold the same data. If one node dies, the other two keep working.
- It saves data to disk, so if a node restarts it does not lose its data.

In one line: **3 Redis-like servers that agree on the same data and survive a
node dying.**

## How to run

You need **Go 1.23+**. Docker is optional but easier.

### Run the tests

Open a terminal in the `kvraft` folder and run:

```sh
go test ./...
```

### Run 3 nodes on your own machine

Open **3 terminals**, all inside the `kvraft` folder. Run one command in each:

```sh
go run ./cmd/server -id=1 -resp=:6380 -raft=:5001 -data=data/1
```

```sh
go run ./cmd/server -id=2 -resp=:6381 -raft=:5002 -data=data/2
```

```sh
go run ./cmd/server -id=3 -resp=:6382 -raft=:5003 -data=data/3
```

Now try it with `redis-cli`:

```sh
redis-cli -p 6380 set a 1     # write on the leader
redis-cli -p 6381 get a       # read from another node, returns 1
redis-cli -p 6382 set b 2     # write to a follower, still works
```

Try killing the leader with `Ctrl+C`. After about 1 second a new leader is
picked and `set`/`get` still work. Start the node again and its data is back.

### Run with Docker (easier)

Inside the `kvraft` folder:

```sh
docker compose -f deploy/docker-compose.yml up --build
```

Stop it with:

```sh
docker compose -f deploy/docker-compose.yml down
```

If you have `make`, you can also use `make up`, `make down` and `make test`.

### Test failover

The client keeps writing and tells you the biggest pause when a node dies:

```sh
go run ./cmd/client
```

While it runs, stop the leader (Ctrl+C) and watch the "max gap" it prints.

### Benchmarks

```sh
sh bench/run.sh
```

This compares our store with Redis using `redis-benchmark`. Run it on Linux or
WSL2, because disk sync is slow on Windows.

## A little about the internals

Each node is made of a few small parts:

- **resp** - reads and writes the Redis protocol over TCP (ports 6380-6382).
- **store** - the actual key-value data, kept in memory in 16 small maps so
  many readers do not block each other.
- **raft** - the part that makes all 3 nodes agree. It picks one leader, and
  the leader copies every write to the other nodes.
- **wal** - a "write-ahead log" on disk. Every change is written here first, so
  a node can rebuild its data after a crash or restart.
- **node** - glue that connects all of the above.

How a write flows:

1. A client sends `SET a 1` to any node.
2. If it is not the leader, the node forwards it to the leader.
3. The leader writes it to its log, saves it to disk, and copies it to the
   other nodes.
4. When most nodes have it, it is "committed" and applied to the store.
5. The client gets `OK`.

Reads (`GET`) are answered straight from memory, so they are fast, but a
follower's copy can be a tiny bit old right after a leader change.

Nodes talk to each other using **gRPC**. Clients talk to nodes using the
**Redis protocol**. The Raft part does not know about keys or values; it only
copies raw bytes, which keeps it simple and easy to test.

