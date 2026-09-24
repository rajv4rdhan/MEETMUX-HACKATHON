#!/bin/sh
# Benchmarks for the kvraft cluster.
#
# redis-benchmark runs inside the redis container so it shares the Docker
# network with the nodes. fsync is much slower on Windows than on Linux, so
# run this on a Linux host or in WSL2 for comparable numbers.
#
# Usage: sh bench/run.sh

set -e

COMPOSE="docker compose -f deploy/docker-compose.yml"
N=${N:-200000}
CLIENTS=${CLIENTS:-50}
PIPE=${PIPE:-16}

# run <label> <host> <port> [extra redis-benchmark flags...]
run() {
	label=$1
	host=$2
	port=$3
	shift 3
	echo "== $label =="
	$COMPOSE exec -T redis redis-benchmark -h "$host" -p "$port" \
		-t set,get -n "$N" -c "$CLIENTS" -d 64 -q "$@"
	echo
}

$COMPOSE up -d --build node1 node2 node3 redis
echo "waiting for the cluster to elect a leader..."
sleep 5

run "Redis default, pipelined" redis 6379 -P "$PIPE"
run "Redis default, no pipeline" redis 6379

# A second Redis with the strongest durability setting, for a fair write test.
docker run -d --rm --name kvraft-redis-durable --network deploy_default \
	redis:7-alpine redis-server --appendonly yes --appendfsync always >/dev/null
sleep 2
run "Redis appendfsync always, pipelined" kvraft-redis-durable 6379 -P "$PIPE"
run "Redis appendfsync always, no pipeline" kvraft-redis-durable 6379
docker stop kvraft-redis-durable >/dev/null

run "kvraft 3 nodes, pipelined" node1 6380 -P "$PIPE"
run "kvraft 3 nodes, no pipeline" node1 6380
