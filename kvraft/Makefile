.PHONY: proto build test race up down bench fmt vet clean

# Generate the protobuf and gRPC Go files from proto/raft.proto.
proto:
	protoc --proto_path=proto \
		--go_out=. --go_opt=module=kvraft \
		--go-grpc_out=. --go-grpc_opt=module=kvraft \
		proto/raft.proto

build:
	go build ./...

test:
	go test ./...

race:
	go test -race ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

up:
	docker compose -f deploy/docker-compose.yml up --build

down:
	docker compose -f deploy/docker-compose.yml down

bench:
	sh bench/run.sh

clean:
	rm -rf data
