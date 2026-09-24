package main

import (
	"log"

	"kvraft/internal/config"
	"kvraft/internal/node"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	n, err := node.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(n.Run())
}
