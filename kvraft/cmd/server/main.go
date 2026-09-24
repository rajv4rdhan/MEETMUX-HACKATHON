// Command server runs one node of the key-value cache.
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

	n := node.New(cfg)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
