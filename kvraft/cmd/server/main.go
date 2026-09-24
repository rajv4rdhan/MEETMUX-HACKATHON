// Command server runs one node of the key-value cache.
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

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

	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
		<-stop
		log.Printf("node %d shutting down", cfg.ID)
		if err := n.Shutdown(); err != nil {
			log.Printf("node %d shutdown: %v", cfg.ID, err)
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
