// Command server runs one node of the key-value cache.
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"kvraft/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("node %d starting: resp=%s raft=%s peers=%v", cfg.ID, cfg.RespAddr, cfg.RaftAddr, cfg.Peers)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Printf("node %d shutting down", cfg.ID)
}
