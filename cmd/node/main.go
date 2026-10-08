package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"p2p-overlay/internal/config"
	"p2p-overlay/internal/node"
)

func main() {
	cfg, err := config.Load("internal/config/config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	n, err := node.NewNode("node-1", cfg.Node.Port)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	go func() {
		if err := n.Run(ctx); err != nil {
			log.Printf("node stopped: %v", err)
		}
	}()
	<-ctx.Done()
	_ = n.Stop()
}
