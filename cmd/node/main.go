package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"p2p-overlay/internal/config"
	"p2p-overlay/internal/node"
)

func main() {
	path := "internal/config/config.yaml"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	cfg, err := config.Load(path)
	if err != nil {
		log.Fatal(err)
	}
	n, err := node.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := n.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
