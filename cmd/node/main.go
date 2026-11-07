package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"p2p-overlay/internal/config"
	"p2p-overlay/internal/identity"
	"p2p-overlay/internal/node"
)

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "gen-identities" {
		runGenIdentities(os.Args[2:])
		return
	}

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

func runGenIdentities(args []string) {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: node gen-identities <N> <dir>")
		os.Exit(1)
	}
	count, err := strconv.Atoi(args[0])
	if err != nil || count <= 0 {
		fmt.Fprintln(os.Stderr, "invalid N:", args[0])
		os.Exit(1)
	}
	dir := args[1]
	for i := 0; i < count; i++ {
		id, err := identity.LoadOrCreate(filepath.Join(dir, fmt.Sprintf("node-%d", i)))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(id.NodeID().String())
	}
}
