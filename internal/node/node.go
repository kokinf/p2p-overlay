package node

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"p2p-overlay/internal/protocol"
	"p2p-overlay/internal/transport"
)

type Node struct {
	ID     string
	Port   int
	server *transport.Server
}

func NewNode(id string, port int) (*Node, error) {
	srv, err := transport.NewServer(fmt.Sprintf(":%d", port), 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &Node{ID: id, Port: port, server: srv}, nil
}

func (n *Node) Run(ctx context.Context) error {
	for {
		conn, err := n.server.Accept(ctx)
		if err != nil {
			return err
		}
		go n.handleConn(conn)
	}
}

func (n *Node) handleConn(conn net.Conn) {
	defer conn.Close()
	dec := protocol.NewStreamDecoder(conn)
	for {
		frame, err := dec.ReadFrame()
		if err != nil {
			log.Printf("node %s: read error: %v", n.ID, err)
			return
		}
		log.Printf("node %s: frame type=%d req=%x len=%d",
			n.ID, frame.Type, frame.RequestID[:4], len(frame.Payload))
		resp := &protocol.Frame{
			Version:   frame.Version,
			Type:      protocol.FramePong,
			RequestID: frame.RequestID,
			Payload:   []byte("pong"),
		}
		if err := resp.Encode(conn); err != nil {
			log.Printf("node %s: write error: %v", n.ID, err)
			return
		}
	}
}

func (n *Node) Stop() error {
	return n.server.Close()
}
