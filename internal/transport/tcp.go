package transport

import (
	"context"
	"net"
	"time"
)

type Server struct {
	listener net.Listener
	timeout  time.Duration
}

func NewServer(addr string, timeout time.Duration) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Server{listener: ln, timeout: timeout}, nil
}

func (s *Server) Accept(ctx context.Context) (net.Conn, error) {
	conn, err := s.listener.Accept()
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(s.timeout))
	return conn, nil
}

func (s *Server) Close() error {
	return s.listener.Close()
}
