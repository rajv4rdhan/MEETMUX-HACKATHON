// Package resp implements a small server for the Redis serialization protocol.
package resp

import (
	"bufio"
	"errors"
	"net"
)

// Handler turns a parsed command into a reply.
type Handler interface {
	Handle(args []string) Reply
}

// Server accepts RESP clients and dispatches their commands to a Handler.
type Server struct {
	addr    string
	handler Handler
	ln      net.Listener
}

// NewServer creates a RESP server that listens on addr.
func NewServer(addr string, handler Handler) *Server {
	return &Server{addr: addr, handler: handler}
}

// ListenAndServe accepts connections until the listener is closed.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.handleConn(conn)
	}
}

// Close stops the listener and unblocks ListenAndServe.
func (s *Server) Close() error {
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}

// handleConn serves one client connection until it goes away.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		args, err := ReadCommand(reader)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}

		reply := s.handler.Handle(args)
		if _, err := writer.Write(reply); err != nil {
			return
		}
		// Flush only when no more commands are buffered, so pipelined
		// commands share one write.
		if reader.Buffered() == 0 {
			if err := writer.Flush(); err != nil {
				return
			}
		}
	}
}
