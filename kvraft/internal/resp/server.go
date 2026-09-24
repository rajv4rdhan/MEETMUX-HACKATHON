package resp

import (
	"bufio"
	"net"
)

type Handler interface {
	Handle(args []string) Reply
}

type Server struct {
	addr    string
	handler Handler
}

func NewServer(addr string, handler Handler) *Server {
	return &Server{addr: addr, handler: handler}
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

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
