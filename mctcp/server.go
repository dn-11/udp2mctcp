package mctcp

import (
	"context"
	"fmt"
	"net"

	"go.uber.org/zap"
)

type Server struct {
	ctx        context.Context
	size       int
	listenAddr string

	listener net.Listener
	pool     *Pool
}

func NewServer(ctx context.Context, size int, addr string) (*Server, error) {
	s := new(Server)
	s.size = size
	s.listenAddr = addr
	s.ctx = ctx
	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		zap.L().Error("listen", zap.Error(err))
	}
	s.listener = listener
	s.pool = NewPool(int32(size), s.mustAcceptConn)
	return s, nil
}

func (s *Server) Read() ([]byte, error) {
	return s.pool.Read(), nil
}

func (s *Server) Write(buf []byte) error {
	s.pool.Write(buf)
	return nil
}

func (s *Server) mustAcceptConn() (tconn *net.TCPConn) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			zap.L().Error("accept fail", zap.Error(err))
			continue
		}
		zap.L().Info("new connection", zap.String("conn", fmt.Sprintf("%s <-> %s", conn.LocalAddr().String(), conn.RemoteAddr().String())))
		tconn = conn.(*net.TCPConn)
		if err := tconn.SetKeepAlive(true); err != nil {
			zap.L().Warn("set keepalive failed", zap.Error(err))
		}
		if err := tconn.SetNoDelay(true); err != nil {
			zap.L().Warn("set nodelay failed", zap.Error(err))
		}
		return
	}
}
