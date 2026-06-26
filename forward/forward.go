package forward

import (
	"net"

	"github.com/BaiMeow/udp2mctcp/mctcp"
	"go.uber.org/zap"
)

func Mctcp2Udp(r mctcp.Reader, conn *net.UDPConn) error {
	for {
		buf, err := r.Read()
		if err != nil {
			return err
		}
		zap.L().Debug("mctcp->", zap.Int("len", len(buf)))
		_, err = conn.Write(buf)
		if err != nil {
			return err
		}
		zap.L().Debug("->udp", zap.Int("len", len(buf)))
	}
}

func Udp2Mctcp(conn *net.UDPConn, w mctcp.Writer) error {
	writeErr := make(chan error, 1)
	for {
		buf := make([]byte, 1600)
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			return err
		}
		zap.L().Debug("udp->", zap.Int("len", n))
		buf = buf[:n]

		// check writer ok
		select {
		case err := <-writeErr:
			return err
		default:
		}

		go func() {
			err := w.Write(buf[:n])
			if err != nil {
				writeErr <- err
			}
			zap.L().Debug("->mctcp", zap.Int("len", n))
		}()
	}
}
