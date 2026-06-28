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
		_, err = conn.Write(buf)
		if err != nil {
			return err
		}
	}
}

func Udp2Mctcp(conn *net.UDPConn, w mctcp.Writer) error {
	for {
		buf := make([]byte, 1500)
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			return err
		}
		buf = buf[:n]

		err = w.Write(buf)
		if err != nil {
			zap.L().Error("write error", zap.Error(err))
		}
	}
}
