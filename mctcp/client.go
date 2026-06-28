package mctcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/BaiMeow/udp2mctcp/utils"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var (
	ErrClosed     = errors.New("pool closed")
	ErrBrokenConn = errors.New("broken connection")
)

type Client struct {
	ctx             context.Context
	size            int
	dialAddr        string
	dialRateLimiter *rate.Limiter

	resumeCounter      int
	readBrokenCounter  int
	writeBrokenCounter int
	counterLock        sync.Mutex

	pool *Pool
}

func NewClient(ctx context.Context, size int, addr string) (*Client, error) {
	c := new(Client)
	c.size = size
	c.dialAddr = addr
	c.dialRateLimiter = rate.NewLimiter(rate.Every(time.Second/4), 4)
	c.ctx = ctx
	c.pool = NewPool(int32(size), c.mustCreateConn)
	return c, nil
}

// Write select an available conn and then write data to it
func (c *Client) Write(buf []byte) error {
	if c.Closed() {
		return ErrClosed
	}
	c.pool.Write(buf)
	return nil
}

func (c *Client) Read() ([]byte, error) {
	if c.Closed() {
		return nil, ErrClosed
	}
	return c.pool.Read(), nil
}

func (c *Client) Closed() bool {
	select {
	case <-c.ctx.Done():
		return true
	default:
		return false
	}
}

func (c *Client) mustCreateConn() (tconn *net.TCPConn) {
	utils.InfiniteRetry(func() error {
		_ = c.dialRateLimiter.Wait(c.ctx)
		conn, err := net.Dial("tcp", c.dialAddr)
		if err != nil {
			zap.L().Error("dial fail", zap.Error(err))
			return err
		}
		zap.L().Info("new connection", zap.String("conn", fmt.Sprintf("%s <-> %s", conn.LocalAddr().String(), conn.RemoteAddr().String())))
		tconn = conn.(*net.TCPConn)
		if err := tconn.SetKeepAlive(true); err != nil {
			zap.L().Warn("set keepalive failed", zap.Error(err))
		}
		if err := tconn.SetNoDelay(true); err != nil {
			zap.L().Warn("set nodelay failed", zap.Error(err))
		}
		return nil
	})
	return
}
