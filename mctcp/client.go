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

	pool *TcpPool
}

func NewClient(ctx context.Context, size int, readBufferSize int, addr string) (*Client, error) {
	c := new(Client)
	c.size = size
	c.dialAddr = addr
	c.dialRateLimiter = rate.NewLimiter(rate.Every(time.Second/4), 4)
	c.ctx = ctx
	c.pool = NewPool(ctx, size, readBufferSize)
	c.pool.RegisterTcpReadFailed(func(err error) {
		if errors.Is(err, ErrBrokenConn) {
			zap.L().Debug("read connection broken", zap.Error(err))
			c.counterLock.Lock()
			defer c.counterLock.Unlock()
			c.readBrokenCounter++
			if c.readBrokenCounter > c.resumeCounter {
				c.resumeCounter++
				zap.L().Debug("try add connection",
					zap.Int("resumeCounter", c.resumeCounter),
					zap.Int("readBrokenCounter", c.readBrokenCounter),
				)
				go func() {
					_ = utils.Retry(func() error {
						return c.addConn()
					}, 3)
				}()
			}
		}

	})
	for range size {
		if err := c.dialRateLimiter.Wait(ctx); err != nil {
			return nil, err
		}
		tconn, err := createConnection(addr)
		if err != nil {
			return nil, fmt.Errorf("create connection failed: %v", err)
		}
		c.pool.Push(tconn)
	}
	return c, nil
}

// Write select an available conn and then write data to it
func (c *Client) Write(buf []byte) error {
	if c.Closed() {
		return ErrClosed
	}

	err := c.pool.Write(buf)
	if errors.Is(err, ErrBrokenConn) {
		zap.L().Debug("write connection broken")
		c.counterLock.Lock()
		defer c.counterLock.Unlock()
		c.writeBrokenCounter++
		if c.writeBrokenCounter > c.resumeCounter {
			c.resumeCounter++
			zap.L().Debug("try add connection",
				zap.Int("resumeCounter", c.resumeCounter),
				zap.Int("writeBrokenCounter", c.writeBrokenCounter),
			)
			go func() {
				_ = utils.Retry(func() error {
					return c.addConn()
				}, 3)
			}()
		}
		return nil
	}
	return err
}

func (c *Client) Read() ([]byte, error) {
	if c.Closed() {
		return nil, ErrClosed
	}
	return c.pool.Read()
}

func (c *Client) Closed() bool {
	select {
	case <-c.ctx.Done():
		return true
	default:
		return false
	}
}

func (c *Client) addConn() error {
	if err := c.dialRateLimiter.Wait(c.ctx); err != nil {
		return err
	}
	tconn, err := createConnection(c.dialAddr)
	if err != nil {
		return fmt.Errorf("create connection failed: %v", err)
	}
	zap.L().Info("new connection",
		zap.String("conn", fmt.Sprintf("%s <-> %s",
			tconn.LocalAddr().String(),
			tconn.RemoteAddr().String(),
		)))
	c.pool.Push(tconn)
	return nil
}

func createConnection(addr string) (*net.TCPConn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	tconn := conn.(*net.TCPConn)
	if err := tconn.SetKeepAlive(true); err != nil {
		zap.L().Warn("set keepalive failed", zap.Error(err))
	}
	if err := tconn.SetNoDelay(true); err != nil {
		zap.L().Warn("set nodelay failed", zap.Error(err))
	}
	zap.L().Info("new connection", zap.String("conn", fmt.Sprintf("%s <-> %s", conn.LocalAddr().String(), conn.RemoteAddr().String())))
	return conn.(*net.TCPConn), nil
}
