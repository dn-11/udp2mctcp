package main

import (
	"context"
	"net"
	"net/url"

	"github.com/BaiMeow/udp2mctcp/forward"
	"github.com/BaiMeow/udp2mctcp/log"
	"github.com/BaiMeow/udp2mctcp/mctcp"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	listenUrl  string
	forwardUrl string
	connCount  int
	logLevel   string
)

func main() {
	flag.StringVarP(&listenUrl, "listen", "l", "", "udp://addr:port or mctcp://addr:port")
	flag.StringVarP(&forwardUrl, "forward", "f", "", "udp://addr:port or mctcp://addr:port")
	flag.IntVarP(&connCount, "tcp-connections", "c", 8, "tcp connection count used by mctcp")
	flag.StringVarP(&logLevel, "log-level", "log", "info", "log level")
	flag.Parse()
	level, err := zapcore.ParseLevel(logLevel)
	if err != nil {
		level = zapcore.InfoLevel
	}
	log.Init(level)
	if err != nil {
		zap.L().Warn("prase log level fail, use info", zap.Error(err), zap.String("arg", logLevel))
	}
	parsedListenUrl, err := url.Parse(listenUrl)
	if err != nil {
		zap.L().Fatal("invalid listen url", zap.String("url", listenUrl))
	}
	parsedForwardUrl, err := url.Parse(forwardUrl)
	if err != nil {
		zap.L().Fatal("invalid forward url", zap.String("url", forwardUrl))
	}
	ctx, cancel := context.WithCancel(context.Background())
	var (
		udpConn   *net.UDPConn
		mctcpConn *mctcp.Server
	)

	if parsedListenUrl.Scheme == "mctcp" && parsedForwardUrl.Scheme == "udp" {
		conn, err := net.Dial("udp", parsedForwardUrl.Host)
		if err != nil {
			zap.L().Fatal("dial udp", zap.Error(err))
		}
		udpConn = conn.(*net.UDPConn)
		mctcpConn, err = mctcp.NewServer(ctx, 16, 4096, parsedListenUrl.Host)
		if err != nil {
			zap.L().Fatal("create mctcp server", zap.Error(err))
		}
	} else if parsedListenUrl.Scheme == "udp" && parsedForwardUrl.Scheme == "mctcp" {
		conn, err := net.Dial("udp", parsedListenUrl.Host)
		if err != nil {
			zap.L().Fatal("dial udp", zap.Error(err))
		}
		udpConn = conn.(*net.UDPConn)
		mctcpConn, err = mctcp.NewServer(ctx, 16, 4096, parsedForwardUrl.Host)
		if err != nil {
			zap.L().Fatal("create mctcp server", zap.Error(err))
		}
	} else {
		zap.L().Fatal("listen and forward must be one mctcp and one udp")
	}

	// mctcp -> udp
	go func() {
		err := forward.Mctcp2Udp(mctcpConn, udpConn)
		zap.L().Fatal("mctcp2udp fail", zap.Error(err))
		cancel()
	}()
	zap.L().Info("run mctcp -> udp")

	// udp -> mctcp
	go func() {
		err := forward.Udp2Mctcp(udpConn, mctcpConn)
		zap.L().Fatal("udp2mctcp fail", zap.Error(err))
		cancel()
	}()
	zap.L().Info("run udp -> mctcp")
	zap.L().Info("setup done")
	<-ctx.Done()
}
