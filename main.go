package main

import (
	"context"
	"net"
	"net/netip"
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
	logLevel   string
)

func main() {
	flag.StringVarP(&listenUrl, "listen", "l", "", "udp://addr:port or mctcp://addr:port")
	flag.StringVarP(&forwardUrl, "forward", "f", "", "udp://addr:port or mctcp://addr:port")
	flag.StringVar(&logLevel, "log-level", "info", "log level")
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
	if parsedListenUrl.Scheme == "mctcp" && parsedForwardUrl.Scheme == "udp" {
		conn, err := net.Dial("udp", parsedForwardUrl.Host)
		if err != nil {
			zap.L().Fatal("dial udp", zap.Error(err))
		}
		udpConn := conn.(*net.UDPConn)
		mctcpConn, err := mctcp.NewServer(ctx, 16, parsedListenUrl.Host)
		if err != nil {
			zap.L().Fatal("create mctcp server", zap.Error(err))
		}
		// mctcp -> udp
		go func() {
			err := forward.Mctcp2Udp(mctcpConn, udpConn)
			zap.L().Fatal("mctcp2udp fail", zap.Error(err))
			cancel()
		}()
		// udp -> mctcp
		go func() {
			err := forward.Udp2Mctcp(udpConn, mctcpConn)
			zap.L().Fatal("udp2mctcp fail", zap.Error(err))
			cancel()
		}()
	} else if parsedListenUrl.Scheme == "udp" && parsedForwardUrl.Scheme == "mctcp" {
		addrPort, err := netip.ParseAddrPort(parsedListenUrl.Host)
		if err != nil {
			zap.L().Fatal("parse addr port fail", zap.Error(err), zap.String("addr", parsedListenUrl.Host))
		}
		laddr := &net.UDPAddr{
			IP:   addrPort.Addr().AsSlice(),
			Port: int(addrPort.Port()),
		}
		listener, err := net.ListenUDP("udp", laddr)
		if err != nil {
			zap.L().Fatal("dial udp", zap.Error(err))
		}
		var buf [1500]byte
		_, raddr, err := listener.ReadFromUDP(buf[:])
		zap.L().Info("get remote udp addr", zap.String("addr", raddr.String()))
		if err != nil {
			zap.L().Fatal("read first udp fail", zap.Error(err))
		}
		listener.Close()
		udpConn, err := net.DialUDP("udp", laddr, raddr)
		if err != nil {
			zap.L().Fatal("dial udp", zap.Error(err))
		}
		mctcpConn, err := mctcp.NewClient(ctx, 16, parsedForwardUrl.Host)
		if err != nil {
			zap.L().Fatal("create mctcp server", zap.Error(err))
		}
		// mctcp -> udp
		go func() {
			err := forward.Mctcp2Udp(mctcpConn, udpConn)
			zap.L().Fatal("mctcp2udp fail", zap.Error(err))
			cancel()
		}()
		// udp -> mctcp
		go func() {
			err := forward.Udp2Mctcp(udpConn, mctcpConn)
			zap.L().Fatal("udp2mctcp fail", zap.Error(err))
			cancel()
		}()
	} else {
		zap.L().Fatal("listen and forward must be one mctcp and one udp")
	}

	zap.L().Info("setup done")
	<-ctx.Done()
}
