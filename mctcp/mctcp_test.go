package mctcp

import (
	"context"
	"fmt"
	"net"
	_ "net/http/pprof"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// gears in Mbps to test.
var writeGears = []int{100, 200, 400, 800, 1600, 0} // 0 = unlimited

// packet sizes tested for throughput curve.
var packetSizes = []int{64, 256, 512, 1024, 1400, 2048, 4096, 8192, 16384, 32768}

func TestClientWriteThroughput(t *testing.T) {
	runtime.GOMAXPROCS(1)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dur := 3 * time.Second
	if testing.Short() {
		dur = 1 * time.Second
	}

	for _, gear := range writeGears {
		t.Run(gearName(gear), func(t *testing.T) {
			throughput := testWriteGear(t, gear, 1400, dur)
			t.Logf("gear: %s → achieved: %.0f Mbps", gearName(gear), throughput)
		})
	}
}

func TestClientWriteThroughputBySize(t *testing.T) {
	runtime.GOMAXPROCS(1)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dur := 3 * time.Second
	if testing.Short() {
		dur = 1 * time.Second
	}

	for _, sz := range packetSizes {
		t.Run(fmt.Sprintf("%dB", sz), func(t *testing.T) {
			throughput := testWriteGear(t, 0, sz, dur)
			pps := throughput * 1e6 / 8 / float64(sz+HeaderLen)
			t.Logf("size %5d → %.0f Mbps  (%.0f pps)", sz, throughput, pps)
		})
	}
}

func testWriteGear(t *testing.T, gearMbps int, packetSize int, dur time.Duration) float64 {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	var (
		receivedBytes   atomic.Int64
		receivedPackets atomic.Int64
	)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				tcpConn := conn.(*net.TCPConn)
				tcpConn.SetNoDelay(true)
				defer tcpConn.Close()
				for {
					pk, err := Stream2Packet(tcpConn)
					if err != nil {
						return
					}
					receivedBytes.Add(int64(len(pk)))
					receivedPackets.Add(1)
				}
			}()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := NewClient(ctx, 16, listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, packetSize)

	// Client dial rate limiter: 4/sec, burst 4.
	// For a pool of 16: first 4 instant, remaining 12 at 250ms each = 3s warmup.
	time.Sleep(3 * time.Second)

	var limiter *rate.Limiter
	if gearMbps > 0 {
		packetsPerSec := float64(gearMbps) * 1e6 / 8 / float64(len(payload)+HeaderLen)
		limiter = rate.NewLimiter(rate.Limit(packetsPerSec), int(packetsPerSec/10)+1)
	}

	var (
		attemptedWrites int64
		done            atomic.Bool
	)
	start := time.Now()
	time.AfterFunc(dur, func() { done.Store(true) })

	for !done.Load() {
		if limiter != nil {
			limiter.Wait(context.Background())
		}
		client.Write(payload)
		attemptedWrites++
	}

	end := time.Now()
	time.Sleep(300 * time.Millisecond)

	elapsed := end.Sub(start)
	rcvBytes := receivedBytes.Load()

	if rcvBytes == 0 {
		t.Fatal("no packets received")
	}
	return float64(rcvBytes*8) / elapsed.Seconds() / 1e6
}

var readGears = []int{100, 200, 400, 800, 1600, 0}

func TestClientReadThroughput(t *testing.T) {
	runtime.GOMAXPROCS(1)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dur := 3 * time.Second
	if testing.Short() {
		dur = 1 * time.Second
	}

	for _, gear := range readGears {
		t.Run(gearName(gear), func(t *testing.T) {
			throughput := testReadGear(t, gear, 1400, dur)
			t.Logf("gear: %s → achieved: %.0f Mbps", gearName(gear), throughput)
		})
	}
}

func TestClientReadThroughputBySize(t *testing.T) {
	runtime.GOMAXPROCS(1)
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	dur := 3 * time.Second
	if testing.Short() {
		dur = 1 * time.Second
	}

	for _, sz := range packetSizes {
		t.Run(fmt.Sprintf("%dB", sz), func(t *testing.T) {
			throughput := testReadGear(t, 0, sz, dur)
			pps := throughput * 1e6 / 8 / float64(sz+HeaderLen)
			t.Logf("size %5d → %.0f Mbps  (%.0f pps)", sz, throughput, pps)
		})
	}
}

func testReadGear(t *testing.T, gearMbps int, packetSize int, dur time.Duration) float64 {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, err := NewServer(ctx, 16, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.listener.Close()

	client, err := NewClient(ctx, 16, server.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, packetSize)

	// Client dial rate limiter: 4/sec, burst 4.
	// For a pool of 16: first 4 instant, remaining 12 at 250ms each = 3s warmup.
	time.Sleep(3 * time.Second)

	var limiter *rate.Limiter
	if gearMbps > 0 {
		packetsPerSec := float64(gearMbps) * 1e6 / 8 / float64(len(payload)+HeaderLen)
		limiter = rate.NewLimiter(rate.Limit(packetsPerSec), int(packetsPerSec/10)+1)
	}

	var (
		attemptedWrites int64
		receivedBytes   atomic.Int64
		receivedPackets atomic.Int64
		writeDone       atomic.Bool
		done            atomic.Bool
	)

	go func() {
		for !done.Load() {
			pk, err := client.Read()
			if err != nil {
				return
			}
			receivedBytes.Add(int64(len(pk)))
			receivedPackets.Add(1)
		}
	}()

	start := time.Now()
	time.AfterFunc(dur, func() { writeDone.Store(true) })

	for !writeDone.Load() {
		if limiter != nil {
			limiter.Wait(context.Background())
		}
		server.Write(payload)
		attemptedWrites++
	}

	end := time.Now()
	time.Sleep(300 * time.Millisecond)
	done.Store(true)

	elapsed := end.Sub(start)
	rcvBytes := receivedBytes.Load()

	if rcvBytes == 0 {
		t.Fatal("no packets received")
	}
	return float64(rcvBytes*8) / elapsed.Seconds() / 1e6
}

func gearName(mbps int) string {
	if mbps == 0 {
		return "unlimited"
	}
	return formatMbps(mbps)
}

func formatMbps(mbps int) string {
	if mbps >= 1000 {
		return fmt.Sprintf("%.1fGbps", float64(mbps)/1000)
	}
	return fmt.Sprintf("%dMbps", mbps)
}
