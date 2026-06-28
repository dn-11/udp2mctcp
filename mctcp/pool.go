package mctcp

import (
	"context"
	"net"
	"sync/atomic"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	connStateAllInuse = iota
	connStateReadInuse
	connStateUninitialized
)

type Pool struct {
	size int32

	storage     []*net.TCPConn
	connState   []atomic.Int32
	pendingInit chan int
	readBuffer  chan []byte
	writeBuffer chan []byte

	writeCursor      atomic.Int32
	newFunc          func() *net.TCPConn
	writeDropLimiter *rate.Limiter
	readDropLimiter  *rate.Limiter
}

func NewPool(size int32, create func() *net.TCPConn) *Pool {
	p := &Pool{
		size:        size,
		storage:     make([]*net.TCPConn, size),
		connState:   make([]atomic.Int32, size),
		readBuffer:  make(chan []byte, 512),
		writeBuffer: make(chan []byte, 512),
		pendingInit: make(chan int, size),
	}
	p.newFunc = create
	dropRate := 10000
	dropBurst := 512
	p.writeDropLimiter = rate.NewLimiter(rate.Limit(dropRate), dropBurst)
	p.readDropLimiter = rate.NewLimiter(rate.Limit(dropRate), dropBurst)
	// init
	for i := range p.storage {
		p.connState[i].Store(connStateUninitialized)
		p.pendingInit <- i
	}
	zap.L().Debug("new pool", zap.Int32("size", size))
	go p.spawnerThread()
	go p.writeThread()
	return p
}

func (p *Pool) spawnerThread() {
	for i := range p.pendingInit {
		p.storage[i] = p.newFunc()
		go p.readThread(i)
		p.connState[i].Store(connStateReadInuse)
	}
}

func (p *Pool) readThread(idx int) {
	defer func() {
		for !p.connState[idx].CompareAndSwap(connStateReadInuse, connStateUninitialized) {
		}
		p.pendingInit <- idx
	}()
	for {
		pk, err := Stream2Packet(p.storage[idx])
		zap.L().Debug("tcp read", zap.Int("len", len(pk)))
		if err != nil {
			zap.L().Error("read failed", zap.Int("idx", idx), zap.Error(err))
			return
		}
		select {
		case p.readBuffer <- pk:
		default:
			_ = p.readDropLimiter.Wait(context.Background())
		}
	}
}

func (p *Pool) writeThread() {
	for pk := range p.writeBuffer {
		var pks [][]byte
		pks = append(pks, pk)
		for i := 0; i < 7; i++ {
			select {
			case pk2, ok := <-p.writeBuffer:
				if !ok {
					return
				}
				pks = append(pks, pk2)
			default:
				break
			}
		}
		var next int32
		for {
			c := p.writeCursor.Load()
			next = (c + 1) % p.size
			if !p.writeCursor.CompareAndSwap(c, next) {
				continue
			}
			if !p.connState[next].CompareAndSwap(connStateReadInuse, connStateAllInuse) {
				continue
			}
			break
		}
		err := BatchPacket2Stream(pks, p.storage[next])
		var totalLen int
		for _, pk := range pks {
			totalLen += len(pk)
		}
		zap.L().Debug("tcp write", zap.Int("len", totalLen))
		for !p.connState[next].CompareAndSwap(connStateAllInuse, connStateReadInuse) {
		}
		if err != nil {
			zap.L().Warn("write fail", zap.Int("idx", int(next)), zap.Error(err))
		}
	}
}

func (p *Pool) Read() []byte {
	return <-p.readBuffer
}

func (p *Pool) Write(pk []byte) {
	select {
	case p.writeBuffer <- pk:
	default:
		_ = p.writeDropLimiter.Wait(context.Background())
	}
}
