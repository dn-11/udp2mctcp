package mctcp

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPool_Read(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	var t1 time.Time
	go func() {
		conn, err := listener.Accept()
		assert.NoError(t, err)
		t1 = time.Now()
		_, err = conn.Write([]byte{0, 5, 'h', 'e', 'l', 'l', 'o', 0, 2, 'o', 'k'})
		assert.NoError(t, err)
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	assert.NoError(t, err)
	p := NewPool(1, func() *net.TCPConn {
		return conn.(*net.TCPConn)
	})
	assert.Equal(t, []byte("hello"), p.Read())
	assert.Equal(t, []byte("ok"), p.Read())
	t.Log("time cost:", time.Now().Sub(t1))
}

func TestPool_Write(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	var t1 time.Time
	p := NewPool(1, func() *net.TCPConn {
		conn, err := net.Dial("tcp", listener.Addr().String())
		assert.NoError(t, err)
		t1 = time.Now()
		return conn.(*net.TCPConn)
	})
	go func() {
		p.Write([]byte("hello"))
		p.Write([]byte("ok"))
	}()
	conn, err := listener.Accept()
	assert.NoError(t, err)
	var buf [11]byte
	_, err = io.ReadFull(conn, buf[:])
	assert.NoError(t, err)
	assert.Equal(t, []byte{0, 5, 'h', 'e', 'l', 'l', 'o', 0, 2, 'o', 'k'}, buf[:])
	t.Log("time cost:", time.Now().Sub(t1))
}
