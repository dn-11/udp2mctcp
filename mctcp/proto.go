package mctcp

import (
	"encoding/binary"
	"errors"
	"io"
)

const HeaderLen = 2

func Stream2Packet(r io.Reader) ([]byte, error) {
	var lenRaw [HeaderLen]byte
	if _, err := io.ReadFull(r, lenRaw[:]); err != nil {
		return nil, errors.Join(ErrBrokenConn, err)
	}
	contentLen := binary.BigEndian.Uint16(lenRaw[:])
	contentRaw := make([]byte, contentLen)
	if _, err := io.ReadFull(r, contentRaw); err != nil {
		return nil, errors.Join(ErrBrokenConn, err)
	}
	return contentRaw, nil
}

func Packet2Stream(data []byte, w io.Writer) error {
	buf := make([]byte, len(data)+2)
	if len(buf) > 65535 {
		return errors.New("packet too long")
	}
	binary.BigEndian.PutUint16(buf[:], uint16(len(data)))
	copy(buf[2:], data)

	n, err := w.Write(buf)
	if err != nil {
		return errors.Join(ErrBrokenConn, err)
	}
	if n != len(buf) {
		return errors.Join(ErrBrokenConn, errors.New("write not complete"))
	}
	return nil
}

func BatchPacket2Stream(data [][]byte, w io.Writer) error {
	totalLen := len(data) * 2
	for _, pk := range data {
		if len(pk) > 65535 {
			return errors.New("some packet too long")
		}
		totalLen += len(pk)
	}
	buf := make([]byte, totalLen)
	var i int
	for _, pk := range data {
		binary.BigEndian.PutUint16(buf[i:], uint16(len(pk)))
		i += 2
		copy(buf[i:], pk)
		i += len(pk)
	}
	n, err := w.Write(buf)
	if err != nil {
		return errors.Join(ErrBrokenConn, err)
	}
	if n != len(buf) {
		return errors.Join(ErrBrokenConn, errors.New("write not complete"))
	}
	return nil
}
