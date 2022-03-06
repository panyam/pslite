package core

import (
	"fmt"
	"io"
)

/**
 * A Message implementation for representing published messages.
 */
type PubMsg struct {
	message   []byte
	timestamp int64

	seekOffset int64
}

func (m *PubMsg) Reader() io.Reader {
	return m
}

func (m *PubMsg) Length() int64 {
	return int64(len(m.message))
}

func (m *PubMsg) Timestamp() int64 {
	return m.timestamp
}

func (m *PubMsg) Read(p []byte) (n int, err error) {
	remaining := int(m.Length() - m.seekOffset)
	if remaining <= 0 {
		return 0, io.EOF
	}
	n = len(p)
	if remaining < n {
		n = remaining
	}
	copy(m.message[m.seekOffset:n], p)
	m.seekOffset += int64(n)
	return
}

func (m *PubMsg) Seek(offset int64, whence int) (newOffset int64, err error) {
	newOffset = 0
	if whence == io.SeekStart {
		newOffset = offset
	} else if whence == io.SeekEnd {
		newOffset = m.Length() + offset
	} else {
		newOffset = m.seekOffset + offset
	}
	if newOffset < 0 {
		err = fmt.Errorf("Invalid seek offset")
	} else {
		m.seekOffset = newOffset
	}
	return
}
