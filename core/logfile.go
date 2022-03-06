package core

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

/**
 * A Message implementation for representing published messages.
 */
type LogEntry struct {
	length     int64
	message    []byte
	seekOffset int64
}

func (m *LogEntry) Length() int64 {
	return m.length
}

type LogFile struct {
	file       *os.File
	msgBuffer  []LogEntry // Buffer of unsaved messages.
	bufferLock sync.RWMutex
}

func LogFromFile(index_path string) (lf *LogFile, err error) {
	lf = &LogFile{}
	lf.file, err = os.OpenFile(index_path, os.O_RDWR|os.O_CREATE, 0755)
	return
}

/**
 * Publish a new message into this topic.
 */
func (lf *LogFile) Publish(message []byte) error {
	msg := LogEntry{
		message: message,
	}
	lf.bufferLock.Lock()
	lf.msgBuffer = append(lf.msgBuffer, msg)
	lf.bufferLock.Unlock()
	// notify readers we have a new message
	return nil
}

/**
 * Checkpoints the latest state.
 */
func (lf *LogFile) Checkpoint() error {
	// First write the buffered records into the chunks
	for _, msg := range lf.msgBuffer {
		if int64(len(msg.message)) != msg.Length() {
			fmt.Printf("Entry size (%d) does not match Size of Message (%d)", len(msg.message), msg.Length())
			log.Fatal(nil)
		}
		if _, err := lf.file.Write(msg.message); err != nil {
			return err
		}
	}
	// saved so clear it
	lf.msgBuffer = nil
	return nil
}

func (lf *LogFile) Size() (int64, error) {
	stat, err := lf.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (lf *LogFile) Seek(offset int64, whence int) (int64, error) {
	return lf.file.Seek(0, io.SeekStart)
}

func (lf *LogFile) Read(b []byte) (int, error) {
	return lf.file.Read(b)
}

func (lf *LogFile) Write(b []byte) (int, error) {
	return lf.file.Write(b)
}
