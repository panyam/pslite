package core

import (
	"io"
	"os"
	"sync"
)

/**
 * A Message implementation for representing published messages.
 */
type LogEntry struct {
	message    []byte
	seekOffset int64
}

type OffsetListener struct {
	minOffset     int64
	waiterChannel chan int64
}

type LogFile struct {
	file_path       string
	file            *os.File
	offset          int64
	subIDCount      int64 // Tracks readers
	subs            map[int64]*Subscriber
	olistMutex      sync.RWMutex
	offsetListeners []OffsetListener
	msgBuffer       []LogEntry // Buffer of unsaved messages.
	bufferLock      sync.RWMutex
}

func LogFromFile(index_path string) (lf *LogFile, err error) {
	lf = &LogFile{
		file_path:  index_path,
		subs:       make(map[int64]*Subscriber),
		subIDCount: 0,
	}
	lf.file, err = os.OpenFile(index_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
	lf.offset, err = lf.Size()
	return
}

/**
 * Creates a new subscriber at a given offset.
 */
func (lf *LogFile) NewSubscriber(offset int64) (sub *Subscriber, err error) {
	lf.subIDCount += 1
	sub = &Subscriber{
		logfile:    lf,
		id:         lf.subIDCount,
		offset:     0,
		waiting:    false,
		waitOffset: -1,
		dataWaiter: make(chan int64, 1),
	}
	sub.file, err = os.Open(lf.file_path)
	if err != nil {
		close(sub.dataWaiter)
	}
	if offset > 0 {
		sub.Seek(offset, 0)
	}
	lf.subs[sub.id] = sub
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
		if _, err := lf.file.Write(msg.message); err != nil {
			return err
		}
	}
	// saved so clear it
	lf.msgBuffer = nil
	return nil
}

/**
 * Appends data to the end of the log file.
 * Also notifies any subscribers waiting for data at the end of the file.
 */
func (lf *LogFile) Write(b []byte) (n int, err error) {
	n, err = lf.file.Write(b)
	if n > 0 {
		lf.offset += int64(n)
	}
	if err == nil {
		lf.NotifyOffsets()
	}
	return
}

/**
 * Used by readers that have reached EOF to "wait" for data to be available.
 */
func (lf *LogFile) WaitForOffset(minOffset int64, waiterChannel chan int64) {
	if lf.offset >= minOffset {
		waiterChannel <- lf.offset
	} else {
		// add to our list of waiters (these are already at EOF)
		lf.olistMutex.Lock()
		waiter := OffsetListener{
			minOffset,
			waiterChannel,
		}
		lf.offsetListeners = append(lf.offsetListeners, waiter)
		lf.olistMutex.Unlock()
	}
}

/**
 * Notify all listenrs waiting to know when their min offset has been breached.
 */
func (lf *LogFile) NotifyOffsets() {
	var newList []OffsetListener
	lf.olistMutex.Lock()
	for _, olist := range lf.offsetListeners {
		if lf.offset >= olist.minOffset {
			// have data
			olist.waiterChannel <- lf.offset
		} else {
			newList = append(newList, olist)
		}
	}
	// set offset listener list to those whose thresholds have not met
	lf.offsetListeners = newList
	lf.olistMutex.Unlock()
}

/**
 * Truncates the size of the file and updates offset accordingly.
 */
func (lf *LogFile) Truncate(newsize int64) (err error) {
	if err = lf.file.Truncate(newsize); err == nil {
		newsize, err = lf.file.Seek(0, io.SeekEnd)
		if err == nil {
			lf.offset = newsize
		}
	}
	return
}

func (lf *LogFile) Size() (int64, error) {
	stat, err := lf.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

/**
 * Reader objects consume contents from the log file and will block
 * until data is available (ie written by the publisher)
 */
type Subscriber struct {
	id         int64 // ID of the subscriber
	logfile    *LogFile
	file       *os.File   // File pointer in read mode
	offset     int64      // Subscriber's file offset
	waiting    bool       // Tells if the subscriber is waiting for data is available
	waitOffset int64      // Tells what is the min offset until which subscriber will wait
	dataWaiter chan int64 // The channel on which to notify the subscriber to stop waiting
}

func (s *Subscriber) Seek(offset int64, whence int) (int64, error) {
	newoff, err := s.file.Seek(0, io.SeekStart)
	if err == nil {
		s.offset = newoff
	}
	return newoff, err
}

func (sub *Subscriber) Read(b []byte) (n int, err error) {
	total := 0
	for total < len(b) {
		n, err = sub.file.Read(b[total:])
		if err == nil {
			total += n
		}
		if total < len(b) {
			newoff := sub.offset + int64(total)
			sub.logfile.WaitForOffset(newoff, sub.dataWaiter)
			<-sub.dataWaiter
		}
	}
	return
}

func (sub *Subscriber) Close() {
	close(sub.dataWaiter)
	sub.file.Close()
}
