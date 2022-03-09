package core

import (
	"io"
	"log"
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
	olistMutex      sync.RWMutex
	offsetListeners []OffsetListener
	msgBuffer       []LogEntry // Buffer of unsaved messages.
	bufferLock      sync.RWMutex
}

func LogFromFile(index_path string) (lf *LogFile, err error) {
	lf = &LogFile{
		file_path: index_path,
	}
	lf.file, err = os.OpenFile(index_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
	lf.offset, err = lf.Size()
	return
}

/**
 * Creates a new subscriber at a given offset.
 */
func (lf *LogFile) NewSubscriber(offset int64) (sub *Subscriber, err error) {
	sub = &Subscriber{
		logfile:    lf,
		offset:     0,
		waiting:    false,
		waitOffset: -1,
		dataWaiter: make(chan int64, 1),
	}
	sub.file, err = os.Open(lf.file_path)
	if err != nil {
		close(sub.dataWaiter)
	}
	if offset >= 0 {
		_, err = sub.Seek(offset, io.SeekStart)
	}
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
func (lf *LogFile) Sync() error {
	// First write the buffered records into the chunks
	lf.bufferLock.Lock()
	numWritten := 0
	defer func() {
		if numWritten >= len(lf.msgBuffer) {
			lf.msgBuffer = nil
		} else {
			lf.msgBuffer = lf.msgBuffer[numWritten:]
		}
		lf.file.Sync()
		lf.bufferLock.Unlock()
		if numWritten > 0 {
			lf.NotifyOffsets(lf.offset)
		}
	}()
	log.Println("Flushing buffered messages...")
	for _, msg := range lf.msgBuffer {
		if _, err := lf.file.Write(msg.message); err != nil {
			return err
		}
		lf.offset += int64(len(msg.message))
		numWritten += 1
	}
	// saved so clear it
	return nil
}

/**
 * Used by readers that have reached EOF to "wait" for data to be available.
 */
func (lf *LogFile) WaitForOffset(minOffset int64, waiterChannel chan int64) {
	if lf.offset > minOffset {
		log.Println("Here?", lf.offset, minOffset)
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
func (lf *LogFile) NotifyOffsets(offset int64) {
	var newList []OffsetListener
	log.Println("Notifying offsets: ", offset)
	lf.olistMutex.Lock()
	for _, olist := range lf.offsetListeners {
		if offset > olist.minOffset {
			// have data
			olist.waiterChannel <- offset
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
	newoff, err := s.file.Seek(offset, io.SeekStart)
	if err == nil {
		s.offset = newoff
	}
	return newoff, err
}

func (sub *Subscriber) Read(b []byte, wait bool) (n int, err error) {
	total := 0
	currOff := sub.offset
	endOff := currOff + int64(len(b))
	for total < len(b) {
		n, err = sub.file.Read(b[total:])
		if err == nil {
			total += n
		}
		if !wait || (err != nil && err != io.EOF) {
			log.Println("Error: ", err)
			return total, err
		}
		if total < len(b) {
			log.Printf("Waiting at offset %d, to hit newoffset (%d), Len: %d", sub.offset, endOff, len(b))
			sub.logfile.WaitForOffset(endOff, sub.dataWaiter)
			// log.Println("Len of num waiters: ", len(sub.logfile.offsetListeners))
			<-sub.dataWaiter
			// log.Println("Returned from wait: ", endOff, off)
		}
	}
	log.Println("Here: ", total, err)
	return total, err
}

func (sub *Subscriber) Close() {
	close(sub.dataWaiter)
	sub.file.Close()
}
