package core

import (
	"io"
	"log"
	"os"
	"sync"
)

type OffsetListener struct {
	minOffset     int64
	waiterChannel chan int64
}

type OnOffsetReachedType = func(offset int64)

type LogFile struct {
	file_path       string
	file            *os.File
	offset          int64
	olistMutex      sync.RWMutex
	offsetListeners []OffsetListener
	bufferLock      sync.RWMutex
	OnOffsetReached OnOffsetReachedType
}

func NewLogFile(index_path string) (lf *LogFile, err error) {
	lf = &LogFile{
		file_path: index_path,
	}
	lf.file, err = os.OpenFile(index_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
	lf.offset, err = lf.Size()
	return
}

/**
 * Creates a new iterator at a given offset.
 */
func (lf *LogFile) IterFrom(offset int64) (sub *LogIter, err error) {
	sub = &LogIter{
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
		_, err = sub.SeekOffset(offset)
	}
	return
}

func (lf *LogFile) Offset() int64 {
	return lf.offset
}

/**
 * Publish a new message into this topic.
 */
func (lf *LogFile) Publish(message []byte) (offset int64, err error) {
	if len(message) > 0 {
		if _, err = lf.file.Write(message); err == nil {
			lf.file.Sync()
			lf.offset += int64(len(message))
			offset = lf.offset
			lf.NotifyOffsets(lf.offset)
		}
	}
	return
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
	if lf.OnOffsetReached != nil {
		lf.OnOffsetReached(offset)
	}
	lf.olistMutex.Lock()
	for _, olist := range lf.offsetListeners {
		if offset >= olist.minOffset {
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
type LogIter struct {
	id         int64 // ID of the iterator
	logfile    *LogFile
	file       *os.File   // File pointer in read mode
	offset     int64      // LogIter's file offset
	waiting    bool       // Tells if the iterator is waiting for data is available
	waitOffset int64      // Tells what is the min offset until which iterator will wait
	dataWaiter chan int64 // The channel on which to notify the iterator to stop waiting
	closed     bool       // Tells if channel is closed or not
}

func (s *LogIter) SeekOffset(offset int64) (int64, error) {
	newoff, err := s.file.Seek(offset, io.SeekStart)
	if err == nil {
		s.offset = newoff
	}
	return newoff, err
}

func (sub *LogIter) Read(b []byte, wait bool) (n int, err error) {
	total := 0
	currOff := sub.offset
	endOff := currOff + int64(len(b))
	for total < len(b) {
		n, err = sub.file.Read(b[total:])
		if err == nil && n > 0 {
			total += n
			sub.offset += int64(n)
		}
		if !wait || (err != nil && err != io.EOF) {
			// log.Println("LogFile Read Error: ", err)
			return total, err
		}
		if total < len(b) {
			// log.Printf("Waiting at offset %d, to hit newoffset (%d), Len: %d", sub.offset, endOff, len(b))
			sub.logfile.WaitForOffset(endOff, sub.dataWaiter)
			// log.Println("Len of num waiters: ", len(sub.logfile.offsetListeners))
			<-sub.dataWaiter
			// log.Println("-------- Returned from wait: ", endOff, off)
		}
	}
	return total, err
}

func (sub *LogIter) Close() {
	if !sub.closed {
		sub.closed = true
		close(sub.dataWaiter)
		sub.file.Close()
	}
}
