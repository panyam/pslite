package core

import (
	// 	"bytes"
	"encoding/binary"
	"fmt"
	// "github.com/panyam/klite/utils"
	"io"
	"log"
	// "os"
	"strings"
)

const (
	DEFAULT_CHUNK_SIZE        = 10000000
	DEFAULT_CHUNK_NAME_FORMAT = "chunk_%05d"
	SIZEOF_INDEX_ENTRY        = 8
)

type IndexEntry struct {
	/**
	 * Tells where in the record file this message begins.
	 */
	FileOffset uint32

	/**
	 * Length of the given record.
	 */
	RecordLength uint32

	/**
	 * Anything else we want in the index.
	 */
	// Data interface{}
}

func (ie *IndexEntry) Bytes() []byte {
	// var bb bytes.Buffer
	// enc := gob.NewEncoder(&bb)
	// enc.Encode(ie)
	// return bb.Bytes()
	out := make([]byte, 8)
	binary.LittleEndian.PutUint32(out, ie.FileOffset)
	binary.LittleEndian.PutUint32(out[4:], ie.RecordLength)
	return out
}

func (ie *IndexEntry) FromBytes(b []byte) error {
	ie.FileOffset = binary.LittleEndian.Uint32(b)
	ie.RecordLength = binary.LittleEndian.Uint32(b[4:])
	return nil
	// dec := gob.NewDecoder(bytes.NewReader(b))
	// return dec.Decode(ie)
}

type FileTopic struct {
	/**
	 * Name of the topic.
	 */
	Name        string
	RecordsPath string
	IndexPath   string

	/**
	 * Folder where topic will be persisted.
	 */
	numEntries int64
	recordFile *LogFile
	indexFile  *LogFile
}

func NewFileTopic(name string, records_path string, index_path string) (*FileTopic, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("Topic name cannot be empty")
	}
	// ensure topic's folder either does not exist or is empty
	topic := &FileTopic{
		Name:        name,
		RecordsPath: records_path,
		IndexPath:   index_path,
	}
	err := topic.ReloadIndex()
	if err == nil {
		err = topic.ReloadRecords()
	}
	return topic, err
}

func (topic *FileTopic) MsgCount() int64 {
	return topic.numEntries
}

/**
 * Subscribe to this topic at a given offset and return a channel from which
 * newer published messages can be sent to.
 */
func (topic *FileTopic) Subscribe(start_offset int64) (sub Subscriber, err error) {
	index_reader, err := topic.indexFile.IterFrom(start_offset * int64(SIZEOF_INDEX_ENTRY))
	if err != nil {
		return nil, err
	}
	out := &FileSubscriber{
		topic:        topic,
		index_reader: index_reader,
	}
	out.NextMessage(true)
	sub = out
	return sub, nil
}

/**
 * Publish a new message into this topic.
 */
func (topic *FileTopic) Publish(message []byte) (offset int64, err error) {
	offset, err = topic.recordFile.Publish(message)
	if err == nil {
		L := uint32(len(message))
		err = topic.PublishIndex(&IndexEntry{
			FileOffset:   uint32(offset) - L,
			RecordLength: L,
		})
	}
	// notify readers we have a new message
	return
}

/**
 * Add a new index entry.
 */
func (topic *FileTopic) PublishIndex(ientry *IndexEntry) (err error) {
	b := ientry.Bytes()
	if len(b) != SIZEOF_INDEX_ENTRY {
		log.Println("B: ", b)
		fmt.Printf("Entry size (%d) does not match indexSize (%d)", len(b), SIZEOF_INDEX_ENTRY)
		log.Fatal(nil)
	}
	if _, err = topic.indexFile.Publish(b); err == nil {
		topic.numEntries += 1
	}
	return
}

/**
 * Reloads the index entries from the index file.
 */
func (t *FileTopic) ReloadIndex() (err error) {
	if t.indexFile == nil {
		t.indexFile, err = NewLogFile(t.IndexPath)
		if err != nil {
			return
		}
	}
	ifSize, err := t.indexFile.Size()
	if err != nil {
		return err
	}
	if ifSize%int64(SIZEOF_INDEX_ENTRY) != 0 {
		err = fmt.Errorf("Index file size (%d) is not a multiple of (%d)", ifSize, SIZEOF_INDEX_ENTRY)
		return err
	}
	if err != nil {
		return err
	}
	t.numEntries = ifSize / int64(SIZEOF_INDEX_ENTRY)
	return err
}

func (topic *FileTopic) ReloadRecords() (err error) {
	topic.recordFile, err = NewLogFile(topic.RecordsPath)
	if err == nil {
		var ientry *IndexEntry
		ientry, err = topic.IndexAtOffset(int64(topic.numEntries - 1))
		endpos := int64(0)
		if ientry != nil && (err == nil || err == io.EOF) {
			endpos = int64(ientry.FileOffset + ientry.RecordLength)
		}
		err = topic.recordFile.Truncate(endpos)
	}
	return
}

/**
 * Read the index entry at the current file offset.
 */
func (topic *FileTopic) ReadIndexEntry(iter *LogIter) (ientry *IndexEntry, err error) {
	inbytes := make([]byte, SIZEOF_INDEX_ENTRY)
	var n int
	if n, err = iter.Read(inbytes, false); err == nil {
		if n != SIZEOF_INDEX_ENTRY {
			err = fmt.Errorf("Expected index size to be %d, Found: %d", SIZEOF_INDEX_ENTRY, n)
		}
		ientry = &IndexEntry{}
		err = ientry.FromBytes(inbytes)
	}
	return
}

/**
 * Gets the index entry for the record at a given offset.
 */
func (topic *FileTopic) IndexAtOffset(offset int64) (ientry *IndexEntry, err error) {
	var reader *LogIter
	if reader, err = topic.indexFile.IterFrom(offset * SIZEOF_INDEX_ENTRY); err == nil {
		ientry, err = topic.ReadIndexEntry(reader)
	}
	return
}

type FileSubscriber struct {
	topic         *FileTopic
	index_reader  *LogIter
	record_reader *LogIter
	curr_ientry   *IndexEntry
	curr_read     int64
}

/**
 * Closes the subscriber.
 */
func (fs *FileSubscriber) Close() {
	fs.curr_ientry = nil
	fs.index_reader.Close()
	fs.record_reader.Close()
}

func (fs *FileSubscriber) HasMore() bool {
	return fs.curr_ientry != nil
}

/**
 * Proceed to the next message.
 */
func (fs *FileSubscriber) NextMessage(wait bool) (err error) {
	fs.curr_read = 0
	fs.curr_ientry, err = fs.NextIndexEntry(wait)
	return
}

func (fs *FileSubscriber) Read(b []byte, wait bool) (n int, err error) {
	if fs.record_reader == nil {
		if fs.record_reader, err = fs.topic.recordFile.IterFrom(int64(fs.curr_ientry.FileOffset)); err != nil {
			return
		}
	}
	remaining := int64(fs.curr_ientry.RecordLength) - fs.curr_read
	if remaining <= 0 {
		return 0, EOM
	}
	if int64(len(b)) > remaining {
		b = b[:remaining]
	}
	n, err = fs.record_reader.Read(b, wait)
	if n > 0 {
		fs.curr_read += int64(n)
	}
	return
}

func (fs *FileSubscriber) NextIndexEntry(wait bool) (ientry *IndexEntry, err error) {
	inbytes := make([]byte, SIZEOF_INDEX_ENTRY)
	var n int
	n, err = fs.index_reader.Read(inbytes, wait)
	if err == nil {
		if n != SIZEOF_INDEX_ENTRY {
			err = fmt.Errorf("Expected index size to be %d, Found: %d", SIZEOF_INDEX_ENTRY, n)
		}
		ientry = &IndexEntry{}
		err = ientry.FromBytes(inbytes)
	}
	return
}
