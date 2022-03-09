package core

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
)

type IndexEntry struct {
	/**
	 * Length of the given record.
	 */
	RecordLength uint32

	/**
	 * Time stamp of this record.
	 */
	RecordTimestamp int64

	/**
	 * The offset within the chunk where this record lies.
	 */
	FileOffset uint32
}

func (ie *IndexEntry) Bytes() []byte {
	var bb bytes.Buffer
	enc := gob.NewEncoder(&bb)
	enc.Encode(ie)
	return bb.Bytes()
}

func (ie *IndexEntry) FromBytes(b []byte) error {
	dec := gob.NewDecoder(bytes.NewReader(b))
	return dec.Decode(ie)
}

// should be sizeof(IndexEntry)
const SIZEOF_INDEX_ENTRY = 20

type KLIndex struct {
	indexFile       *LogFile
	allEntries      []IndexEntry // All entries
	bufferLock      sync.RWMutex
	checkpointIndex int // index of last checkpointed entry
}

func IndexFromFile(index_path string) (ki *KLIndex, err error) {
	ki = &KLIndex{}
	ki.indexFile, err = LogFromFile(index_path)
	if err == nil {
		err = ki.ReloadIndex()
	}
	return
}

func (ki *KLIndex) Count() int {
	ki.bufferLock.RLock()
	defer ki.bufferLock.RUnlock()
	return len(ki.allEntries)
}

/**
 * Gets the index entry for the record at a given offset.
 */
func (ki *KLIndex) OffsetIndex(offset int) *IndexEntry {
	ki.bufferLock.RLock()
	defer ki.bufferLock.RUnlock()
	off := len(ki.allEntries) + offset
	if off < 0 || off >= len(ki.allEntries) {
		return nil
	}
	return &ki.allEntries[off]
}

func (ki *KLIndex) Add(recLength uint32, recTime int64, fileOffset uint32) *IndexEntry {
	entry := IndexEntry{
		RecordLength:    recLength,
		RecordTimestamp: recTime,
		FileOffset:      fileOffset,
	}
	ki.bufferLock.Lock()
	ki.allEntries = append(ki.allEntries, entry)
	ki.bufferLock.Unlock()
	return &entry
}

func (ki *KLIndex) ReloadIndex() error {
	ifSize, err := ki.indexFile.Size()
	if err != nil {
		return err
	}
	if ifSize%SIZEOF_INDEX_ENTRY != 0 {
		err = fmt.Errorf("Index file size (%d) is not a multiple of (%d)", ifSize, SIZEOF_INDEX_ENTRY)
		return err
	}
	numEntries := int(ifSize / SIZEOF_INDEX_ENTRY)
	ki.checkpointIndex = numEntries
	inbytes := make([]byte, SIZEOF_INDEX_ENTRY)
	sub, err := ki.indexFile.NewSubscriber(0)
	defer sub.Close()
	if err != nil {
		return err
	}
	for i := 0; i < numEntries; i++ {
		var ientry IndexEntry
		_, err = sub.Read(inbytes, true)
		if err == nil {
			err = ientry.FromBytes(inbytes)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

/**
 * Checkpoints the latest state.
 */
func (ki *KLIndex) Checkpoint() {
	// First write the buffered records into the chunks
	for ki.checkpointIndex < len(ki.allEntries) {
		ientry := ki.allEntries[ki.checkpointIndex]
		b := ientry.Bytes()
		if len(b) != SIZEOF_INDEX_ENTRY {
			fmt.Printf("Entry size (%d) does not match SOIE", len(b))
			log.Fatal(nil)
		}
		ki.indexFile.Publish(b)
		ki.checkpointIndex += 1
	}
	ki.indexFile.Sync()
}
