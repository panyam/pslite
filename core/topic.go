package core

import (
	"encoding/json"
	"fmt"
	"github.com/panyam/klite/utils"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

const (
	DEFAULT_CHUNK_SIZE         = 10000000
	DEFAULT_CHECKPOINT_TIMEOUT = 5 * time.Second
	DEFAULT_CHUNK_NAME_FORMAT  = "chunk_%05d"
)

type KLTopic struct {
	/**
	 * Name of the topic.
	 */
	Name string

	/**
	 * Folder where topic will be persisted.
	 */
	TopicFolder string

	/**
	 * How often will checkpointing be performed (in number of messages)?
	 */
	CheckpointThreshold int

	/**
	 * How often will checkpointing be done (in seconds) if # messages threshold
	 * not met.
	 */
	CheckpointTimeout time.Duration

	/**
	 * Controls whether to include a timestamp in the message.
	 */
	IncludeTimestamp bool

	/**
	 * Controls whether an index file is to be created.
	 */
	CreateIndexes bool

	totalSize        uint32 // Total size of the topic (in bytes).
	recordFile       *LogFile
	recordIndex      *KLIndex
	lastCheckpointAt int64
	msgBuffer        []PubMsg // Buffer of unsaved messages.
	msgBufferLock    sync.RWMutex
}

func NewTopic(topic *KLTopic) error {
	// ensure topic's folder either does not exist or is empty
	_, err := os.Stat(topic.TopicFolder)
	if err == nil {
		// see if dir is empty
		if _, err := utils.IsDirEmpty(topic.TopicFolder); err != nil {
			return fmt.Errorf("Cannot create topic (%s).  Folder already exists and is not empty.", topic.TopicFolder)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	// Sanitize things
	if strings.TrimSpace(topic.Name) == "" {
		return fmt.Errorf("Topic name cannot be empty")
	}

	if topic.CheckpointThreshold <= 0 {
		// Will checkpoint after every publish!
	}

	if topic.CheckpointTimeout <= 0 {
		topic.CheckpointTimeout = DEFAULT_CHECKPOINT_TIMEOUT
	}

	// Create topic folder if it doesnt exist
	os.MkdirAll(topic.TopicFolder, 0777)

	// Write the topic into json
	marshalled, err := json.Marshal(topic)
	d1 := []byte(marshalled)
	info_path := topic.InfoPath()
	if err := os.WriteFile(info_path, d1, 0777); err != nil {
		log.Fatalf("Could not marshall Topic (%s) to JSON", topic.Name)
		return err
	}
	return nil
}

func (topic *KLTopic) MsgCount() int64 {
	return int64(topic.recordIndex.Count())
}

func (topic *KLTopic) InfoPath() string {
	return path.Join(topic.TopicFolder, "Info.json")
}

/**
 * Opens a topic from a given folder.
 */
func OpenTopic(topic_folder string) (topic *KLTopic, err error) {
	topic = nil
	_, err = os.Stat(topic_folder)
	if err != nil {
		return
	}
	info_path := path.Join(topic_folder, "Info.json")
	info_file, err := os.Open(info_path)
	if err != nil {
		return
	}
	jsonParser := json.NewDecoder(info_file)
	topic = &KLTopic{}
	err = jsonParser.Decode(topic)
	if err != nil {
		return
	}

	// Ensure index and record files exist
	topic.recordIndex, err = IndexFromFile(path.Join(topic.TopicFolder, "Indexes"))
	if err != nil {
		return
	}

	records_path := path.Join(topic.TopicFolder, "Records")
	topic.recordFile, err = LogFromFile(records_path)
	if err == nil {
		ientry := topic.recordIndex.OffsetIndex(-1)
		endpos := int64(0)
		if ientry != nil {
			endpos = int64(ientry.FileOffset + ientry.RecordLength)
		}
		err = topic.recordFile.Truncate(endpos)
	}
	return
}

// func (topic *KLTopic) Name() string { return topic.Name }

/**
 * Subscribe to this topic at a given offset and return a channel from which
 * newer published messages can be sent to.
 */
func (topic *KLTopic) Subscribe(start_offset int64,
	end_offset int64, by_index bool) (chan *io.ReaderAt, error) {
	if !by_index {
		// offsets are timestamps so get real offsets for these timestamps
		start_offset, end_offset = topic.OffsetsForTimestampRange(start_offset, end_offset)
	}
	readerChannel := make(chan *io.ReaderAt)
	go func() {
		// iterator := topic.OpenIterator(start_offset, end_offset)
		curr_offset := start_offset
		for false && (curr_offset <= end_offset || end_offset < start_offset) {
			// here kepe sending stuff into the readerChannel
			/*
				nextMessage := iterator.Read()
			*/
		}
	}()
	return readerChannel, nil
}

/**
 * Publish a new message into this topic.
 */
func (topic *KLTopic) Publish(message []byte) error {
	now := time.Now().UTC().UnixNano()
	topic.msgBufferLock.Lock()
	topic.recordFile.Publish(message)
	size, err := topic.recordFile.Size()
	if err != nil {
		return err
	}
	topic.recordIndex.Add(uint32(len(message)), now, uint32(size))
	if topic.CheckpointNeeded() {
		topic.Checkpoint()
	}
	topic.msgBufferLock.Unlock()
	// notify readers we have a new message
	return nil
}

/**
 * Tells if a checkpoint is needed.
 */
func (topic *KLTopic) CheckpointNeeded() bool {
	now := time.Now().UTC().UnixNano()
	if topic.CheckpointThreshold > 0 {
		if len(topic.msgBuffer) >= topic.CheckpointThreshold {
			return true
		}
	}
	dur := time.Duration(now - topic.lastCheckpointAt)
	if dur >= topic.CheckpointTimeout {
		return false
	}
	return false
}

/**
 * Checkpoints the latest state.
 */
func (topic *KLTopic) Checkpoint() {
	// First write the buffered records into the chunks
	topic.recordIndex.Checkpoint()
	topic.lastCheckpointAt = time.Now().UTC().UnixNano()
}

/**
 * Seeks a particular offset or timestamp in the topic and returns a MessageReader
 * that can start reading messages at this offset.
 */
func (topic *KLTopic) SeekOffset(offset int64, as_offset bool) (*MessageReader, error) {
	return nil, nil
}

/**
 * Converts a timestamp range into start and end offsets.
 */
func (topic *KLTopic) OffsetsForTimestampRange(start_timestamp int64, end_timestamp int64) (start_offset int64, end_offset int64) {
	return
}
