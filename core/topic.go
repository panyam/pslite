package core

import "io"

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
	CheckpointThreshold int32

	/**
	 * How often will checkpointing be done (in seconds) if # messages threshold
	 * not met.
	 */
	CheckpointTimeout int64

	/**
	 * Messages are appended to chunk files.  The name of this chunk file is
	 * appended with the chunk number.
	 */
	ChunkNameFormat string

	/**
	 * Max number of bytes per topic chunk file.  New chunk files will be created
	 * when this many bytes have been written.
	 */
	ChunkSize int64

	/**
	 * Controls whether to include a timestamp in the message.
	 */
	IncludeTimestamp bool

	/**
	 * Controls whether an index file is to be created.
	 */
	CreateIndexes bool
}

func NewTopic(topic *KLTopic) error {
	// ensure topic's folder either does not exist or is empty
	return nil
}

/**
 * Opens a topic from a given folder.
 */
func OpenTopic(topic_folder string) (*KLTopic, error) {
	return nil, nil
}

/**
 * Subscribe to this topic at a given offset and return a channel from which
 * newer published messages can be sent to.
 */
func (topic *KLTopic) Subscribe(offset int64) (chan *io.Reader, error) {
	return nil, nil
}

/**
 * Publish a new message into this topic.
 */
func (topic *KLTopic) Publish(message []byte, offset int64, length int64) error {
	return nil
}

/**
 * Seeks a particular offset or timestamp in the topic and returns a MessageReader
 * that can start reading messages at this offset.
 */
func (topic *KLTopic) SeekOffset(offset int64, as_offset bool) (*MessageReader, error) {
	return nil, nil
}
