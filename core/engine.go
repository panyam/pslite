package core

import (
	"fmt"
	"io"
)

/**
 * Message interface for iterable and streamable messages.
 */
type Message interface {
	Reader() io.ReaderAt
	Offset() int64
	Length() int64
	Timestamp() int64
}

/**
 * An interface over a message iterator.
 * The way a message reader is to be used is when a Read is called
 * on a topic the read is to be performed on a range.
 *
 * The client would do something like:
 *
 *		mr := topic.Read(start, end)
 *		buffer := create_buffer(TEN_MB)
 *		for nextMsg, err := mr.NextMessage() ; err == nil && nextMsg != nil {
 *			offset = 0
 *		  for {
 *				len, err := mr.Reader().ReadAt(buffer, offset)
 *		  	if len > 0 { offset += len }
 *				if len == 0 || err != nil {
 *					// All done so we can stop
 *					break
 *				}
 *			}
 *			// No need to do a "Forward" here since the Reader above
 *			// is an implementation that will kick off the next message to
 *			// be pointed to when it has exhausted the message's bytes
 *			// Forward is typically called if as part of the streaming
 *		}
 */
type MessageReader interface {
	/**
	 * Returns true if more messages exist.
	 */
	HasMore() bool
	NextMessage() Message
}

type Topic interface {
	Name() string
	Publish(message []byte, offset int64, length int64) error
	Subscribe(offset int64) chan *io.ReaderAt
	SeekOffset(offset int64, as_offset bool) (*MessageReader, error)
}

/**
 * The KLite engine manages multiple topics and all their associated publishers
 * and subscribers.
 */
type KLEngine struct {
	Basedir string
	Topics  map[string]Topic
}

/**
 * Create a new engine to hold a bunch of topics in place.
 */
func NewEngine(basedir string) (*KLEngine, error) {
	engine := KLEngine{
		Basedir: basedir,
		Topics:  make(map[string]Topic),
	}
	return &engine, nil
}

/**
 * Adds a new topic to the engine.
 */
func (eng *KLEngine) AddTopic(topic Topic) error {
	curr, ok := eng.Topics[topic.Name()]
	if ok || curr != nil {
		return fmt.Errorf("Topic '%s' already exists", topic.Name())
	}
	eng.Topics[topic.Name()] = topic
	return nil
}

/**
 * Gets a named topic.
 */
func (eng *KLEngine) GetTopic(name string) Topic {
	return eng.Topics[name]
}
