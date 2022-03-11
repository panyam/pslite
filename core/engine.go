package core

import (
	"errors"
	"fmt"
)

var EOM = errors.New("EndOfMessage")

/**
 * Generic topic interface.
 */
type Topic interface {
	MsgCount() int64
	Publish(message []byte) (int64, error)
	Subscribe(msg_offset int64) (Subscriber, error)
}

/**
 * A subscriber for a particular topic.
 * An interface over a message iterator.
 * The way a subscriber is to be used is when a Read is called
 * on a topic the read is to be performed on a range.
 *
 * The client would do something like:
 *
 *		sub := topic.Subscribe(5)	// start subscribing from 5th message
 * 		bytes := make([]byte, SOME_BUFF_SIZE)
 *		while sub.HasMore() {
 *		  L := nextMsg.Length()
 *			len, err := sub.Read(buffer, true)
 *			assert(len == L)
 *			// Note: cannot read more than message size regardless of bytes buff size
 *
 *			// forward to the message if we need to If do *not* forward then
 *			// Any subsequent calls to Read() above must return an EOM error
 *			sub.NextMessage()
 *		}
 *		It is upto the Reader above to ensure no more than L bytes are returned
 *	  (eg even if bytes is a byt array with more capacity than L)
 */
type Subscriber interface {
	HasMore() bool
	NextMessage(wait bool) (msg Message, err error)
	Close()
}

type Message interface {
	Length() int64
	Read(b []byte, wait bool) (n int, err error)
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
func (eng *KLEngine) AddTopic(name string, topic Topic) error {
	curr, ok := eng.Topics[name]
	if ok || curr != nil {
		return fmt.Errorf("Topic '%s' already exists", name)
	}
	eng.Topics[name] = topic
	return nil
}

/**
 * Gets a named topic.
 */
func (eng *KLEngine) GetTopic(name string) Topic {
	if curr, ok := eng.Topics[name]; ok {
		return curr
	}
	// open it otherwise
	return eng.Topics[name]
}
