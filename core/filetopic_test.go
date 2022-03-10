package core

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	// "io/ioutil"
	// "strings"
	"log"
	"os"
	"testing"
	// "time"
)

func OpenTopic(t *testing.T, n string, rp string, ip string, reset bool) *FileTopic {
	if reset {
		if err := os.Remove(rp); err != nil {
			log.Println(err)
		}
		if err := os.Remove(ip); err != nil {
			log.Println(err)
		}
	}
	topic, err := NewFileTopic(n, rp, ip)
	if err != nil {
		log.Fatal(err)
	}
	if reset {
		assert.Equal(t, topic.MsgCount(), int64(0), "resetted topics should have no msgs")
	}
	return topic
}

func TestBasicOpen(t *testing.T) {
	OpenTopic(t, "test", "test", "index", true)
}

func TestPublish10Messages(t *testing.T) {
	topic := OpenTopic(t, "test", "test2", "index2", true)
	for i := 0; i < 10; i++ {
		topic.Publish([]byte(fmt.Sprintf("Hello World %03d\n", i)))
	}

	// Now read them back
	sub, err := topic.Subscribe(0)
	if err != nil {
		log.Fatal(err)
	}
	buffer := make([]byte, 1024)
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("Hello World %03d\n", i)
		n, err := sub.Read(buffer, true)
		if err != nil {
			log.Fatal(err)
		}
		assert.Equal(t, n, 16, fmt.Sprintf("Messages are only 16 bytes in length, Found: %d", n))
		assert.Equal(t, msg, string(buffer[:n]), "Messages dont match")
	}
}

func TestPublish10MessagesAndReadDiffOffset(t *testing.T) {
	topic := OpenTopic(t, "test", "test2", "index2", true)
	for i := 0; i < 10; i++ {
		topic.Publish([]byte(fmt.Sprintf("Hello World %03d\n", i)))
	}

	// Now read them back
	sub, err := topic.Subscribe(2)
	if err != nil {
		log.Fatal(err)
	}
	buffer := make([]byte, 1024)
	for i := 2; i < 10; i++ {
		msg := fmt.Sprintf("Hello World %03d\n", i)
		n, err := sub.Read(buffer, true)
		if err != nil {
			log.Fatal(err)
		}
		assert.Equal(t, n, 16, fmt.Sprintf("Messages are only 16 bytes in length, Found: %d", n))
		assert.Equal(t, msg, string(buffer[:n]), "Messages dont match")
	}
}
