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
	OpenTopic(t, "test", "/tmp/test", "/tmp/index", true)
}

func TestPublish10Messages(t *testing.T) {
	topic := OpenTopic(t, "test", "/tmp/test2", "/tmp/index2", true)
	for i := 0; i < 10; i++ {
		topic.Publish([]byte(fmt.Sprintf("Hello World %03d\n", i)))
	}

	// Now read them back
	sub, err := topic.Subscribe(0)
	defer sub.Close()
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
		n, err = sub.Read(buffer, true)
		assert.Equal(t, n, 0, "Should have no more messages until nextMessage")
		assert.Equal(t, err, EOM, "Should have no more messages until nextMessage")
		sub.NextMessage(i < 9)
	}
}

func TestPublish10MessagesAndReadDiffOffset(t *testing.T) {
	topic := OpenTopic(t, "test", "/tmp/test3", "/tmp/index3", true)
	for i := 0; i < 10; i++ {
		topic.Publish([]byte(fmt.Sprintf("Hello World %03d\n", i)))
	}

	// Now read them back
	sub, err := topic.Subscribe(2)
	defer sub.Close()
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
		n, err = sub.Read(buffer, true)
		assert.Equal(t, n, 0, "Should have no more messages until nextMessage")
		assert.Equal(t, err, EOM, "Should have no more messages until nextMessage")
		sub.NextMessage(i < 9)
	}
}
