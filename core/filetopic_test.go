package core

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	// "io/ioutil"
	// "strings"
	"log"
	"os"
	"testing"
	"time"
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
	log.Println("Started TestPublish10Messages")
	defer log.Println("Finished TestPublish10Messages")
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
		m, err := sub.NextMessage(true)
		if err != nil {
			log.Fatal(err)
		}
		n, err := m.Read(buffer, true)
		if err != nil {
			log.Fatal(err)
		}
		assert.Equal(t, n, 16, fmt.Sprintf("Messages are only 16 bytes in length, Found: %d", n))
		assert.Equal(t, msg, string(buffer[:n]), "Messages dont match")
		n, err = m.Read(buffer, true)
		assert.Equal(t, n, 0, "Should have no more messages until nextMessage")
		assert.Equal(t, err, EOM, "Should have no more messages until nextMessage")
	}
}

func TestPublish10MessagesAndReadDiffOffset(t *testing.T) {
	log.Println("Started TestPublish10MessagesAndReadDiffOffset")
	defer log.Println("Finished TestPublish10MessagesAndReadDiffOffset")
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
		m, err := sub.NextMessage(true)
		if err != nil {
			log.Fatal(err)
		}
		n, err := m.Read(buffer, true)
		if err != nil {
			log.Fatal(err)
		}
		assert.Equal(t, n, 16, fmt.Sprintf("Messages are only 16 bytes in length, Found: %d", n))
		assert.Equal(t, msg, string(buffer[:n]), "Messages dont match")
		n, err = m.Read(buffer, true)
		assert.Equal(t, n, 0, "Should have no more messages until nextMessage")
		assert.Equal(t, err, EOM, "Should have no more messages until nextMessage")
	}
}

// Test closing of a subcriber while it is waiting for messages to be published.
// This should close and close cleanly.
func TestClosingSubscriber(t *testing.T) {
	log.Println("Started TestClosingSubscriber")
	defer log.Println("Finished TestClosingSubscriber")
	topic := OpenTopic(t, "test", "/tmp/test4", "/tmp/index4", true)
	ourchan := make(chan error)
	sub, err := topic.Subscribe(2)
	go func() {
		if err != nil {
			log.Fatal(err)
		}
		buffer := make([]byte, 1024)
		m, err := sub.NextMessage(true)
		if err == nil {
			n, err := m.Read(buffer, true)
			log.Println("Read finished, n, err: ", n, err)
		}
		ourchan <- err
	}()

	time.Sleep(1 * time.Second)
	sub.Close()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	tickerCalled := false
	select {
	case err = <-ourchan:
		break
	case <-ticker.C:
		tickerCalled = true
		break
	}
	assert.Equal(t, fmt.Sprint(err), "read /tmp/index4: file already closed", "Err should be about file being closed")
	assert.Equal(t, tickerCalled, false, "Ticker should not have been received first")
}
