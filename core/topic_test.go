package core

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"
	// "time"
)

func DefaultTopic(name string, topic_root string) *KLTopic {
	if len(strings.TrimSpace(topic_root)) == 0 {
		dir, err := ioutil.TempDir("/tmp", "kltopic")
		if err != nil {
			log.Fatal(err)
		}
		topic_root = dir
	}
	fmt.Println("Test Topic Root: ", topic_root)

	return &KLTopic{
		Name:             name,
		TopicFolder:      topic_root,
		IncludeTimestamp: true,
		CreateIndexes:    true,
	}
}

func OpenTestTopic(t *testing.T, topic *KLTopic) *KLTopic {
	err := NewTopic(topic)
	if err != nil {
		log.Fatal(err)
	}
	if topic == nil {
		err = fmt.Errorf("Could not create topic")
		log.Fatal(err)
	}
	topic2, err := OpenTopic(topic.TopicFolder)
	if err != nil {
		log.Fatal(err)
	}
	assert.Equal(t, topic, topic2, "Created and opened topic are not equal")
	return topic2
}

func TestBasicOpen(t *testing.T) {
	topic := DefaultTopic("test", "")
	defer os.RemoveAll(topic.TopicFolder)
	OpenTestTopic(t, topic)
}

func TestPublish10Messages(t *testing.T) {
	topic := DefaultTopic("test", "")
	defer os.RemoveAll(topic.TopicFolder)
	OpenTestTopic(t, topic)
}
