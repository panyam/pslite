package core

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"sync"
	"testing"
	// "time"
	// "io/ioutil"
	// "strings"
	// "time"
)

func OpenLogFile(t *testing.T, fname string, clean bool) *LogFile {
	logfile, err := NewLogFile(fname)
	if err != nil {
		log.Fatal(err)
	}
	if logfile == nil {
		err = fmt.Errorf("Could not create logfile")
		log.Fatal(err)
	}
	if clean {
		logfile.Truncate(0)
		size, err := logfile.Size()
		if err != nil {
			log.Fatal(err)
		}
		assert.Equal(t, size, int64(0), "Logfile size should be 0")
	}
	return logfile
}

func TestBasicNew(t *testing.T) {
	OpenLogFile(t, "/tmp/test", true)
}

func Publish(lf *LogFile, contents string) {
	if _, err := lf.Publish([]byte(contents)); err != nil {
		log.Fatal("Error Publishing: ", err)
	}
}

func NewSub(lf *LogFile, offset int64) *LogIter {
	sub, err := lf.IterFrom(offset)
	if err != nil {
		log.Fatal("Error Subscribing: ", err)
	}
	return sub
}

func ReadSub(sub *LogIter, count int64, wait bool) string {
	b := make([]byte, count)
	n, err := sub.Read(b, wait)
	if err != nil && err != io.EOF {
		log.Fatal("Error Reading Sub: ", err)
	}
	return string(b[:n])
}

func TestBasicRW(t *testing.T) {
	lf := OpenLogFile(t, "/tmp/test", true)
	Publish(lf, "Hello World 1")
	Publish(lf, "Hello World 2")
	Publish(lf, "Hello World 3")

	r1 := NewSub(lf, 0)
	r2 := NewSub(lf, 13)
	s1 := ReadSub(r1, 13, false)
	s2 := ReadSub(r2, 13, false)
	s22 := ReadSub(r2, 13, false)
	s23 := ReadSub(r2, 13, false)
	assert.Equal(t, s1, "Hello World 1")
	assert.Equal(t, s2, "Hello World 2")
	assert.Equal(t, s22, "Hello World 3")
	assert.Equal(t, s23, "")
}

func TestMultiReaders(t *testing.T) {
	lf := OpenLogFile(t, "/tmp/test", true)

	// A way to check that an offset is monotonically increasing
	lastOffset := int64(0)
	lf.OnOffsetReached = func(offset int64) {
		if offset <= lastOffset {
			log.Fatalf("Offset regressed: %d, lastOffset: %d", offset, lastOffset)
		}
		lastOffset = offset
		// log.Println("Notifying offsets: ", offset)
	}

	reader := func(i int, rlen int64, total int64, res chan string, wg *sync.WaitGroup) {
		defer wg.Done()
		sub := NewSub(lf, 0)
		out := ""
		nread := int64(0)
		for nread < total {
			remaining := rlen
			if remaining > (total - nread) {
				remaining = total - nread
			}
			out += ReadSub(sub, remaining, true)
			nread += remaining
		}
		log.Println("Finished reader: ", i)
		res <- out
	}

	var cs []chan string
	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		c := make(chan string, 1)
		cs = append(cs, c)
		wg.Add(1)
		go reader(i-1, int64(i*15), 1500, c, &wg)
	}
	// Wait for all go routines to finish
	x := ""
	log.Println("Started writer: ")
	for i := 0; len(x) < 1500; i += 1 {
		s := fmt.Sprintf("Hello World %3d", i)
		Publish(lf, s)
		// time.Sleep(1 * time.Millisecond)
		x += s
	}
	log.Println("Writer Finished: ")
	wg.Wait()
	for _, c := range cs {
		assert.Equal(t, <-c, x, "Read and written dont match")
	}
}
