package main

import (
	"flag"
	"github.com/panyam/pslite/cli"
	"github.com/panyam/pslite/utils"
	"log"
	"strconv"
)

var (
	serverAddr = flag.String("server", utils.DefaultServerAddress(), "Address of the grpc server to test as client.")
	topicName  = flag.String("topic", "testtopic", "Name of topic where all pubsub actions are performed.")
)

func main() {
	flag.Parse()
	args := flag.Args()
	pubsub, err := cli.NewPubSub(*serverAddr)
	defer pubsub.Close()
	if err != nil {
		log.Fatal(err)
	}
	if args[0] == "new" {
		err = pubsub.EnsureTopic(*topicName, args[1])
	}
	if args[0] == "pub" {
		err = pubsub.Publish(*topicName, []byte(args[1]))
	} else if args[0] == "sub" {
		offset := int64(0)
		end_offset := int64(-1)
		if len(args) > 1 {
			if offset, err = strconv.ParseInt(args[1], 10, 64); err == nil {
				if len(args) > 2 {
					end_offset, err = strconv.ParseInt(args[2], 10, 64)
				}
			}
		}
		rchan, errchan, err := pubsub.Subscribe(*topicName, offset, end_offset)
		for err == nil {
			select {
			case msg := <-rchan:
				if msg == nil {
					log.Println("No more messages")
					return
				} else {
					log.Println("Msg: ", string(msg.Content))
				}
				break
			case err = <-errchan:
				log.Println("Error in subscription: ", err)
				return
			}
		}
	}
	if err != nil {
		log.Fatal("PubSub Error: ", err)
	}
}
