package main

import "fmt"

import (
	"flag"
	"strconv"
	// "fmt"
	"github.com/panyam/klite/core"
	protos "github.com/panyam/klite/protos"
	svc "github.com/panyam/klite/services"
	utils "github.com/panyam/klite/utils"
	"google.golang.org/grpc"
	"log"
	"net"
)

var (
	port        = flag.Int("port", 9111, "Port on which gRPC server should listen TCP conn.")
	serverAddr  = flag.String("server", "localhost:9111", "Address of the grpc server to test as client.")
	topicName   = flag.String("topic", "testtopic", "Name of topic where all pubsub actions are performed.")
	topics_root = flag.String("topics_root", "~/.klite", "Root location where all topics are created")
)

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		grpcServer := grpc.NewServer()
		engine, err := core.NewEngine(*topics_root)
		protos.RegisterKLiteServiceServer(grpcServer, svc.NewKLiteService(engine))
		log.Printf("Initializing gRPC server on port %d", *port)
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		grpcServer.Serve(lis)
	} else {
		pubsub, err := utils.NewPubSub(*serverAddr)
		defer pubsub.Close()
		if err != nil {
			log.Fatal(err)
		}
		if args[0] == "new" {
			err = pubsub.EnsureTopic(*topicName, args[1])
		}
		if args[0] == "pub" {
			err = pubsub.Publish(*topicName, args[1])
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
}
