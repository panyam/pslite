package main

import "fmt"

import (
	"flag"
	// "fmt"
	"github.com/panyam/klite/core"
	protos "github.com/panyam/klite/protos"
	svc "github.com/panyam/klite/services"
	"google.golang.org/grpc"
	"log"
	"net"
)

var (
	port        = flag.Int("port", 9111, "Port on which gRPC server should listen TCP conn.")
	topics_root = flag.String("topics_root", "~/.klite", "Root location where all topics are created")
)

func main() {
	flag.Parse()
	grpcServer := grpc.NewServer()
	engine, err := core.NewEngine(*topics_root)
	protos.RegisterStreamerServiceServer(grpcServer, svc.NewStreamerService(engine))
	log.Printf("Initializing gRPC server on port %d", *port)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer.Serve(lis)
}
