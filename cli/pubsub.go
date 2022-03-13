package cli

import (
	"context"
	"fmt"
	"github.com/panyam/pslite/core"
	protos "github.com/panyam/pslite/protos"
	svc "github.com/panyam/pslite/services"
	"github.com/panyam/pslite/utils"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"os"
	"path"
)

type PubSub struct {
	serverAddr string
	conn       *grpc.ClientConn
	client     protos.PSLiteServiceClient
}

func PSLServe(port int, topics_root string) {
	grpcServer := grpc.NewServer()
	engine, err := core.NewEngine(topics_root)
	protos.RegisterPSLiteServiceServer(grpcServer, svc.NewPSLiteService(engine))
	log.Printf("Initializing PSLite gRPC server on port %d", port)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer.Serve(lis)
}

func NewPubSub(serverAddr string) (out *PubSub, err error) {
	out = &PubSub{
		serverAddr: serverAddr,
	}
	out.conn, err = grpc.Dial(serverAddr, grpc.WithInsecure())
	if err == nil {
		out.client = protos.NewPSLiteServiceClient(out.conn)
	}
	return
}

func (ps *PubSub) EnsureTopic(topic string, folder string) error {
	folder = utils.ExpandUserPath(folder)
	log.Println("Expanded folder: ", folder)
	if err := os.MkdirAll(folder, 0777); err != nil {
		return err
	}
	req := &protos.OpenTopicRequest{
		Topic: &protos.Topic{
			Name:        topic,
			RecordsPath: path.Join(folder, "RECORDS"),
			IndexPath:   path.Join(folder, "INDEX"),
		},
	}
	_, err := ps.client.OpenTopic(context.Background(), req)
	return err
}

func (ps *PubSub) Publish(topic string, data []byte) error {
	pubreq := &protos.PublishRequest{
		TopicName: topic,
		Content: &protos.PublishRequest_ContentBytes{
			ContentBytes: data,
		},
	}
	_, err := ps.client.Publish(context.Background(), pubreq)
	return err
}

func (ps *PubSub) Subscribe(topic string, offset int64, end_offset int64) (rchan chan *protos.Message, errchan chan error, err error) {
	req := &protos.SubscribeRequest{
		TopicName: topic,
		Offset:    offset,
		EndOffset: end_offset,
	}
	stream, err := ps.client.Subscribe(context.Background(), req)
	if err != nil {
		return
	}
	rchan = make(chan *protos.Message, 1)
	errchan = make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err == nil {
				rchan <- msg
			} else {
				if err == io.EOF {
					rchan <- nil
				} else if err != nil {
					errchan <- nil
				}
				break
			}
		}
	}()
	return rchan, errchan, nil
}

func (ps *PubSub) Close() {
	ps.conn.Close()
}
