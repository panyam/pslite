package utils

import (
	"context"
	protos "github.com/panyam/klite/protos"
	"google.golang.org/grpc"
	"io"
	"path"
)

type PubSub struct {
	serverAddr string
	conn       *grpc.ClientConn
	client     protos.KLiteServiceClient
}

func NewPubSub(serverAddr string) (out *PubSub, err error) {
	out = &PubSub{
		serverAddr: serverAddr,
	}
	out.conn, err = grpc.Dial(serverAddr, grpc.WithInsecure())
	if err == nil {
		out.client = protos.NewKLiteServiceClient(out.conn)
	}
	return
}

func (ps *PubSub) EnsureTopic(topic string, folder string) error {
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

func (ps *PubSub) Publish(topic string, data string) error {
	pubreq := &protos.PublishRequest{
		TopicName: topic,
		Content: &protos.PublishRequest_ContentString{
			ContentString: data,
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
