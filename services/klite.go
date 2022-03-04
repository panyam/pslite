package services

import (
	// "context"
	"fmt"
	"github.com/panyam/klite/core"
	protos "github.com/panyam/klite/protos"
	// "github.com/panyam/klite/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	// structpb "google.golang.org/protobuf/types/known/structpb"
	// "log"
)

type StreamerService struct {
	protos.UnimplementedStreamerServiceServer
	Engine *core.KLEngine
	subs   map[string]*Subscription
}

func NewStreamerService(engine *core.KLEngine) *StreamerService {
	out := StreamerService{
		Engine: engine,
		subs:   make(map[string]*Subscription),
	}
	return &out
}

func (s *StreamerService) Subscribe(subreq *protos.SubscribeRequest, stream protos.StreamerService_SubscribeServer) error {
	topic := s.Engine.GetTopic(subreq.TopicName)
	if topic == nil {
		// Already exists and a client is listening to it so return error
		return status.Error(codes.NotFound, fmt.Sprintf("Topic (%s) already running", subreq.TopicName))
	}
	/*
		newSocket := td.NewSocket(s.TDClient, nil)
		sub := NewSubscription(name, newSocket)
		s.subs[name] = sub
		go sub.Socket.Connect()

		// Now read from the channel that was created and pump it out
		for {
			newMessage := <-sub.Socket.ReaderChannel()
			if newMessage == nil {
				break
			}
			info, err := structpb.NewStruct(newMessage)
			if err != nil {
				return err
			}
			msgproto := protos.Message{Info: info}
			if err := stream.Send(&msgproto); err != nil {
				log.Printf("%v.Send(%v) = %v", stream, &msgproto, err)
				return err
			}
		}
		delete(s.subs, name)
	*/
	/*
		reply, err := stream.CloseAndRecv()
		if err != nil {
			log.Printf("%v.CloseAndRecv() got error %v, want %v", stream, err, nil)
			return err
		}
	*/
	return nil
}
