package services

import (
	"context"
	"fmt"
	"github.com/panyam/pslite/core"
	protos "github.com/panyam/pslite/protos"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	// structpb "google.golang.org/protobuf/types/known/structpb"
	// "log"
)

type PSLiteService struct {
	protos.UnimplementedPSLiteServiceServer
	Engine *core.KLEngine
	subs   map[string]*Subscription
}

func NewPSLiteService(engine *core.KLEngine) *PSLiteService {
	out := PSLiteService{
		Engine: engine,
		subs:   make(map[string]*Subscription),
	}
	return &out
}

func FromTopicProto(topic *protos.Topic) *core.FileTopic {
	return &core.FileTopic{
		Name:        topic.Name,
		RecordsPath: topic.RecordsPath,
		IndexPath:   topic.IndexPath,
	}
}

func ToTopicProto(topic *core.FileTopic) *protos.Topic {
	return &protos.Topic{
		Name:        topic.Name,
		RecordsPath: topic.RecordsPath,
		IndexPath:   topic.IndexPath,
	}
}

func (s *PSLiteService) OpenTopic(ctx context.Context, request *protos.OpenTopicRequest) (out *protos.EmptyMessage, err error) {
	topic_proto := request.Topic
	topic := s.Engine.GetTopic(topic_proto.Name)
	if topic == nil {
		topic, err = core.NewFileTopic(topic_proto.Name, topic_proto.RecordsPath, topic_proto.IndexPath)
		if err == nil {
			s.Engine.AddTopic(topic_proto.Name, topic)
		}
	}
	return &protos.EmptyMessage{}, err
}

func (s *PSLiteService) Publish(ctx context.Context, pubreq *protos.PublishRequest) (*protos.EmptyMessage, error) {
	topic := s.Engine.GetTopic(pubreq.TopicName)
	if topic == nil {
		// Topic does not exist
		return nil, status.Error(codes.NotFound, fmt.Sprintf("Topic (%s) does not exist", pubreq.TopicName))
	}
	var msg []byte
	switch pubreq.Content.(type) {
	case *protos.PublishRequest_ContentString:
		msg = []byte(pubreq.GetContentString())
		break
	case *protos.PublishRequest_ContentBytes:
		msg = pubreq.GetContentBytes()
		break
	}
	_, err := topic.Publish(msg)
	return &protos.EmptyMessage{}, err
}

func (s *PSLiteService) Subscribe(subreq *protos.SubscribeRequest, stream protos.PSLiteService_SubscribeServer) error {
	topic := s.Engine.GetTopic(subreq.TopicName)
	if topic == nil {
		// Topic does not exist
		return status.Error(codes.NotFound, fmt.Sprintf("Topic (%s) does not exist", subreq.TopicName))
	}

	offset := subreq.Offset
	end_offset := subreq.EndOffset
	sub, err := topic.Subscribe(offset)
	if err != nil {
		return err
	}
	closed := false
	go func() {
		select {
		case <-stream.Context().Done():
			// Client disconnected so can stop now
			log.Println("Client disconnected.")
			closed = true
			sub.Close()
		}
	}()

	var msg core.Message
	buffer := make([]byte, 4096)
	for curr_offset := subreq.Offset; end_offset < offset || curr_offset < end_offset; curr_offset += 1 {
		msg, err = sub.NextMessage(end_offset < offset)
		if err == nil {
			log.Printf("Msg Offset: %d, Len: %d", curr_offset, msg.Length())
			if msg.Length() > int64(len(buffer)) {
				buffer = make([]byte, msg.Length())
			}
			_, err = msg.Read(buffer[:msg.Length()], true)
		}
		if err != nil {
			break
		} else {
			data := buffer[:msg.Length()]
			msg := protos.Message{Content: data}
			if err = stream.Send(&msg); err != nil {
				log.Printf("%v.Send(%v) = %v", stream, &msg, err)
				break
			}
		}
	}

	if !closed {
		sub.Close()
	}
	return err
}
