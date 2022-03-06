package services

import (
	"context"
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

func FromTopicProto(topic *protos.Topic) *core.KLTopic {
	return &core.KLTopic{
		Name:              topic.Name,
		TopicFolder:       topic.TopicFolder,
		CheckpointTimeout: topic.CheckpointTimeout,
		IncludeTimestamp:  topic.IncludeTimestamp,
		CreateIndexes:     topic.CreateIndexes,
	}
}

func ToTopicProto(topic *core.KLTopic) *protos.Topic {
	return &protos.Topic{
		Name:              topic.Name,
		TopicFolder:       topic.TopicFolder,
		CheckpointTimeout: topic.CheckpointTimeout,
		IncludeTimestamp:  topic.IncludeTimestamp,
		CreateIndexes:     topic.CreateIndexes,
	}
}

func (s *StreamerService) CreateTopic(ctx context.Context, request *protos.CreateTopicRequest) (*protos.Topic, error) {
	topic_proto := request.Topic
	topic := FromTopicProto(topic_proto)
	err := core.NewTopic(topic)
	return ToTopicProto(topic), err
}

func (s *StreamerService) Publish(ctx context.Context, pubreq *protos.PublishRequest) (*protos.EmptyMessage, error) {
	topic := s.Engine.GetTopic(pubreq.TopicName).(*core.KLTopic)
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
	return nil, topic.Publish(msg, -1, -1)
}

func (s *StreamerService) Subscribe(subreq *protos.SubscribeRequest, stream protos.StreamerService_SubscribeServer) error {
	topic := s.Engine.GetTopic(subreq.TopicName).(*core.KLTopic)
	if topic == nil {
		// Topic does not exist
		return status.Error(codes.NotFound, fmt.Sprintf("Topic (%s) does not exist", subreq.TopicName))
	}

	readerChan, err := topic.Subscribe(subreq.Offset, subreq.EndOffset, subreq.ByIndex)
	if err != nil {
		return err
	}
	stop := false
	for !stop {
		select {
		case nextMsg := <-readerChan:
			if nextMsg == nil {
				stop = true
				// Done so we can stop now
				break
			}
			break
		case <-stream.Context().Done():
			// Client disconnected so can stop now
			close(readerChan)
			stop = true
			break
		}
	}
	return nil
}
