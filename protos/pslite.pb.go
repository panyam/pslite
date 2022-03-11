// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.19.4
// source: protos/pslite.proto

package pslite

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type EmptyMessage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *EmptyMessage) Reset() {
	*x = EmptyMessage{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_pslite_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *EmptyMessage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EmptyMessage) ProtoMessage() {}

func (x *EmptyMessage) ProtoReflect() protoreflect.Message {
	mi := &file_protos_pslite_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EmptyMessage.ProtoReflect.Descriptor instead.
func (*EmptyMessage) Descriptor() ([]byte, []int) {
	return file_protos_pslite_proto_rawDescGZIP(), []int{0}
}

type Topic struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	//*
	// Name of the topic.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	//*
	// Folder where topic will be persisted.
	RecordsPath string `protobuf:"bytes,2,opt,name=records_path,json=recordsPath,proto3" json:"records_path,omitempty"`
	//*
	// Folder where topic indexes will be persisted.
	IndexPath string `protobuf:"bytes,3,opt,name=index_path,json=indexPath,proto3" json:"index_path,omitempty"`
}

func (x *Topic) Reset() {
	*x = Topic{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_pslite_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Topic) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Topic) ProtoMessage() {}

func (x *Topic) ProtoReflect() protoreflect.Message {
	mi := &file_protos_pslite_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Topic.ProtoReflect.Descriptor instead.
func (*Topic) Descriptor() ([]byte, []int) {
	return file_protos_pslite_proto_rawDescGZIP(), []int{1}
}

func (x *Topic) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Topic) GetRecordsPath() string {
	if x != nil {
		return x.RecordsPath
	}
	return ""
}

func (x *Topic) GetIndexPath() string {
	if x != nil {
		return x.IndexPath
	}
	return ""
}

type OpenTopicRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Topic *Topic `protobuf:"bytes,1,opt,name=topic,proto3" json:"topic,omitempty"`
}

func (x *OpenTopicRequest) Reset() {
	*x = OpenTopicRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_pslite_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OpenTopicRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OpenTopicRequest) ProtoMessage() {}

func (x *OpenTopicRequest) ProtoReflect() protoreflect.Message {
	mi := &file_protos_pslite_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OpenTopicRequest.ProtoReflect.Descriptor instead.
func (*OpenTopicRequest) Descriptor() ([]byte, []int) {
	return file_protos_pslite_proto_rawDescGZIP(), []int{2}
}

func (x *OpenTopicRequest) GetTopic() *Topic {
	if x != nil {
		return x.Topic
	}
	return nil
}

type PublishRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	//*
	// Name of the topic to publish to.
	TopicName string `protobuf:"bytes,1,opt,name=topic_name,json=topicName,proto3" json:"topic_name,omitempty"`
	//*
	// Content can be bytes or strings.
	//
	// Types that are assignable to Content:
	//	*PublishRequest_ContentBytes
	//	*PublishRequest_ContentString
	Content isPublishRequest_Content `protobuf_oneof:"content"`
}

func (x *PublishRequest) Reset() {
	*x = PublishRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_pslite_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PublishRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PublishRequest) ProtoMessage() {}

func (x *PublishRequest) ProtoReflect() protoreflect.Message {
	mi := &file_protos_pslite_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PublishRequest.ProtoReflect.Descriptor instead.
func (*PublishRequest) Descriptor() ([]byte, []int) {
	return file_protos_pslite_proto_rawDescGZIP(), []int{3}
}

func (x *PublishRequest) GetTopicName() string {
	if x != nil {
		return x.TopicName
	}
	return ""
}

func (m *PublishRequest) GetContent() isPublishRequest_Content {
	if m != nil {
		return m.Content
	}
	return nil
}

func (x *PublishRequest) GetContentBytes() []byte {
	if x, ok := x.GetContent().(*PublishRequest_ContentBytes); ok {
		return x.ContentBytes
	}
	return nil
}

func (x *PublishRequest) GetContentString() string {
	if x, ok := x.GetContent().(*PublishRequest_ContentString); ok {
		return x.ContentString
	}
	return ""
}

type isPublishRequest_Content interface {
	isPublishRequest_Content()
}

type PublishRequest_ContentBytes struct {
	ContentBytes []byte `protobuf:"bytes,2,opt,name=content_bytes,json=contentBytes,proto3,oneof"`
}

type PublishRequest_ContentString struct {
	ContentString string `protobuf:"bytes,3,opt,name=content_string,json=contentString,proto3,oneof"`
}

func (*PublishRequest_ContentBytes) isPublishRequest_Content() {}

func (*PublishRequest_ContentString) isPublishRequest_Content() {}

type SubscribeRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	//*
	// Name of the topic to subscribe to.
	TopicName string `protobuf:"bytes,1,opt,name=topic_name,json=topicName,proto3" json:"topic_name,omitempty"`
	//*
	// Offset of the topic to start consuming from.
	Offset int64 `protobuf:"varint,2,opt,name=offset,proto3" json:"offset,omitempty"`
	//*
	// The end offset.  If this is specified, the subscription will end as soon as this offset is reached.
	EndOffset int64 `protobuf:"varint,3,opt,name=end_offset,json=endOffset,proto3" json:"end_offset,omitempty"`
}

func (x *SubscribeRequest) Reset() {
	*x = SubscribeRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_pslite_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SubscribeRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SubscribeRequest) ProtoMessage() {}

func (x *SubscribeRequest) ProtoReflect() protoreflect.Message {
	mi := &file_protos_pslite_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SubscribeRequest.ProtoReflect.Descriptor instead.
func (*SubscribeRequest) Descriptor() ([]byte, []int) {
	return file_protos_pslite_proto_rawDescGZIP(), []int{4}
}

func (x *SubscribeRequest) GetTopicName() string {
	if x != nil {
		return x.TopicName
	}
	return ""
}

func (x *SubscribeRequest) GetOffset() int64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

func (x *SubscribeRequest) GetEndOffset() int64 {
	if x != nil {
		return x.EndOffset
	}
	return 0
}

type Message struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Content []byte `protobuf:"bytes,1,opt,name=content,proto3" json:"content,omitempty"`
	Offset  int64  `protobuf:"varint,2,opt,name=offset,proto3" json:"offset,omitempty"`
	Length  int64  `protobuf:"varint,3,opt,name=length,proto3" json:"length,omitempty"`
}

func (x *Message) Reset() {
	*x = Message{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_pslite_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Message) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Message) ProtoMessage() {}

func (x *Message) ProtoReflect() protoreflect.Message {
	mi := &file_protos_pslite_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Message.ProtoReflect.Descriptor instead.
func (*Message) Descriptor() ([]byte, []int) {
	return file_protos_pslite_proto_rawDescGZIP(), []int{5}
}

func (x *Message) GetContent() []byte {
	if x != nil {
		return x.Content
	}
	return nil
}

func (x *Message) GetOffset() int64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

func (x *Message) GetLength() int64 {
	if x != nil {
		return x.Length
	}
	return 0
}

var File_protos_pslite_proto protoreflect.FileDescriptor

var file_protos_pslite_proto_rawDesc = []byte{
	0x0a, 0x13, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f, 0x70, 0x73, 0x6c, 0x69, 0x74, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x22, 0x0e, 0x0a,
	0x0c, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x5d, 0x0a,
	0x05, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x21, 0x0a, 0x0c, 0x72, 0x65,
	0x63, 0x6f, 0x72, 0x64, 0x73, 0x5f, 0x70, 0x61, 0x74, 0x68, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x0b, 0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x73, 0x50, 0x61, 0x74, 0x68, 0x12, 0x1d, 0x0a,
	0x0a, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x5f, 0x70, 0x61, 0x74, 0x68, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x09, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x50, 0x61, 0x74, 0x68, 0x22, 0x37, 0x0a, 0x10,
	0x4f, 0x70, 0x65, 0x6e, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x23, 0x0a, 0x05, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x0d, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2e, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x52, 0x05,
	0x74, 0x6f, 0x70, 0x69, 0x63, 0x22, 0x8a, 0x01, 0x0a, 0x0e, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73,
	0x68, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x6f, 0x70, 0x69,
	0x63, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x74, 0x6f,
	0x70, 0x69, 0x63, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x25, 0x0a, 0x0d, 0x63, 0x6f, 0x6e, 0x74, 0x65,
	0x6e, 0x74, 0x5f, 0x62, 0x79, 0x74, 0x65, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x48, 0x00,
	0x52, 0x0c, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x42, 0x79, 0x74, 0x65, 0x73, 0x12, 0x27,
	0x0a, 0x0e, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x5f, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x0d, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e,
	0x74, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x42, 0x09, 0x0a, 0x07, 0x63, 0x6f, 0x6e, 0x74, 0x65,
	0x6e, 0x74, 0x22, 0x68, 0x0a, 0x10, 0x53, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x62, 0x65, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x5f,
	0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x74, 0x6f, 0x70, 0x69,
	0x63, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x12, 0x1d, 0x0a,
	0x0a, 0x65, 0x6e, 0x64, 0x5f, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x09, 0x65, 0x6e, 0x64, 0x4f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x22, 0x53, 0x0a, 0x07,
	0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x63, 0x6f, 0x6e, 0x74, 0x65,
	0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e,
	0x74, 0x12, 0x16, 0x0a, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x12, 0x16, 0x0a, 0x06, 0x6c, 0x65, 0x6e,
	0x67, 0x74, 0x68, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x06, 0x6c, 0x65, 0x6e, 0x67, 0x74,
	0x68, 0x32, 0xbf, 0x01, 0x0a, 0x0d, 0x50, 0x53, 0x4c, 0x69, 0x74, 0x65, 0x53, 0x65, 0x72, 0x76,
	0x69, 0x63, 0x65, 0x12, 0x3b, 0x0a, 0x09, 0x4f, 0x70, 0x65, 0x6e, 0x54, 0x6f, 0x70, 0x69, 0x63,
	0x12, 0x18, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2e, 0x4f, 0x70, 0x65, 0x6e, 0x54, 0x6f,
	0x70, 0x69, 0x63, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x14, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x73, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x12, 0x37, 0x0a, 0x07, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68, 0x12, 0x16, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x73, 0x2e, 0x50, 0x75, 0x62, 0x6c, 0x69, 0x73, 0x68, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x14, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2e, 0x45, 0x6d, 0x70,
	0x74, 0x79, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x38, 0x0a, 0x09, 0x53, 0x75, 0x62,
	0x73, 0x63, 0x72, 0x69, 0x62, 0x65, 0x12, 0x18, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2e,
	0x53, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x62, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x1a, 0x0f, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2e, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x30, 0x01, 0x42, 0x1a, 0x5a, 0x18, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f,
	0x6d, 0x2f, 0x70, 0x61, 0x6e, 0x79, 0x61, 0x6d, 0x2f, 0x70, 0x73, 0x6c, 0x69, 0x74, 0x65, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_protos_pslite_proto_rawDescOnce sync.Once
	file_protos_pslite_proto_rawDescData = file_protos_pslite_proto_rawDesc
)

func file_protos_pslite_proto_rawDescGZIP() []byte {
	file_protos_pslite_proto_rawDescOnce.Do(func() {
		file_protos_pslite_proto_rawDescData = protoimpl.X.CompressGZIP(file_protos_pslite_proto_rawDescData)
	})
	return file_protos_pslite_proto_rawDescData
}

var file_protos_pslite_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_protos_pslite_proto_goTypes = []interface{}{
	(*EmptyMessage)(nil),     // 0: protos.EmptyMessage
	(*Topic)(nil),            // 1: protos.Topic
	(*OpenTopicRequest)(nil), // 2: protos.OpenTopicRequest
	(*PublishRequest)(nil),   // 3: protos.PublishRequest
	(*SubscribeRequest)(nil), // 4: protos.SubscribeRequest
	(*Message)(nil),          // 5: protos.Message
}
var file_protos_pslite_proto_depIdxs = []int32{
	1, // 0: protos.OpenTopicRequest.topic:type_name -> protos.Topic
	2, // 1: protos.PSLiteService.OpenTopic:input_type -> protos.OpenTopicRequest
	3, // 2: protos.PSLiteService.Publish:input_type -> protos.PublishRequest
	4, // 3: protos.PSLiteService.Subscribe:input_type -> protos.SubscribeRequest
	0, // 4: protos.PSLiteService.OpenTopic:output_type -> protos.EmptyMessage
	0, // 5: protos.PSLiteService.Publish:output_type -> protos.EmptyMessage
	5, // 6: protos.PSLiteService.Subscribe:output_type -> protos.Message
	4, // [4:7] is the sub-list for method output_type
	1, // [1:4] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_protos_pslite_proto_init() }
func file_protos_pslite_proto_init() {
	if File_protos_pslite_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_protos_pslite_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*EmptyMessage); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_protos_pslite_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Topic); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_protos_pslite_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OpenTopicRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_protos_pslite_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PublishRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_protos_pslite_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SubscribeRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_protos_pslite_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Message); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_protos_pslite_proto_msgTypes[3].OneofWrappers = []interface{}{
		(*PublishRequest_ContentBytes)(nil),
		(*PublishRequest_ContentString)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_protos_pslite_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_protos_pslite_proto_goTypes,
		DependencyIndexes: file_protos_pslite_proto_depIdxs,
		MessageInfos:      file_protos_pslite_proto_msgTypes,
	}.Build()
	File_protos_pslite_proto = out.File
	file_protos_pslite_proto_rawDesc = nil
	file_protos_pslite_proto_goTypes = nil
	file_protos_pslite_proto_depIdxs = nil
}