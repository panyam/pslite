// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package pslite

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// PSLiteServiceClient is the client API for PSLiteService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type PSLiteServiceClient interface {
	OpenTopic(ctx context.Context, in *OpenTopicRequest, opts ...grpc.CallOption) (*EmptyMessage, error)
	Publish(ctx context.Context, in *PublishRequest, opts ...grpc.CallOption) (*EmptyMessage, error)
	Subscribe(ctx context.Context, in *SubscribeRequest, opts ...grpc.CallOption) (PSLiteService_SubscribeClient, error)
}

type pSLiteServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewPSLiteServiceClient(cc grpc.ClientConnInterface) PSLiteServiceClient {
	return &pSLiteServiceClient{cc}
}

func (c *pSLiteServiceClient) OpenTopic(ctx context.Context, in *OpenTopicRequest, opts ...grpc.CallOption) (*EmptyMessage, error) {
	out := new(EmptyMessage)
	err := c.cc.Invoke(ctx, "/pslite_protos.PSLiteService/OpenTopic", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *pSLiteServiceClient) Publish(ctx context.Context, in *PublishRequest, opts ...grpc.CallOption) (*EmptyMessage, error) {
	out := new(EmptyMessage)
	err := c.cc.Invoke(ctx, "/pslite_protos.PSLiteService/Publish", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *pSLiteServiceClient) Subscribe(ctx context.Context, in *SubscribeRequest, opts ...grpc.CallOption) (PSLiteService_SubscribeClient, error) {
	stream, err := c.cc.NewStream(ctx, &PSLiteService_ServiceDesc.Streams[0], "/pslite_protos.PSLiteService/Subscribe", opts...)
	if err != nil {
		return nil, err
	}
	x := &pSLiteServiceSubscribeClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type PSLiteService_SubscribeClient interface {
	Recv() (*Message, error)
	grpc.ClientStream
}

type pSLiteServiceSubscribeClient struct {
	grpc.ClientStream
}

func (x *pSLiteServiceSubscribeClient) Recv() (*Message, error) {
	m := new(Message)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// PSLiteServiceServer is the server API for PSLiteService service.
// All implementations must embed UnimplementedPSLiteServiceServer
// for forward compatibility
type PSLiteServiceServer interface {
	OpenTopic(context.Context, *OpenTopicRequest) (*EmptyMessage, error)
	Publish(context.Context, *PublishRequest) (*EmptyMessage, error)
	Subscribe(*SubscribeRequest, PSLiteService_SubscribeServer) error
	mustEmbedUnimplementedPSLiteServiceServer()
}

// UnimplementedPSLiteServiceServer must be embedded to have forward compatible implementations.
type UnimplementedPSLiteServiceServer struct {
}

func (UnimplementedPSLiteServiceServer) OpenTopic(context.Context, *OpenTopicRequest) (*EmptyMessage, error) {
	return nil, status.Errorf(codes.Unimplemented, "method OpenTopic not implemented")
}
func (UnimplementedPSLiteServiceServer) Publish(context.Context, *PublishRequest) (*EmptyMessage, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Publish not implemented")
}
func (UnimplementedPSLiteServiceServer) Subscribe(*SubscribeRequest, PSLiteService_SubscribeServer) error {
	return status.Errorf(codes.Unimplemented, "method Subscribe not implemented")
}
func (UnimplementedPSLiteServiceServer) mustEmbedUnimplementedPSLiteServiceServer() {}

// UnsafePSLiteServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to PSLiteServiceServer will
// result in compilation errors.
type UnsafePSLiteServiceServer interface {
	mustEmbedUnimplementedPSLiteServiceServer()
}

func RegisterPSLiteServiceServer(s grpc.ServiceRegistrar, srv PSLiteServiceServer) {
	s.RegisterService(&PSLiteService_ServiceDesc, srv)
}

func _PSLiteService_OpenTopic_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(OpenTopicRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PSLiteServiceServer).OpenTopic(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/pslite_protos.PSLiteService/OpenTopic",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PSLiteServiceServer).OpenTopic(ctx, req.(*OpenTopicRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PSLiteService_Publish_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PublishRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PSLiteServiceServer).Publish(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/pslite_protos.PSLiteService/Publish",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PSLiteServiceServer).Publish(ctx, req.(*PublishRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _PSLiteService_Subscribe_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(SubscribeRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(PSLiteServiceServer).Subscribe(m, &pSLiteServiceSubscribeServer{stream})
}

type PSLiteService_SubscribeServer interface {
	Send(*Message) error
	grpc.ServerStream
}

type pSLiteServiceSubscribeServer struct {
	grpc.ServerStream
}

func (x *pSLiteServiceSubscribeServer) Send(m *Message) error {
	return x.ServerStream.SendMsg(m)
}

// PSLiteService_ServiceDesc is the grpc.ServiceDesc for PSLiteService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var PSLiteService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "pslite_protos.PSLiteService",
	HandlerType: (*PSLiteServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "OpenTopic",
			Handler:    _PSLiteService_OpenTopic_Handler,
		},
		{
			MethodName: "Publish",
			Handler:    _PSLiteService_Publish_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Subscribe",
			Handler:       _PSLiteService_Subscribe_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "protos/pslite.proto",
}
