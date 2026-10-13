package api

import (
	"context"

	"google.golang.org/grpc"
)

// ServiceName is the fully qualified gRPC service name.
const ServiceName = "aries.vmd.v1.VMD"

// Server is implemented by the daemon.
type Server interface {
	Health(context.Context, *HealthRequest) (*HealthReply, error)
	ImportImage(grpc.ClientStreamingServer[ImageChunk, ImageInfo]) error
	InspectImage(context.Context, *ImageRequest) (*ImageInfo, error)
	CreateNetwork(context.Context, *NetworkSpec) (*NetworkInfo, error)
	InspectNetwork(context.Context, *NetworkRef) (*NetworkInfo, error)
	DeleteNetwork(context.Context, *NetworkRef) (*Empty, error)
	Create(context.Context, *SandboxSpec) (*SandboxInfo, error)
	Inspect(context.Context, *SandboxRef) (*SandboxInfo, error)
	Start(context.Context, *SandboxRef) (*Empty, error)
	Delete(context.Context, *SandboxRef) (*Empty, error)
	Exec(grpc.BidiStreamingServer[ExecFrame, ExecFrame]) error
	PutArchive(grpc.ClientStreamingServer[ArchiveChunk, Empty]) error
	GetArchive(*ArchiveRequest, grpc.ServerStreamingServer[ArchiveChunk]) error
	Logs(*LogsRequest, grpc.ServerStreamingServer[LogChunk]) error
	Stats(context.Context, *StatsRequest) (*StatsReply, error)
	List(context.Context, *ListRequest) (*ListReply, error)
	Cleanup(context.Context, *CleanupRequest) (*CleanupReply, error)
}

// Client is the typed daemon client. Unary errors carry gRPC status codes;
// codes.NotFound always means the named sandbox, network, or image is absent.
type Client struct {
	conn grpc.ClientConnInterface
}

// NewClient wraps a connection created with CallOptions.
func NewClient(conn grpc.ClientConnInterface) *Client { return &Client{conn: conn} }

// CallOptions selects the JSON codec; pass them to grpc.WithDefaultCallOptions.
func CallOptions() []grpc.CallOption { return []grpc.CallOption{grpc.ForceCodec(Codec{})} }

// ServerOptions selects the JSON codec for a grpc.Server.
func ServerOptions() []grpc.ServerOption { return []grpc.ServerOption{grpc.ForceServerCodec(Codec{})} }

func method(name string) string { return "/" + ServiceName + "/" + name }

func unary[Req, Rep any](ctx context.Context, c *Client, name string, request *Req) (*Rep, error) {
	reply := new(Rep)
	if err := c.conn.Invoke(ctx, method(name), request, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func (c *Client) Health(ctx context.Context, r *HealthRequest) (*HealthReply, error) {
	return unary[HealthRequest, HealthReply](ctx, c, "Health", r)
}
func (c *Client) InspectImage(ctx context.Context, r *ImageRequest) (*ImageInfo, error) {
	return unary[ImageRequest, ImageInfo](ctx, c, "InspectImage", r)
}
func (c *Client) CreateNetwork(ctx context.Context, r *NetworkSpec) (*NetworkInfo, error) {
	return unary[NetworkSpec, NetworkInfo](ctx, c, "CreateNetwork", r)
}
func (c *Client) InspectNetwork(ctx context.Context, r *NetworkRef) (*NetworkInfo, error) {
	return unary[NetworkRef, NetworkInfo](ctx, c, "InspectNetwork", r)
}
func (c *Client) DeleteNetwork(ctx context.Context, r *NetworkRef) (*Empty, error) {
	return unary[NetworkRef, Empty](ctx, c, "DeleteNetwork", r)
}
func (c *Client) Create(ctx context.Context, r *SandboxSpec) (*SandboxInfo, error) {
	return unary[SandboxSpec, SandboxInfo](ctx, c, "Create", r)
}
func (c *Client) Inspect(ctx context.Context, r *SandboxRef) (*SandboxInfo, error) {
	return unary[SandboxRef, SandboxInfo](ctx, c, "Inspect", r)
}
func (c *Client) Start(ctx context.Context, r *SandboxRef) (*Empty, error) {
	return unary[SandboxRef, Empty](ctx, c, "Start", r)
}
func (c *Client) Delete(ctx context.Context, r *SandboxRef) (*Empty, error) {
	return unary[SandboxRef, Empty](ctx, c, "Delete", r)
}
func (c *Client) Stats(ctx context.Context, r *StatsRequest) (*StatsReply, error) {
	return unary[StatsRequest, StatsReply](ctx, c, "Stats", r)
}
func (c *Client) List(ctx context.Context, r *ListRequest) (*ListReply, error) {
	return unary[ListRequest, ListReply](ctx, c, "List", r)
}
func (c *Client) Cleanup(ctx context.Context, r *CleanupRequest) (*CleanupReply, error) {
	return unary[CleanupRequest, CleanupReply](ctx, c, "Cleanup", r)
}

func (c *Client) ImportImage(ctx context.Context) (grpc.ClientStreamingClient[ImageChunk, ImageInfo], error) {
	stream, err := c.conn.NewStream(ctx, &serviceDesc.Streams[0], method("ImportImage"))
	if err != nil {
		return nil, err
	}
	return &grpc.GenericClientStream[ImageChunk, ImageInfo]{ClientStream: stream}, nil
}

func (c *Client) Exec(ctx context.Context) (grpc.BidiStreamingClient[ExecFrame, ExecFrame], error) {
	stream, err := c.conn.NewStream(ctx, &serviceDesc.Streams[1], method("Exec"))
	if err != nil {
		return nil, err
	}
	return &grpc.GenericClientStream[ExecFrame, ExecFrame]{ClientStream: stream}, nil
}

func (c *Client) PutArchive(ctx context.Context) (grpc.ClientStreamingClient[ArchiveChunk, Empty], error) {
	stream, err := c.conn.NewStream(ctx, &serviceDesc.Streams[2], method("PutArchive"))
	if err != nil {
		return nil, err
	}
	return &grpc.GenericClientStream[ArchiveChunk, Empty]{ClientStream: stream}, nil
}

func serverStream[Req, Rep any](ctx context.Context, c *Client, index int, name string, request *Req) (grpc.ServerStreamingClient[Rep], error) {
	stream, err := c.conn.NewStream(ctx, &serviceDesc.Streams[index], method(name))
	if err != nil {
		return nil, err
	}
	typed := &grpc.GenericClientStream[Req, Rep]{ClientStream: stream}
	if err := typed.ClientStream.SendMsg(request); err != nil {
		return nil, err
	}
	if err := typed.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return typed, nil
}

func (c *Client) GetArchive(ctx context.Context, r *ArchiveRequest) (grpc.ServerStreamingClient[ArchiveChunk], error) {
	return serverStream[ArchiveRequest, ArchiveChunk](ctx, c, 3, "GetArchive", r)
}

func (c *Client) Logs(ctx context.Context, r *LogsRequest) (grpc.ServerStreamingClient[LogChunk], error) {
	return serverStream[LogsRequest, LogChunk](ctx, c, 4, "Logs", r)
}

// Register installs server on registrar.
func Register(registrar grpc.ServiceRegistrar, server Server) {
	registrar.RegisterService(&serviceDesc, server)
}

func unaryHandler[Req, Rep any](call func(Server, context.Context, *Req) (*Rep, error)) grpc.MethodHandler {
	return func(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		request := new(Req)
		if err := decode(request); err != nil {
			return nil, err
		}
		if interceptor == nil {
			return call(srv.(Server), ctx, request)
		}
		info := &grpc.UnaryServerInfo{Server: srv}
		return interceptor(ctx, request, info, func(ctx context.Context, request any) (any, error) {
			return call(srv.(Server), ctx, request.(*Req))
		})
	}
}

func serverStreamHandler[Req, Rep any](call func(Server, *Req, grpc.ServerStreamingServer[Rep]) error) grpc.StreamHandler {
	return func(srv any, stream grpc.ServerStream) error {
		request := new(Req)
		if err := stream.RecvMsg(request); err != nil {
			return err
		}
		return call(srv.(Server), request, &grpc.GenericServerStream[Req, Rep]{ServerStream: stream})
	}
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: ServiceName,
	HandlerType: (*Server)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Health", Handler: unaryHandler(Server.Health)},
		{MethodName: "InspectImage", Handler: unaryHandler(Server.InspectImage)},
		{MethodName: "CreateNetwork", Handler: unaryHandler(Server.CreateNetwork)},
		{MethodName: "InspectNetwork", Handler: unaryHandler(Server.InspectNetwork)},
		{MethodName: "DeleteNetwork", Handler: unaryHandler(Server.DeleteNetwork)},
		{MethodName: "Create", Handler: unaryHandler(Server.Create)},
		{MethodName: "Inspect", Handler: unaryHandler(Server.Inspect)},
		{MethodName: "Start", Handler: unaryHandler(Server.Start)},
		{MethodName: "Delete", Handler: unaryHandler(Server.Delete)},
		{MethodName: "Stats", Handler: unaryHandler(Server.Stats)},
		{MethodName: "List", Handler: unaryHandler(Server.List)},
		{MethodName: "Cleanup", Handler: unaryHandler(Server.Cleanup)},
	},
	// Client stubs index Streams by position; keep the order stable.
	Streams: []grpc.StreamDesc{
		{StreamName: "ImportImage", ClientStreams: true, Handler: func(srv any, stream grpc.ServerStream) error {
			return srv.(Server).ImportImage(&grpc.GenericServerStream[ImageChunk, ImageInfo]{ServerStream: stream})
		}},
		{StreamName: "Exec", ClientStreams: true, ServerStreams: true, Handler: func(srv any, stream grpc.ServerStream) error {
			return srv.(Server).Exec(&grpc.GenericServerStream[ExecFrame, ExecFrame]{ServerStream: stream})
		}},
		{StreamName: "PutArchive", ClientStreams: true, Handler: func(srv any, stream grpc.ServerStream) error {
			return srv.(Server).PutArchive(&grpc.GenericServerStream[ArchiveChunk, Empty]{ServerStream: stream})
		}},
		{StreamName: "GetArchive", ServerStreams: true, Handler: serverStreamHandler(Server.GetArchive)},
		{StreamName: "Logs", ServerStreams: true, Handler: serverStreamHandler(Server.Logs)},
	},
	Metadata: "pkg/vmd/api",
}
