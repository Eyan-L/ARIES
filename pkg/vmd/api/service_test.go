package api

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeServer struct{ Server }

func (fakeServer) Health(context.Context, *HealthRequest) (*HealthReply, error) {
	return &HealthReply{APIVersion: Version, Isolation: IsolationKataFirecracker}, nil
}
func (fakeServer) Inspect(_ context.Context, ref *SandboxRef) (*SandboxInfo, error) {
	return nil, status.Errorf(codes.NotFound, "sandbox %s not found", ref.ID)
}
func (fakeServer) Exec(stream grpc.BidiStreamingServer[ExecFrame, ExecFrame]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	for {
		frame, err := stream.Recv()
		if err != nil {
			return err
		}
		if frame.Stdin != nil {
			if err := stream.Send(&ExecFrame{Stdout: frame.Stdin}); err != nil {
				return err
			}
		}
		if frame.StdinEOF {
			return stream.Send(&ExecFrame{Exit: &ExecExit{Code: len(first.Start.Args)}})
		}
	}
}
func (fakeServer) GetArchive(request *ArchiveRequest, stream grpc.ServerStreamingServer[ArchiveChunk]) error {
	if err := stream.Send(&ArchiveChunk{Stat: &FileStat{Name: filepath.Base(request.Path), Size: 3, Mode: 0o644}}); err != nil {
		return err
	}
	return stream.Send(&ArchiveChunk{Data: []byte("tar")})
}

func TestServiceRoundTrip(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(ServerOptions()...)
	Register(server, fakeServer{})
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///vmd", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(CallOptions()...))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := NewClient(conn)
	ctx := context.Background()

	health, err := client.Health(ctx, &HealthRequest{})
	if err != nil || health.Isolation != IsolationKataFirecracker {
		t.Fatalf("health = %+v, %v", health, err)
	}
	if _, err := client.Inspect(ctx, &SandboxRef{ID: "x"}); status.Code(err) != codes.NotFound {
		t.Fatalf("inspect error = %v, want NotFound", err)
	}

	exec, err := client.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.Send(&ExecFrame{Start: &ExecStart{Args: []string{"/bin/true", "a"}}}); err != nil {
		t.Fatal(err)
	}
	if err := exec.Send(&ExecFrame{Stdin: []byte("hi")}); err != nil {
		t.Fatal(err)
	}
	if err := exec.Send(&ExecFrame{StdinEOF: true}); err != nil {
		t.Fatal(err)
	}
	var output string
	var exit *ExecExit
	for exit == nil {
		frame, err := exec.Recv()
		if err != nil {
			t.Fatal(err)
		}
		output += string(frame.Stdout)
		exit = frame.Exit
	}
	if output != "hi" || exit.Code != 2 {
		t.Fatalf("exec output %q exit %+v", output, exit)
	}

	archive, err := client.GetArchive(ctx, &ArchiveRequest{Sandbox: "s", Path: "/tmp/f"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := archive.Recv()
	if err != nil || first.Stat == nil || first.Stat.Name != "f" {
		t.Fatalf("first archive chunk = %+v, %v", first, err)
	}
	if chunk, err := archive.Recv(); err != nil || string(chunk.Data) != "tar" {
		t.Fatalf("data chunk = %+v, %v", chunk, err)
	}
	if _, err := archive.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("archive end = %v", err)
	}
}
