package grpcauth

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type authTestService interface {
	Check(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
}

type authTestServer struct {
	calls int
}

func (s *authTestServer) Check(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	s.calls++
	return &emptypb.Empty{}, nil
}

func authTestCheckHandler(srv any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	request := new(emptypb.Empty)
	if err := decode(request); err != nil {
		return nil, err
	}
	handler := func(ctx context.Context, request any) (any, error) {
		return srv.(authTestService).Check(ctx, request.(*emptypb.Empty))
	}
	if interceptor == nil {
		return handler(ctx, request)
	}
	return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: srv, FullMethod: "/auth.Test/Check"}, handler)
}

func TestExternalGRPCCallsRequireInternalToken(t *testing.T) {
	service := &authTestServer{}
	listener := bufconnListen()
	server := grpc.NewServer(grpc.UnaryInterceptor(UnaryServerInterceptor("trusted-token")))
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "auth.Test",
		HandlerType: (*authTestService)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Check",
			Handler:    authTestCheckHandler,
		}},
	}, service)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	for _, tc := range []struct {
		name  string
		token string
		want  codes.Code
	}{
		{name: "missing token", want: codes.Unauthenticated},
		{name: "wrong token", token: "untrusted-token", want: codes.Unauthenticated},
		{name: "trusted token", token: "trusted-token", want: codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := []grpc.DialOption{
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			}
			if tc.token != "" {
				options = append(options, grpc.WithUnaryInterceptor(UnaryClientInterceptor(tc.token)))
			}
			conn, err := grpc.DialContext(context.Background(), "bufnet", options...)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			err = conn.Invoke(context.Background(), "/auth.Test/Check", &emptypb.Empty{}, &emptypb.Empty{})
			if status.Code(err) != tc.want {
				t.Fatalf("gRPC status=%s err=%v, want %s", status.Code(err), err, tc.want)
			}
		})
	}
	if service.calls != 1 {
		t.Fatalf("authorized handler calls=%d, want 1", service.calls)
	}
}

func bufconnListen() *bufconn.Listener {
	return bufconn.Listen(1024 * 1024)
}
