package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	walletpb "github.com/apibilling/platform/proto/wallet"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type contentionWalletService struct {
	walletpb.UnimplementedWalletServiceServer
}

func (contentionWalletService) ReserveFunds(context.Context, *walletpb.ReserveFundsRequest) (*walletpb.ReserveFundsResponse, error) {
	return nil, status.Error(codes.ResourceExhausted, "WALLET_CONTENTION_EXHAUSTED")
}

func TestParsePositiveInt(t *testing.T) {
	if got := parsePositiveInt("10", 1); got != 10 {
		t.Fatalf("parsePositiveInt valid failed: got %d", got)
	}
	if got := parsePositiveInt("0", 7); got != 7 {
		t.Fatalf("parsePositiveInt zero fallback failed: got %d", got)
	}
	if got := parsePositiveInt("", 3); got != 3 {
		t.Fatalf("parsePositiveInt empty fallback failed: got %d", got)
	}
}

func TestHashAPIKeyDeterministic(t *testing.T) {
	h1 := hashAPIKey("salt", "key")
	h2 := hashAPIKey("salt", "key")
	if h1 != h2 {
		t.Fatal("hashAPIKey should be deterministic")
	}
	h3 := hashAPIKey("salt2", "key")
	if h1 == h3 {
		t.Fatal("hashAPIKey should change with salt")
	}
}

func TestRouteCost(t *testing.T) {
	if got := routeCost("/api/v1/data"); got != 1 {
		t.Fatalf("data cost = %d, want 1", got)
	}
	if got := routeCost("/api/v1/compute"); got != 3 {
		t.Fatalf("compute cost = %d, want 3", got)
	}
}

func TestReservationContentionReturnsRetryable503(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	walletpb.RegisterWalletServiceServer(server, contentionWalletService{})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	application := &app{walletClient: walletpb.NewWalletServiceClient(conn)}
	ctx := context.WithValue(context.Background(), ctxUserID, "user-1")
	ctx = context.WithValue(ctx, ctxRequestID, "request-1")
	request := httptest.NewRequest(http.MethodGet, "/api/v1/data", nil).WithContext(ctx)
	response := httptest.NewRecorder()
	application.reservationMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not run when reservation contention is exhausted")
	})).ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") != "1" {
		t.Fatalf("status=%d Retry-After=%q", response.Code, response.Header().Get("Retry-After"))
	}
}
