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

type reservationTestWalletService struct {
	walletpb.UnimplementedWalletServiceServer
	releaseCalls int
}

func (contentionWalletService) ReserveFunds(context.Context, *walletpb.ReserveFundsRequest) (*walletpb.ReserveFundsResponse, error) {
	return nil, status.Error(codes.ResourceExhausted, "WALLET_CONTENTION_EXHAUSTED")
}

func (reservationTestWalletService) ReserveFunds(_ context.Context, req *walletpb.ReserveFundsRequest) (*walletpb.ReserveFundsResponse, error) {
	if req.GetAmountMinor() == 3 {
		return &walletpb.ReserveFundsResponse{Reserved: false, Reason: "INSUFFICIENT_FUNDS"}, nil
	}
	return &walletpb.ReserveFundsResponse{Reserved: true}, nil
}

func (s *reservationTestWalletService) ReleaseReservation(context.Context, *walletpb.ReleaseReservationRequest) (*walletpb.ReleaseReservationResponse, error) {
	s.releaseCalls++
	return &walletpb.ReleaseReservationResponse{Released: true}, nil
}

func testWalletClient(t *testing.T, service walletpb.WalletServiceServer) walletpb.WalletServiceClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	walletpb.RegisterWalletServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return walletpb.NewWalletServiceClient(conn)
}

func reservationRequest(path string) *http.Request {
	ctx := context.WithValue(context.Background(), ctxUserID, "user-1")
	ctx = context.WithValue(ctx, ctxRequestID, "request-1")
	return httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
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
	application := &app{walletClient: testWalletClient(t, contentionWalletService{})}
	request := reservationRequest("/api/v1/data")
	response := httptest.NewRecorder()
	application.reservationMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not run when reservation contention is exhausted")
	})).ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") != "1" {
		t.Fatalf("status=%d Retry-After=%q", response.Code, response.Header().Get("Retry-After"))
	}
}

func TestInsufficientComputeDoesNotRunHandlerButDataDoes(t *testing.T) {
	application := &app{walletClient: testWalletClient(t, &reservationTestWalletService{})}
	handlerCalls := 0
	middleware := application.reservationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handlerCalls++
		w.WriteHeader(http.StatusOK)
	}))

	compute := httptest.NewRecorder()
	middleware.ServeHTTP(compute, reservationRequest("/api/v1/compute"))
	if compute.Code != http.StatusPaymentRequired || handlerCalls != 0 {
		t.Fatalf("compute status=%d handlerCalls=%d, want 402 and 0", compute.Code, handlerCalls)
	}
	data := httptest.NewRecorder()
	middleware.ServeHTTP(data, reservationRequest("/api/v1/data"))
	if data.Code != http.StatusOK || handlerCalls != 1 {
		t.Fatalf("data status=%d handlerCalls=%d, want 200 and 1", data.Code, handlerCalls)
	}
}

func TestServerErrorResponseReleasesReservation(t *testing.T) {
	wallet := &reservationTestWalletService{}
	application := &app{walletClient: testWalletClient(t, wallet)}
	handler := application.reservationMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, reservationRequest("/api/v1/data"))
	if response.Code != http.StatusInternalServerError || wallet.releaseCalls != 1 {
		t.Fatalf("status=%d releaseCalls=%d, want 500 and 1", response.Code, wallet.releaseCalls)
	}
}
