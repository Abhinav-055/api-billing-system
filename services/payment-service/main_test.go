package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apibilling/platform/pkg/events"
	walletpb "github.com/apibilling/platform/proto/wallet"
	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type paymentWalletMock struct {
	walletpb.UnimplementedWalletServiceServer
	mu                sync.Mutex
	failNext          bool
	calls             int
	successfulCredits int
	idempotencyKeys   map[string]bool
}

func (w *paymentWalletMock) CreditBalance(_ context.Context, req *walletpb.CreditRequest) (*walletpb.CreditResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	if w.failNext {
		w.failNext = false
		return nil, status.Error(codes.Unavailable, "injected wallet outage")
	}
	if !w.idempotencyKeys[req.GetIdempotencyKey()] {
		w.idempotencyKeys[req.GetIdempotencyKey()] = true
		w.successfulCredits++
	}
	return &walletpb.CreditResponse{Success: true, NewBalance: int64(w.successfulCredits)}, nil
}

func (w *paymentWalletMock) stats() (int, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls, w.successfulCredits
}

type paymentPublisherMock struct {
	mu    sync.Mutex
	calls int
}

func (p *paymentPublisherMock) PublishPaymentSuccess(context.Context, events.PaymentSuccessEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return nil
}

func (p *paymentPublisherMock) stats() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func paymentFixture(t *testing.T, failNext bool) (*service, *paymentWalletMock, *paymentPublisherMock, func(orderDoc) string) {
	t.Helper()
	if os.Getenv("PAYMENT_COUCHBASE_INTEGRATION") != "1" {
		t.Skip("set PAYMENT_COUCHBASE_INTEGRATION=1 to run payment integration tests")
	}
	host := os.Getenv("COUCHBASE_HOST")
	username := os.Getenv("COUCHBASE_USERNAME")
	password := os.Getenv("COUCHBASE_PASSWORD")
	bucketName := os.Getenv("COUCHBASE_BUCKET")
	if host == "" || username == "" || password == "" || bucketName == "" {
		t.Fatal("Couchbase integration environment is incomplete")
	}
	cluster, err := gocb.Connect(host, gocb.ClusterOptions{Authenticator: gocb.PasswordAuthenticator{Username: username, Password: password}})
	if err != nil {
		t.Fatal(err)
	}
	bucket := cluster.Bucket(bucketName)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := bucket.WaitUntilReady(30*time.Second, &gocb.WaitUntilReadyOptions{Context: ctx}); err != nil {
		cluster.Close(nil)
		t.Fatal(err)
	}
	collection := bucket.DefaultCollection()
	walletService := &paymentWalletMock{failNext: failNext, idempotencyKeys: make(map[string]bool)}
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	walletpb.RegisterWalletServiceServer(grpcServer, walletService)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cluster.Close(nil)
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	publisher := &paymentPublisherMock{}
	logBuffer := &bytes.Buffer{}
	svc := &service{
		logger:       slog.New(slog.NewJSONHandler(logBuffer, nil)),
		collection:   collection,
		cluster:      cluster,
		publisher:    publisher,
		walletClient: walletpb.NewWalletServiceClient(conn),
		cfg: config{
			ProcessingTimeout:     time.Second,
			RazorpayKeySecret:     "confirm-secret",
			RazorpayWebhookSecret: "webhook-secret",
		},
	}
	orderIDs := make([]string, 0)
	t.Cleanup(func() {
		for _, orderID := range orderIDs {
			_, _ = collection.Remove("order:::"+orderID, nil)
		}
		cluster.Close(nil)
	})
	seedOrder := func(order orderDoc) string {
		if order.OrderID == "" {
			order.OrderID = "test-order-" + uuid.NewString()
		}
		if order.Currency == "" {
			order.Currency = "INR"
		}
		if order.CreatedAt.IsZero() {
			order.CreatedAt = time.Now().UTC()
		}
		if _, err := collection.Insert("order:::"+order.OrderID, order, nil); err != nil {
			t.Fatal(err)
		}
		orderIDs = append(orderIDs, order.OrderID)
		return order.OrderID
	}
	return svc, walletService, publisher, seedOrder
}

func confirmRequest(svc *service, orderID, paymentID string) *httptest.ResponseRecorder {
	mac := hmac.New(sha256.New, []byte(svc.cfg.RazorpayKeySecret))
	_, _ = mac.Write([]byte(orderID + "|" + paymentID))
	body, _ := json.Marshal(map[string]string{
		"razorpay_order_id": orderID, "razorpay_payment_id": paymentID, "razorpay_signature": hex.EncodeToString(mac.Sum(nil)),
	})
	request := httptest.NewRequest(http.MethodPost, "/payments/confirm", bytes.NewReader(body))
	response := httptest.NewRecorder()
	svc.handleConfirmPayment(response, request)
	return response
}

func webhookBody(orderID, paymentID string, amount int64, currency string) []byte {
	return []byte(fmt.Sprintf(`{"event":"payment.captured","payload":{"payment":{"entity":{"id":%q,"order_id":%q,"amount":%d,"currency":%q}}}}`, paymentID, orderID, amount, currency))
}

func signWebhook(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func postWebhook(svc *service, body []byte, signature string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/payments/webhook", bytes.NewReader(body))
	if signature != "" {
		request.Header.Set("X-Razorpay-Signature", signature)
	}
	response := httptest.NewRecorder()
	svc.handleWebhook(response, request)
	return response
}

func TestVerifyRazorpaySignature(t *testing.T) {
	secret := "webhook-secret"
	orderID := "order_123"
	paymentID := "pay_456"

	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(orderID + "|" + paymentID))
	signature := hex.EncodeToString(mac.Sum(nil))

	if !verifyRazorpaySignature(orderID, paymentID, signature, secret) {
		t.Fatal("expected signature verification to pass")
	}

	if verifyRazorpaySignature(orderID, paymentID, "bad-signature", secret) {
		t.Fatal("expected invalid signature to fail")
	}

	if verifyRazorpaySignature(orderID, paymentID, signature, "") {
		t.Fatal("expected empty secret to fail")
	}
}

func TestVerifyWebhookSignature(t *testing.T) {
	raw := []byte(`{"event":"payment.captured","payload":{"payment":{"entity":{"amount":10000}}}}`)
	secret := "webhook-secret"
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(raw)
	valid := hex.EncodeToString(mac.Sum(nil))
	if !verifyWebhookSignature(raw, valid, secret) {
		t.Fatal("valid raw-body signature should pass")
	}
	modified := append(append([]byte{}, raw...), 'x')
	if verifyWebhookSignature(modified, valid, secret) {
		t.Fatal("modified body signature should fail")
	}
	if verifyWebhookSignature(raw, "", secret) {
		t.Fatal("missing signature should fail")
	}
}

func TestParseInt64EnvBounds(t *testing.T) {
	t.Setenv("MIN_ORDER_AMOUNT_MINOR", "250")
	if got := parseInt64Env("MIN_ORDER_AMOUNT_MINOR", 100); got != 250 {
		t.Fatalf("got %d, want 250", got)
	}
	t.Setenv("MIN_ORDER_AMOUNT_MINOR", "bad")
	if got := parseInt64Env("MIN_ORDER_AMOUNT_MINOR", 100); got != 100 {
		t.Fatalf("invalid env got %d, want fallback", got)
	}
}

func TestCreditsFromPaiseRequiresWholeRupeesAndAvoidsOverflow(t *testing.T) {
	for _, amount := range []int64{0, -100, 10050} {
		if _, err := creditsFromPaise(amount); err == nil {
			t.Errorf("creditsFromPaise(%d) should fail", amount)
		}
	}
	maxWholeRupees := int64(math.MaxInt64 / 100 * 100)
	credits, err := creditsFromPaise(maxWholeRupees)
	if err != nil {
		t.Fatal(err)
	}
	if credits != math.MaxInt64/100 || credits > math.MaxInt64 {
		t.Fatalf("credits=%d, want safe maximum %d", credits, math.MaxInt64/100)
	}
}

func TestCreateOrderRejectsInvalidAmounts(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "fractional rupees", body: `{"amount":100.50}`},
		{name: "zero", body: `{"amount":0}`},
		{name: "negative", body: `{"amount":-1}`},
		{name: "over maximum", body: `{"amount":10001}`},
		{name: "integer overflow", body: `{"amount":9223372036854775808}`},
		{name: "non-whole paise", body: `{"amount_minor":10050}`},
		{name: "minor-unit overflow", body: `{"amount_minor":9223372036854775808}`},
	}
	svc := &service{cfg: config{MinAmountMinor: 100, MaxAmountMinor: 1_000_000}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/payments/create-order", strings.NewReader(tc.body))
			response := httptest.NewRecorder()
			svc.handleCreateOrder(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s, want 400", response.Code, response.Body.String())
			}
		})
	}
}

func TestWebhookRejectsNonWholeRupeeAmountBeforeCredit(t *testing.T) {
	secret := "test-webhook-secret"
	body := []byte(`{"event":"payment.captured","payload":{"payment":{"entity":{"id":"pay-1","order_id":"order-1","amount":10050,"currency":"INR"}}}}`)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	logBuffer := &bytes.Buffer{}
	svc := &service{
		logger: slog.New(slog.NewJSONHandler(logBuffer, nil)),
		cfg:    config{RazorpayWebhookSecret: secret},
	}
	request := httptest.NewRequest(http.MethodPost, "/payments/webhook", bytes.NewReader(body))
	request.Header.Set("X-Razorpay-Signature", fmt.Sprintf("%x", mac.Sum(nil)))
	response := httptest.NewRecorder()
	svc.handleWebhook(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s, want 400", response.Code, response.Body.String())
	}
	if !strings.Contains(logBuffer.String(), "PAYMENT_AMOUNT_VALIDATION") {
		t.Fatalf("missing amount validation security/data log: %s", logBuffer.String())
	}
}

func TestRupeesJSONToPaiseUsesExactIntegerParsing(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want int64
		ok   bool
	}{
		{raw: "100", want: 10000, ok: true},
		{raw: "100.50", ok: false},
		{raw: "92233720368547758", want: math.MaxInt64 / 100 * 100, ok: true},
		{raw: "92233720368547759", ok: false},
		{raw: "1.001", ok: false},
	} {
		got, err := rupeesJSONToPaise(json.RawMessage(tc.raw))
		if (err == nil) != tc.ok || (tc.ok && got != tc.want) {
			t.Errorf("rupeesJSONToPaise(%s)=(%d,%v), want (%d, ok=%v)", tc.raw, got, err, tc.want, tc.ok)
		}
	}
}

func TestConfirmWalletFailureRetryAndCompletedReplayIntegration(t *testing.T) {
	svc, wallet, publisher, seedOrder := paymentFixture(t, true)
	orderID := seedOrder(orderDoc{UserID: "user-1", AmountMinor: 10000, Currency: "INR", Status: "pending"})
	paymentID := "pay-" + uuid.NewString()

	first := confirmRequest(svc, orderID, paymentID)
	if first.Code != http.StatusBadGateway {
		t.Fatalf("first confirm status=%d body=%s, want 502", first.Code, first.Body.String())
	}
	stored, err := svc.collection.Get("order:::"+orderID, nil)
	if err != nil {
		t.Fatal(err)
	}
	var order orderDoc
	if err := stored.Content(&order); err != nil || order.Status != "processing" {
		t.Fatalf("failed wallet credit did not leave order processing: order=%+v err=%v", order, err)
	}

	second := confirmRequest(svc, orderID, paymentID)
	if second.Code != http.StatusOK {
		t.Fatalf("retry confirm status=%d body=%s, want 200", second.Code, second.Body.String())
	}
	replay := confirmRequest(svc, orderID, paymentID)
	if replay.Code != http.StatusOK {
		t.Fatalf("completed replay status=%d body=%s, want 200", replay.Code, replay.Body.String())
	}
	calls, credits := wallet.stats()
	if calls != 2 || credits != 1 || publisher.stats() != 1 {
		t.Fatalf("wallet calls=%d credits=%d payment events=%d, want 2, 1, 1", calls, credits, publisher.stats())
	}
	stored, err = svc.collection.Get("order:::"+orderID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := stored.Content(&order); err != nil || order.Status != "completed" {
		t.Fatalf("order did not complete after retry: order=%+v err=%v", order, err)
	}
}

func TestConcurrentConfirmAndWebhookCreditOnceIntegration(t *testing.T) {
	svc, wallet, _, seedOrder := paymentFixture(t, false)
	orderID := seedOrder(orderDoc{UserID: "user-1", AmountMinor: 10000, Currency: "INR", Status: "pending"})
	paymentID := "pay-" + uuid.NewString()
	body := webhookBody(orderID, paymentID, 10000, "INR")
	signature := signWebhook(svc.cfg.RazorpayWebhookSecret, body)
	start := make(chan struct{})
	responses := make(chan int, 2)
	go func() {
		<-start
		responses <- confirmRequest(svc, orderID, paymentID).Code
	}()
	go func() {
		<-start
		responses <- postWebhook(svc, body, signature).Code
	}()
	close(start)
	first, second := <-responses, <-responses
	if first >= 500 || second >= 500 {
		t.Fatalf("confirm/webhook statuses=%d,%d", first, second)
	}
	_, credits := wallet.stats()
	if credits != 1 {
		t.Fatalf("successful wallet credits=%d, want exactly one", credits)
	}
	stored, err := svc.collection.Get("order:::"+orderID, nil)
	if err != nil {
		t.Fatal(err)
	}
	var order orderDoc
	if err := stored.Content(&order); err != nil || order.Status != "completed" {
		t.Fatalf("concurrent payment did not complete order: order=%+v err=%v", order, err)
	}
}

func TestFreshProcessingConflictAndStaleProcessingRecoveryIntegration(t *testing.T) {
	svc, wallet, _, seedOrder := paymentFixture(t, false)
	freshID := seedOrder(orderDoc{
		UserID: "user-1", AmountMinor: 10000, Currency: "INR", Status: "processing",
		PaymentID: "pay-existing", ProcessingStartedAt: time.Now().UTC(),
	})
	conflict := confirmRequest(svc, freshID, "pay-other")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("fresh processing order status=%d body=%s, want retryable 409", conflict.Code, conflict.Body.String())
	}

	staleID := seedOrder(orderDoc{
		UserID: "user-1", AmountMinor: 10000, Currency: "INR", Status: "processing",
		PaymentID: "pay-abandoned", ProcessingStartedAt: time.Now().UTC().Add(-time.Hour),
	})
	recoveredPaymentID := "pay-recovered"
	recovered := confirmRequest(svc, staleID, recoveredPaymentID)
	if recovered.Code != http.StatusOK {
		t.Fatalf("stale processing recovery status=%d body=%s, want 200", recovered.Code, recovered.Body.String())
	}
	stored, err := svc.collection.Get("order:::"+staleID, nil)
	if err != nil {
		t.Fatal(err)
	}
	var order orderDoc
	if err := stored.Content(&order); err != nil || order.Status != "completed" || order.PaymentID != recoveredPaymentID {
		t.Fatalf("stale order recovery state=%+v err=%v", order, err)
	}
	_, credits := wallet.stats()
	if credits != 1 {
		t.Fatalf("successful wallet credits=%d, want 1", credits)
	}
}

func TestWebhookRawSignatureAndOrderValidationIntegration(t *testing.T) {
	svc, wallet, _, seedOrder := paymentFixture(t, false)
	orderID := seedOrder(orderDoc{UserID: "user-1", AmountMinor: 10000, Currency: "INR", Status: "pending"})
	paymentID := "pay-" + uuid.NewString()
	body := webhookBody(orderID, paymentID, 10000, "INR")
	signature := signWebhook(svc.cfg.RazorpayWebhookSecret, body)

	modified := append(append([]byte(nil), body...), ' ')
	for _, tc := range []struct {
		name      string
		body      []byte
		signature string
	}{
		{name: "modified body", body: modified, signature: signature},
		{name: "wrong secret", body: body, signature: signWebhook("wrong-secret", body)},
		{name: "missing header", body: body},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := postWebhook(svc, tc.body, tc.signature)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("status=%d body=%s, want 401", response.Code, response.Body.String())
			}
		})
	}
	valid := postWebhook(svc, body, signature)
	if valid.Code != http.StatusOK {
		t.Fatalf("valid raw-body signature status=%d body=%s, want 200", valid.Code, valid.Body.String())
	}
	_, credits := wallet.stats()
	if credits != 1 {
		t.Fatalf("valid webhook credits=%d, want 1", credits)
	}
}

func TestWebhookAmountAndCurrencyMismatchDoNotCreditIntegration(t *testing.T) {
	svc, wallet, _, seedOrder := paymentFixture(t, false)
	amountOrder := seedOrder(orderDoc{UserID: "user-1", AmountMinor: 10000, Currency: "INR", Status: "pending"})
	currencyOrder := seedOrder(orderDoc{UserID: "user-2", AmountMinor: 10000, Currency: "INR", Status: "pending"})
	for _, tc := range []struct {
		name     string
		order    string
		amount   int64
		currency string
	}{
		{name: "amount mismatch", order: amountOrder, amount: 20000, currency: "INR"},
		{name: "currency mismatch", order: currencyOrder, amount: 10000, currency: "USD"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := webhookBody(tc.order, "pay-"+uuid.NewString(), tc.amount, tc.currency)
			response := postWebhook(svc, body, signWebhook(svc.cfg.RazorpayWebhookSecret, body))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s, want 400", response.Code, response.Body.String())
			}
		})
	}
	calls, credits := wallet.stats()
	if calls != 0 || credits != 0 {
		t.Fatalf("mismatch webhook wallet calls=%d credits=%d, want 0", calls, credits)
	}
}
