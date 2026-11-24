package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
