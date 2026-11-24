package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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
