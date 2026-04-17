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
