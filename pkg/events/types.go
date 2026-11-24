package events

import "time"

const (
	SubjectUsageLogged    = "usage.logged"
	SubjectBillingProcess = "billing.process"
	SubjectWalletDeduct   = "wallet.deduct"
	SubjectWalletCapture  = "wallet.capture"
	SubjectWalletDLQ      = "wallet.dlq"
	SubjectPaymentSuccess = "payment.success"
)

type UsageEvent struct {
	UserID        string    `json:"user_id"`
	Endpoint      string    `json:"endpoint"`
	Method        string    `json:"method"`
	CostMinor     int64     `json:"cost_minor"`
	Currency      string    `json:"currency"`
	StatusCode    int       `json:"status_code"`
	LatencyMs     int64     `json:"latency_ms"`
	RequestID     string    `json:"request_id"`
	ReservationID string    `json:"reservation_id"`
	Timestamp     time.Time `json:"timestamp"`
}

type WalletDeductEvent struct {
	UserID         string    `json:"user_id"`
	AmountMinor    int64     `json:"amount_minor"`
	Currency       string    `json:"currency"`
	IdempotencyKey string    `json:"idempotency_key"`
	RequestID      string    `json:"request_id"`
	Timestamp      time.Time `json:"timestamp"`
}

type PaymentSuccessEvent struct {
	UserID            string    `json:"user_id"`
	AmountMinor       int64     `json:"amount_minor"`
	Currency          string    `json:"currency"`
	RazorpayOrderID   string    `json:"razorpay_order_id"`
	RazorpayPaymentID string    `json:"razorpay_payment_id"`
	Timestamp         time.Time `json:"timestamp"`
}
