package events

import "time"

const (
	SubjectUsageLogged    = "usage.logged"
	SubjectBillingProcess = "billing.process"
	SubjectWalletDeduct   = "wallet.deduct"
	SubjectPaymentSuccess = "payment.success"
)

type UsageEvent struct {
	UserID     string    `json:"user_id"`
	Endpoint   string    `json:"endpoint"`
	Method     string    `json:"method"`
	StatusCode int       `json:"status_code"`
	LatencyMs  int64     `json:"latency_ms"`
	RequestID  string    `json:"request_id"`
	Timestamp  time.Time `json:"timestamp"`
}

type WalletDeductEvent struct {
	UserID         string    `json:"user_id"`
	Amount         float64   `json:"amount"`
	IdempotencyKey string    `json:"idempotency_key"`
	RequestID      string    `json:"request_id"`
	Timestamp      time.Time `json:"timestamp"`
}

type PaymentSuccessEvent struct {
	UserID            string    `json:"user_id"`
	Amount            float64   `json:"amount"`
	RazorpayOrderID   string    `json:"razorpay_order_id"`
	RazorpayPaymentID string    `json:"razorpay_payment_id"`
	Timestamp         time.Time `json:"timestamp"`
}
