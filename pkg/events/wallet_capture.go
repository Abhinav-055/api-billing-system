package events

import "time"

type WalletCaptureEvent struct {
	UserID         string    `json:"user_id"`
	ReservationID  string    `json:"reservation_id"`
	ActualAmount   int64     `json:"actual_amount"`
	Currency       string    `json:"currency"`
	IdempotencyKey string    `json:"idempotency_key"`
	Timestamp      time.Time `json:"timestamp"`
}
