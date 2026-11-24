package main

import (
	"context"
	"errors"
	"testing"

	"github.com/apibilling/platform/pkg/events"
)

func TestLegacyFloatToMinorUnits(t *testing.T) {
	if got := legacyFloatToMinorUnits(12.4); got != 12 {
		t.Fatalf("12.4 converted to %d, want 12", got)
	}
	if got := legacyFloatToMinorUnits(12.6); got != 13 {
		t.Fatalf("12.6 converted to %d, want 13", got)
	}
}

func TestTransientErrorClassification(t *testing.T) {
	if !isTransientError(errorString("Couchbase timeout")) {
		t.Fatal("timeout should be transient")
	}
	if isTransientError(errorString("INSUFFICIENT_FUNDS")) {
		t.Fatal("business error should not be transient")
	}
}

type errorString string

func (e errorString) Error() string { return string(e) }

func TestWalletBusinessFailureIsAuditedAndTermed(t *testing.T) {
	payload := []byte(`{"user_id":"user-1","amount_minor":3,"idempotency_key":"usage:req-1"}`)
	processCalls, auditCalls := 0, 0
	process := func(context.Context, string, []byte) error {
		processCalls++
		return walletBusinessError{reason: "INSUFFICIENT_FUNDS"}
	}
	audit := func(_ context.Context, subject string, got []byte, delivery uint64, reason error) error {
		auditCalls++
		if subject != events.SubjectWalletDeduct || string(got) != string(payload) || delivery != 1 || reason.Error() != "INSUFFICIENT_FUNDS" {
			t.Fatalf("unexpected audit data: subject=%q payload=%s delivery=%d reason=%v", subject, got, delivery, reason)
		}
		return nil
	}
	disposition := processWalletDelivery(context.Background(), events.SubjectWalletDeduct, payload, 1, process, audit, func(context.Context, []byte) error {
		t.Fatal("business error must not go to DLQ")
		return nil
	})
	if disposition != deliveryTerm || processCalls != 1 || auditCalls != 1 {
		t.Fatalf("disposition=%v process=%d audit=%d", disposition, processCalls, auditCalls)
	}
}

func TestWalletTransientFailureNAKs(t *testing.T) {
	disposition := processWalletDelivery(context.Background(), events.SubjectWalletCapture, []byte(`{"reservation_id":"r1"}`), 2,
		func(context.Context, string, []byte) error { return errorString("Couchbase timeout") },
		func(context.Context, string, []byte, uint64, error) error {
			t.Fatal("transient error should not be audited")
			return nil
		},
		func(context.Context, []byte) error { t.Fatal("non-final delivery should not publish DLQ"); return nil },
	)
	if disposition != deliveryNak {
		t.Fatalf("disposition=%v, want NAK", disposition)
	}
}

func TestWalletFifthDeliveryPublishesDLQ(t *testing.T) {
	payload := []byte(`{"reservation_id":"r1"}`)
	published := 0
	disposition := processWalletDelivery(context.Background(), events.SubjectWalletCapture, payload, 5,
		func(context.Context, string, []byte) error { return errors.New("network timeout") },
		func(context.Context, string, []byte, uint64, error) error {
			t.Fatal("transient final delivery should go to DLQ")
			return nil
		},
		func(_ context.Context, got []byte) error {
			published++
			if string(got) != string(payload) {
				t.Fatalf("DLQ payload=%s", got)
			}
			return nil
		},
	)
	if disposition != deliveryTerm || published != 1 {
		t.Fatalf("disposition=%v published=%d", disposition, published)
	}
}

func TestWalletFailedDLQPublishRetainsOriginal(t *testing.T) {
	disposition := processWalletDelivery(context.Background(), events.SubjectWalletDeduct, []byte(`{"user_id":"u"}`), 5,
		func(context.Context, string, []byte) error { return errorString("timeout") },
		func(context.Context, string, []byte, uint64, error) error { return nil },
		func(context.Context, []byte) error { return errors.New("NATS unavailable") },
	)
	if disposition != deliveryNak {
		t.Fatalf("disposition=%v, want NAK without ACK/Term", disposition)
	}
}

func TestWalletMalformedPayloadAuditedAndTermed(t *testing.T) {
	auditCalls := 0
	disposition := processWalletDelivery(context.Background(), events.SubjectWalletDeduct, []byte(`not json`), 1,
		func(context.Context, string, []byte) error {
			t.Fatal("malformed payload must not reach processor")
			return nil
		},
		func(_ context.Context, _ string, got []byte, delivery uint64, reason error) error {
			auditCalls++
			if string(got) != "not json" || delivery != 1 || reason.Error() != "MALFORMED_PAYLOAD" {
				t.Fatalf("bad malformed audit payload=%q delivery=%d reason=%v", got, delivery, reason)
			}
			return nil
		},
		func(context.Context, []byte) error {
			t.Fatal("malformed payload is not a transient DLQ case")
			return nil
		},
	)
	if disposition != deliveryTerm || auditCalls != 1 {
		t.Fatalf("disposition=%v audit=%d", disposition, auditCalls)
	}
}
