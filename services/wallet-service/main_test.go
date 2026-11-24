package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/apibilling/platform/pkg/events"
	"github.com/couchbase/gocb/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWalletMutationRetriesLockContention(t *testing.T) {
	svc := &server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	attempts := 0
	err := svc.retryWalletOperation(context.Background(), "test", func(context.Context) error {
		attempts++
		if attempts < 3 {
			return gocb.ErrDocumentLocked
		}
		return nil
	})
	if err != nil || attempts != 3 {
		t.Fatalf("err=%v attempts=%d, want success on third attempt", err, attempts)
	}
}

func TestWalletMutationDoesNotRetryOutage(t *testing.T) {
	svc := &server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	attempts := 0
	wantErr := errors.New("connection refused")
	err := svc.retryWalletOperation(context.Background(), "test", func(context.Context) error {
		attempts++
		return wantErr
	})
	if !errors.Is(err, wantErr) || attempts != 1 {
		t.Fatalf("err=%v attempts=%d, want original error after one attempt", err, attempts)
	}
}

func TestWalletMutationContentionExhaustionIsRetryable(t *testing.T) {
	svc := &server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	attempts := 0
	err := svc.retryWalletOperation(context.Background(), "test", func(context.Context) error {
		attempts++
		return gocb.ErrDocumentLocked
	})
	if status.Code(err) != codes.ResourceExhausted || attempts != walletMutationAttempts {
		t.Fatalf("err=%v code=%s attempts=%d", err, status.Code(err), attempts)
	}
}

func TestLegacyFloatToMinorUnits(t *testing.T) {
	if got := legacyFloatToMinorUnits(12.4); got != 12 {
		t.Fatalf("12.4 converted to %d, want 12", got)
	}
	if got := legacyFloatToMinorUnits(12.6); got != 13 {
		t.Fatalf("12.6 converted to %d, want 13", got)
	}
}

func TestExpiredHoldReleaseRestoresBalances(t *testing.T) {
	now := time.Now().UTC()
	wallet := walletDoc{UserID: "u", Available: 2, Held: 3}
	hold := holdDoc{ReservationID: "r", UserID: "u", Amount: 3, Status: "HELD", ExpiresAt: now.Add(-time.Second)}
	updated, released, err := releaseHoldState(wallet, hold, now)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Available != 5 || updated.Held != 0 || released.Status != "RELEASED" {
		t.Fatalf("wallet=%+v hold=%+v", updated, released)
	}
}

func TestSweeperDoesNotChangeClosedHolds(t *testing.T) {
	now := time.Now().UTC()
	for _, status := range []string{"CAPTURED", "RELEASED"} {
		t.Run(status, func(t *testing.T) {
			wallet := walletDoc{UserID: "u", Available: 7, Held: 0}
			hold := holdDoc{ReservationID: "r", UserID: "u", Amount: 3, Status: status, ExpiresAt: now.Add(-time.Hour)}
			updated, _, err := releaseHoldState(wallet, hold, now)
			if err == nil {
				t.Fatal("closed hold must not be released")
			}
			if updated.Available != wallet.Available || updated.Held != wallet.Held {
				t.Fatalf("closed hold moved funds: before=%+v after=%+v", wallet, updated)
			}
		})
	}
}

func TestSweeperRacingCaptureMovesFundsOnce(t *testing.T) {
	now := time.Now().UTC()
	state := walletDoc{UserID: "u", Available: 0, Held: 3, Holds: map[string]holdDoc{}}
	hold := holdDoc{ReservationID: "r", UserID: "u", Amount: 3, Status: "HELD", ExpiresAt: now.Add(time.Hour)}
	state.Holds[hold.ReservationID] = hold
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		mu.Lock()
		defer mu.Unlock()
		current := state.Holds[hold.ReservationID]
		updated, next, err := captureHoldState(state, current, 3, now)
		if err == nil {
			state = updated
			state.Holds[hold.ReservationID] = next
		}
	}()
	go func() {
		defer wg.Done()
		mu.Lock()
		defer mu.Unlock()
		current := state.Holds[hold.ReservationID]
		updated, next, err := releaseHoldState(state, current, now)
		if err == nil {
			state = updated
			state.Holds[hold.ReservationID] = next
		}
	}()
	wg.Wait()
	if state.Held != 0 || (state.Available != 0 && state.Available != 3) {
		t.Fatalf("funds moved more than once: available=%d held=%d", state.Available, state.Held)
	}
	if got := state.Holds[hold.ReservationID].Status; got != "CAPTURED" && got != "RELEASED" {
		t.Fatalf("final hold status=%s", got)
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
