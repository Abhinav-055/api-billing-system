package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apibilling/platform/pkg/events"
	"github.com/apibilling/platform/pkg/grpcauth"
	walletpb "github.com/apibilling/platform/proto/wallet"
	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestWalletGRPCRejectsMissingAndWrongInternalTokenIntegration(t *testing.T) {
	if os.Getenv("WALLET_COUCHBASE_INTEGRATION") != "1" {
		t.Skip("set WALLET_COUCHBASE_INTEGRATION=1 to run service integration tests")
	}
	address := os.Getenv("WALLET_GRPC_ADDR")
	if address == "" {
		address = "wallet-service:9090"
	}
	for _, tc := range []struct {
		name  string
		token string
	}{
		{name: "missing token"},
		{name: "wrong token", token: "untrusted-token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
			if tc.token != "" {
				options = append(options, grpc.WithUnaryInterceptor(grpcauth.UnaryClientInterceptor(tc.token)))
			}
			conn, err := grpc.NewClient(address, options...)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err = walletpb.NewWalletServiceClient(conn).CheckBalance(ctx, &walletpb.BalanceRequest{UserId: "external-client"})
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("gRPC status=%s err=%v, want Unauthenticated", status.Code(err), err)
			}
		})
	}
}

func integrationWallet(t *testing.T, available int64) (*server, string, func(string)) {
	t.Helper()
	if os.Getenv("WALLET_COUCHBASE_INTEGRATION") != "1" {
		t.Skip("set WALLET_COUCHBASE_INTEGRATION=1 to run Couchbase integration tests")
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
	userID := "race-test-" + uuid.NewString()
	walletKey := "wallet:::" + userID
	if _, err := collection.Insert(walletKey, walletDoc{UserID: userID, Available: available}, nil); err != nil {
		cluster.Close(nil)
		t.Fatal(err)
	}
	var holdMu sync.Mutex
	holdIDs := make([]string, 0)
	t.Cleanup(func() {
		holdMu.Lock()
		defer holdMu.Unlock()
		for _, reservationID := range holdIDs {
			_, _ = collection.Remove("hold:::"+reservationID, nil)
		}
		_, _ = collection.Remove(walletKey, nil)
		cluster.Close(nil)
	})
	svc := &server{
		cfg:        configType{HoldTTL: 5 * time.Minute},
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		cluster:    cluster,
		collection: collection,
	}
	trackHold := func(reservationID string) {
		holdMu.Lock()
		holdIDs = append(holdIDs, reservationID)
		holdMu.Unlock()
	}
	return svc, userID, trackHold
}

func integrationMutationWallet(t *testing.T, available int64) (*server, string) {
	t.Helper()
	svc, userID, _ := integrationWallet(t, available)
	redisURL := os.Getenv("WALLET_TEST_REDIS_URL")
	if redisURL == "" {
		t.Fatal("WALLET_TEST_REDIS_URL is required for mutation integration tests")
	}
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	svc.redis = redis.NewClient(options)
	if err := svc.redis.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.redis.Close() })
	return svc, userID
}

func TestConcurrentReserveFundsIntegration(t *testing.T) {
	svc, userID, trackHold := integrationWallet(t, 3)
	const requestCount = 20
	start := make(chan struct{})
	results := make(chan struct {
		response *walletpb.ReserveFundsResponse
		err      error
	}, requestCount)
	var wg sync.WaitGroup
	for i := 0; i < requestCount; i++ {
		reservationID := uuid.NewString()
		trackHold(reservationID)
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			response, err := svc.ReserveFunds(ctx, &walletpb.ReserveFundsRequest{
				UserId: userID, AmountMinor: 3, ReservationId: id, Currency: "INR",
			})
			results <- struct {
				response *walletpb.ReserveFundsResponse
				err      error
			}{response: response, err: err}
		}(reservationID)
	}
	close(start)
	wg.Wait()
	close(results)
	successes := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("ReserveFunds returned error under contention: type=%T err=%v", result.err, result.err)
		}
		if result.response.GetReserved() {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful reservations=%d, want 1", successes)
	}
	res, err := svc.collection.Get("wallet:::"+userID, nil)
	if err != nil {
		t.Fatal(err)
	}
	var wallet walletDoc
	if err := res.Content(&wallet); err != nil {
		t.Fatal(err)
	}
	if wallet.Available != 0 || wallet.Held != 3 {
		t.Fatalf("final wallet available=%d held=%d, want 0 and 3", wallet.Available, wallet.Held)
	}
}

func TestReservationCallsAreIdempotentIntegration(t *testing.T) {
	svc, userID, trackHold := integrationWallet(t, 10)
	reservationID := uuid.NewString()
	trackHold(reservationID)
	reserveReq := &walletpb.ReserveFundsRequest{UserId: userID, AmountMinor: 3, ReservationId: reservationID, Currency: "INR"}
	for i := 0; i < 2; i++ {
		response, err := svc.ReserveFunds(context.Background(), reserveReq)
		if err != nil || !response.GetReserved() {
			t.Fatalf("reserve replay %d: response=%v err=%v", i, response, err)
		}
	}
	for i := 0; i < 2; i++ {
		response, err := svc.CaptureReservation(context.Background(), &walletpb.CaptureReservationRequest{ReservationId: reservationID, ActualAmount: 3})
		if err != nil || !response.GetCaptured() {
			t.Fatalf("capture replay %d: response=%v err=%v", i, response, err)
		}
	}
	releaseAfterCapture, err := svc.ReleaseReservation(context.Background(), &walletpb.ReleaseReservationRequest{ReservationId: reservationID})
	if err != nil || releaseAfterCapture.GetReleased() {
		t.Fatalf("release after capture: response=%v err=%v", releaseAfterCapture, err)
	}

	releasedID := uuid.NewString()
	trackHold(releasedID)
	releaseReq := &walletpb.ReleaseReservationRequest{ReservationId: releasedID}
	if response, err := svc.ReserveFunds(context.Background(), &walletpb.ReserveFundsRequest{UserId: userID, AmountMinor: 2, ReservationId: releasedID, Currency: "INR"}); err != nil || !response.GetReserved() {
		t.Fatalf("reserve for release: response=%v err=%v", response, err)
	}
	for i := 0; i < 2; i++ {
		response, err := svc.ReleaseReservation(context.Background(), releaseReq)
		if err != nil || !response.GetReleased() {
			t.Fatalf("release replay %d: response=%v err=%v", i, response, err)
		}
	}
	expiredID := uuid.NewString()
	trackHold(expiredID)
	if response, err := svc.ReserveFunds(context.Background(), &walletpb.ReserveFundsRequest{UserId: userID, AmountMinor: 1, ReservationId: expiredID, TtlSeconds: 1, Currency: "INR"}); err != nil || !response.GetReserved() {
		t.Fatalf("reserve for expiry: response=%v err=%v", response, err)
	}
	time.Sleep(1100 * time.Millisecond)
	expired, err := svc.CaptureReservation(context.Background(), &walletpb.CaptureReservationRequest{ReservationId: expiredID, ActualAmount: 1})
	if err != nil || expired.GetCaptured() || expired.GetReason() != "RESERVATION_EXPIRED" {
		t.Fatalf("capture after expiry: response=%v err=%v", expired, err)
	}
	if response, err := svc.ReleaseReservation(context.Background(), &walletpb.ReleaseReservationRequest{ReservationId: expiredID}); err != nil || !response.GetReleased() {
		t.Fatalf("release expired reservation: response=%v err=%v", response, err)
	}

	res, err := svc.collection.Get("wallet:::"+userID, nil)
	if err != nil {
		t.Fatal(err)
	}
	var wallet walletDoc
	if err := res.Content(&wallet); err != nil {
		t.Fatal(err)
	}
	if wallet.Available != 7 || wallet.Held != 0 || len(wallet.Ledger) != 3 {
		t.Fatalf("final wallet available=%d held=%d ledger=%d, want 7, 0, 3", wallet.Available, wallet.Held, len(wallet.Ledger))
	}
}

func TestSweeperReleasesAbandonedReservationIntegration(t *testing.T) {
	svc, userID, trackHold := integrationWallet(t, 3)
	svc.cfg.CouchbaseBucket = os.Getenv("COUCHBASE_BUCKET")
	svc.cfg.HoldTTL = time.Second
	svc.cfg.SweepInterval = 100 * time.Millisecond
	svc.logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go svc.runSweeper(ctx)

	reservationID := uuid.NewString()
	trackHold(reservationID)
	response, err := svc.ReserveFunds(context.Background(), &walletpb.ReserveFundsRequest{
		UserId: userID, AmountMinor: 3, ReservationId: reservationID, Currency: "INR",
	})
	if err != nil || !response.GetReserved() {
		t.Fatalf("reserve abandoned hold: response=%v err=%v", response, err)
	}
	var hold holdDoc
	for {
		holdResult, err := svc.collection.Get("hold:::"+reservationID, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := holdResult.Content(&hold); err != nil {
			t.Fatal(err)
		}
		if hold.Status == "RELEASED" {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("expired hold did not sweep before deadline: status=%s", hold.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
	walletResult, err := svc.collection.Get("wallet:::"+userID, nil)
	if err != nil {
		t.Fatal(err)
	}
	var wallet walletDoc
	if err := walletResult.Content(&wallet); err != nil {
		t.Fatal(err)
	}
	if hold.Status != "RELEASED" || wallet.Available != 3 || wallet.Held != 0 {
		t.Fatalf("sweeper final hold=%s available=%d held=%d", hold.Status, wallet.Available, wallet.Held)
	}
}

func TestLoadConfigHoldExpiryDurations(t *testing.T) {
	t.Setenv("HOLD_TTL", "2s")
	t.Setenv("SWEEP_INTERVAL", "250ms")
	cfg := loadConfig(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if cfg.HoldTTL != 2*time.Second || cfg.SweepInterval != 250*time.Millisecond {
		t.Fatalf("hold_ttl=%s sweep_interval=%s", cfg.HoldTTL, cfg.SweepInterval)
	}
}

func TestMutationFailureBoundariesRecoverExactlyOnceIntegration(t *testing.T) {
	steps := []string{
		"after_idempotency_insert",
		"after_wallet_replace",
		"before_ledger_write",
		"after_ledger_write",
		"before_cache_write",
		"after_cache_write",
	}
	for _, step := range steps {
		t.Run(step, func(t *testing.T) {
			svc, userID := integrationMutationWallet(t, 10)
			idempotencyKey := "failure-test:" + uuid.NewString()
			fingerprint := "credit|" + userID + "|5|INR"
			failed := false
			svc.mutationFailureHook = func(current string) error {
				if current == step && !failed {
					failed = true
					return errors.New("injected " + step)
				}
				return nil
			}
			response, err := svc.mutation(context.Background(), userID, 5, "credit", idempotencyKey, "test:"+idempotencyKey, "INR", fingerprint)
			if err == nil || !failed {
				t.Fatalf("failure injection %q did not fail: response=%v err=%v", step, response, err)
			}
			svc.mutationFailureHook = nil

			walletResult, err := svc.collection.Get("wallet:::"+userID, nil)
			if err != nil {
				t.Fatal(err)
			}
			var wallet walletDoc
			if err := walletResult.Content(&wallet); err != nil {
				t.Fatal(err)
			}
			changed := step != "after_idempotency_insert"
			if changed && (wallet.Available != 15 || len(wallet.Ledger) != 1 || wallet.Ledger[0].IdempotencyKey != idempotencyKey) {
				t.Fatalf("post-failure wallet lost recoverable mutation: available=%d ledger=%+v", wallet.Available, wallet.Ledger)
			}
			if !changed && (wallet.Available != 10 || len(wallet.Ledger) != 0) {
				t.Fatalf("pre-mutation failure changed wallet: available=%d ledger=%+v", wallet.Available, wallet.Ledger)
			}
			idemResult, err := svc.collection.Get("idempotency:::"+idempotencyKey, nil)
			if err != nil {
				t.Fatalf("recoverable idempotency record missing: %v", err)
			}
			var idem idempotencyDoc
			if err := idemResult.Content(&idem); err != nil || idem.Fingerprint != fingerprint {
				t.Fatalf("invalid idempotency record=%+v err=%v", idem, err)
			}

			replay, err := svc.mutation(context.Background(), userID, 5, "credit", idempotencyKey, "test:"+idempotencyKey, "INR", fingerprint)
			if err != nil || replay == nil || !replay.GetSuccess() || replay.GetNewBalance() != 15 {
				t.Fatalf("retry failed: response=%v err=%v", replay, err)
			}
			walletResult, err = svc.collection.Get("wallet:::"+userID, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := walletResult.Content(&wallet); err != nil {
				t.Fatal(err)
			}
			if wallet.Available != 15 || len(wallet.Ledger) != 1 {
				t.Fatalf("retry duplicated mutation: available=%d ledger=%d", wallet.Available, len(wallet.Ledger))
			}
			var completed idempotencyDoc
			if result, err := svc.collection.Get("idempotency:::"+idempotencyKey, nil); err != nil || result.Content(&completed) != nil || completed.Status != "completed" {
				t.Fatalf("idempotency record not completed: %+v err=%v", completed, err)
			}
			if _, err := svc.collection.Get("transaction:::"+wallet.Ledger[0].TxID, nil); err != nil {
				t.Fatalf("transaction ledger projection missing after retry: %v", err)
			}
		})
	}
}

func TestMutationReplaySurvivesRedisFlushAndElapsedTimeIntegration(t *testing.T) {
	svc, userID := integrationMutationWallet(t, 10)
	idempotencyKey := "durable-replay:" + uuid.NewString()
	fingerprint := "credit|" + userID + "|5|INR"
	first, err := svc.mutation(context.Background(), userID, 5, "credit", idempotencyKey, "test:"+idempotencyKey, "INR", fingerprint)
	if err != nil || first == nil || !first.GetSuccess() || first.GetNewBalance() != 15 {
		t.Fatalf("initial mutation: response=%v err=%v", first, err)
	}
	if err := svc.redis.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	result, err := svc.collection.Get("idempotency:::"+idempotencyKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	var idem idempotencyDoc
	if err := result.Content(&idem); err != nil {
		t.Fatal(err)
	}
	idem.CreatedAt = time.Now().UTC().Add(-25 * time.Hour)
	if _, err := svc.collection.Replace("idempotency:::"+idempotencyKey, idem, &gocb.ReplaceOptions{Cas: result.Cas()}); err != nil {
		t.Fatal(err)
	}
	replayed, err := svc.mutation(context.Background(), userID, 5, "credit", idempotencyKey, "test:"+idempotencyKey, "INR", fingerprint)
	if err != nil || replayed == nil || !replayed.GetSuccess() || replayed.GetNewBalance() != 15 {
		t.Fatalf("durable replay: response=%v err=%v", replayed, err)
	}
	_, err = svc.mutation(context.Background(), userID, 6, "credit", idempotencyKey, "test:"+idempotencyKey, "INR", "different-fingerprint")
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("same key with different fingerprint err=%v, want InvalidArgument", err)
	}
	walletResult, err := svc.collection.Get("wallet:::"+userID, nil)
	if err != nil {
		t.Fatal(err)
	}
	var wallet walletDoc
	if err := walletResult.Content(&wallet); err != nil {
		t.Fatal(err)
	}
	if wallet.Available != 15 || len(wallet.Ledger) != 1 {
		t.Fatalf("replay changed wallet twice: available=%d ledger=%d", wallet.Available, len(wallet.Ledger))
	}
}

func TestWalletMutationRetriesLockContention(t *testing.T) {
	svc := &server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	attempts := 0
	err := svc.retryWalletOperation(context.Background(), "test", func(context.Context) error {
		attempts++
		if attempts < 3 {
			return errorString(`ambiguous timeout; "retry_reasons":["KV_LOCKED"]`)
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

func TestParseLegacyWalletAmountRequiresExactInt64(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int64
	}{
		{value: "0", want: 0},
		{value: "12", want: 12},
		{value: "9223372036854775807", want: 9223372036854775807},
	} {
		got, err := parseLegacyWalletAmount(json.Number(tc.value))
		if err != nil || got != tc.want {
			t.Errorf("parseLegacyWalletAmount(%q)=(%d,%v), want (%d,nil)", tc.value, got, err, tc.want)
		}
	}
	for _, value := range []string{"12.4", "-1", "9223372036854775808", "1e3"} {
		if got, err := parseLegacyWalletAmount(json.Number(value)); err == nil {
			t.Errorf("parseLegacyWalletAmount(%q)=(%d,nil), want error", value, got)
		}
	}
}

func TestEnsureWalletRejectsFractionalLegacyAmountIntegration(t *testing.T) {
	svc, _, _ := integrationWallet(t, 0)
	userID := "legacy-wallet-" + uuid.NewString()
	key := "wallet:::" + userID
	raw := json.RawMessage(`{"user_id":"` + userID + `","available":12.4,"currency":"INR"}`)
	if _, err := svc.collection.Insert(key, raw, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = svc.collection.Remove(key, nil)
	})
	if _, err := svc.ensureWallet(context.Background(), userID, "INR"); err == nil {
		t.Fatal("fractional legacy amount was silently accepted")
	}
	result, err := svc.collection.Get(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	var persisted map[string]json.RawMessage
	if err := result.Content(&persisted); err != nil {
		t.Fatal(err)
	}
	if string(persisted["available"]) != "12.4" {
		t.Fatalf("fractional legacy amount changed during rejected migration: %s", persisted["available"])
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
