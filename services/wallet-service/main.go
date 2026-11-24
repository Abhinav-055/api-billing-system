package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"time"

	sharedconfig "github.com/apibilling/platform/pkg/config"
	"github.com/apibilling/platform/pkg/events"
	"github.com/apibilling/platform/pkg/grpcauth"
	walletpb "github.com/apibilling/platform/proto/wallet"
	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	serviceName      = "wallet-service"
	couchbaseTimeout = 10 * time.Second
	lockDuration     = 10 * time.Second
)

type configType struct {
	GRPCPort          string
	HealthPort        string
	RedisURL          string
	NATSURL           string
	CouchbaseHost     string
	CouchbaseUsername string
	CouchbasePassword string
	CouchbaseBucket   string
	GRPCSecret        string
	Env               string
	HoldTTL           time.Duration
	SweepInterval     time.Duration
}

type walletDoc struct {
	UserID      string    `json:"user_id"`
	Available   int64     `json:"available"`
	Balance     int64     `json:"balance,omitempty"`
	Held        int64     `json:"held"`
	Currency    string    `json:"currency"`
	AppliedKeys []string  `json:"applied_keys,omitempty"`
	Ledger      []txDoc   `json:"embedded_ledger,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type txDoc struct {
	TxID         string    `json:"tx_id"`
	UserID       string    `json:"user_id"`
	Type         string    `json:"type"`
	AmountMinor  int64     `json:"amount_minor"`
	BalanceAfter int64     `json:"balance_after"`
	Reference    string    `json:"reference"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"created_at"`
}

type idempotencyDoc struct {
	Key         string           `json:"key"`
	Fingerprint string           `json:"fingerprint"`
	Result      idempotentResult `json:"result"`
	Status      string           `json:"status"`
	CreatedAt   time.Time        `json:"created_at"`
}

type idempotentResult struct {
	Success    bool   `json:"success"`
	NewBalance int64  `json:"new_balance"`
	Error      string `json:"error"`
}

type holdDoc struct {
	ReservationID string    `json:"reservation_id"`
	UserID        string    `json:"user_id"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
	ExpiresAt     time.Time `json:"expires_at"`
}

type failedChargeDoc struct {
	ID              string    `json:"id"`
	UserID          string    `json:"user_id"`
	AmountMinor     int64     `json:"amount_minor"`
	Reason          string    `json:"reason"`
	OriginalPayload string    `json:"original_payload"`
	CreatedAt       time.Time `json:"created_at"`
	DeliveryCount   uint64    `json:"delivery_count"`
}

const walletMaxDeliver = 5

type deliveryDisposition uint8

const (
	deliveryAck deliveryDisposition = iota
	deliveryTerm
	deliveryNak
)

type walletBusinessError struct{ reason string }

func (e walletBusinessError) Error() string { return e.reason }

type malformedWalletPayload struct{}

func (malformedWalletPayload) Error() string { return "MALFORMED_PAYLOAD" }

type server struct {
	walletpb.UnimplementedWalletServiceServer
	cfg        configType
	logger     *slog.Logger
	redis      *redis.Client
	cluster    *gocb.Cluster
	collection *gocb.Collection
	startedAt  time.Time
	cancel     context.CancelFunc
}

// time.Now type alias is intentionally avoided at runtime; this field is initialized below.
type nowTime struct{}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig(logger)
	if err := sharedconfig.RequireSecrets(logger, map[string]string{
		"COUCHBASE_PASSWORD": cfg.CouchbasePassword,
		"JWT_SECRET":         getEnv("JWT_SECRET", ""),
		"GRPC_SHARED_SECRET": cfg.GRPCSecret,
	}); err != nil {
		logger.Error("startup secret validation failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	svc, err := newServer(cfg, logger)
	if err != nil {
		logger.Error("failed to initialize wallet", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer svc.close()

	ctx, cancel := context.WithCancel(context.Background())
	svc.cancel = cancel
	go svc.startHealthServer(cfg.HealthPort)
	go svc.subscribeWalletEvents(ctx)
	go svc.runSweeper(ctx)

	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("grpc listen failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(svc.recoveryInterceptor, grpcauth.UnaryServerInterceptor(cfg.GRPCSecret)),
	)
	walletpb.RegisterWalletServiceServer(grpcServer, svc)
	logger.Info("grpc server started", slog.String("port", cfg.GRPCPort))
	if err := grpcServer.Serve(lis); err != nil {
		logger.Error("grpc server stopped", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func loadConfig(logger *slog.Logger) configType {
	env := sharedconfig.Environment()
	holdTTL := parseDuration(getEnv("HOLD_TTL", "5m"), 5*time.Minute)
	sweepInterval := parseDuration(getEnv("SWEEP_INTERVAL", "1m"), time.Minute)
	return configType{
		GRPCPort:          getEnv("WALLET_GRPC_PORT", "9090"),
		HealthPort:        getEnv("WALLET_HTTP_PORT", "9089"),
		RedisURL:          getEnv("REDIS_URL", "redis://localhost:6379"),
		NATSURL:           getEnv("NATS_URL", "nats://localhost:4222"),
		CouchbaseHost:     getEnv("COUCHBASE_HOST", "couchbase://localhost"),
		CouchbaseUsername: getEnv("COUCHBASE_USERNAME", "Administrator"),
		CouchbasePassword: getEnv("COUCHBASE_PASSWORD", "password"),
		CouchbaseBucket:   getEnv("COUCHBASE_BUCKET", "apibilling"),
		GRPCSecret:        getEnv("GRPC_SHARED_SECRET", "dev-grpc-secret"),
		Env:               env,
		HoldTTL:           holdTTL,
		SweepInterval:     sweepInterval,
	}
}

func newServer(cfg configType, logger *slog.Logger) (*server, error) {
	rOpt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis: %w", err)
	}
	r := redis.NewClient(rOpt)
	if err := r.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	cluster, err := gocb.Connect(cfg.CouchbaseHost, gocb.ClusterOptions{Authenticator: gocb.PasswordAuthenticator{Username: cfg.CouchbaseUsername, Password: cfg.CouchbasePassword}})
	if err != nil {
		return nil, fmt.Errorf("connect couchbase: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), couchbaseTimeout)
	defer cancel()
	bucket := cluster.Bucket(cfg.CouchbaseBucket)
	if err := bucket.WaitUntilReady(couchbaseTimeout, &gocb.WaitUntilReadyOptions{Context: ctx}); err != nil {
		return nil, fmt.Errorf("wait bucket: %w", err)
	}
	collection := bucket.DefaultCollection()
	indexCtx, indexCancel := context.WithTimeout(context.Background(), couchbaseTimeout)
	defer indexCancel()
	_, _ = cluster.Query(
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_hold_status_expires ON `%s`(status, expires_at) WHERE META().id LIKE 'hold::%%'", cfg.CouchbaseBucket),
		&gocb.QueryOptions{Context: indexCtx},
	)
	return &server{cfg: cfg, logger: logger, redis: r, cluster: cluster, collection: collection, startedAt: time.Now().UTC()}, nil
}

func (s *server) close() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.cluster != nil {
		s.cluster.Close(nil)
	}
	if s.redis != nil {
		_ = s.redis.Close()
	}
}

func (s *server) recoveryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logger.Error("panic recovered", slog.String("method", info.FullMethod), slog.Any("panic", recovered), slog.String("stack", string(debug.Stack())))
			err = status.Error(codes.Internal, "internal server error")
		}
	}()
	return handler(ctx, req)
}

func legacyAvailable(w walletDoc) int64 {
	if w.Available != 0 {
		return w.Available
	}
	if w.Balance != 0 {
		return w.Balance
	}
	return 0
}

func legacyFloatToMinorUnits(value float64) int64 {
	return int64(math.Round(value))
}

func (s *server) ensureWallet(ctx context.Context, userID, currency string) (walletDoc, error) {
	key := "wallet:::" + userID
	res, err := s.collection.Get(key, &gocb.GetOptions{Context: ctx})
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		w := walletDoc{UserID: userID, Available: 0, Held: 0, Currency: currency, UpdatedAt: time.Now().UTC()}
		_, err = s.collection.Insert(key, w, &gocb.InsertOptions{Context: ctx})
		if err != nil && !errors.Is(err, gocb.ErrDocumentExists) {
			return walletDoc{}, err
		}
		res, err = s.collection.Get(key, &gocb.GetOptions{Context: ctx})
	}
	if err != nil {
		return walletDoc{}, err
	}
	var w walletDoc
	if err := res.Content(&w); err != nil {
		var raw map[string]any
		if rawErr := res.Content(&raw); rawErr != nil {
			return walletDoc{}, err
		}
		w.UserID, _ = raw["user_id"].(string)
		switch value := raw["available"].(type) {
		case float64:
			w.Available = legacyFloatToMinorUnits(value)
		case int64:
			w.Available = value
		}
		if w.Available == 0 {
			if value, ok := raw["balance"].(float64); ok {
				w.Available = legacyFloatToMinorUnits(value)
			}
		}
		w.Currency, _ = raw["currency"].(string)
		w.UpdatedAt = time.Now().UTC()
		s.logger.Info("migrated legacy wallet float to int64", slog.String("user_id", w.UserID), slog.Int64("available", w.Available))
		if migrationErr := s.migrateLegacyWallet(ctx, "wallet:::"+userID, w); migrationErr != nil {
			return walletDoc{}, migrationErr
		}
	}
	// Migration-safe legacy float reader: Couchbase decoding of a JSON float into int64 fails,
	// so raw float-shaped documents are normalized here by re-reading and replacing before use.
	w.Available = legacyAvailable(w)
	if w.Currency == "" {
		w.Currency = "INR"
	}
	return w, nil
}

func (s *server) migrateLegacyWallet(ctx context.Context, key string, w walletDoc) error {
	if w.Held < 0 {
		w.Held = 0
	}
	_, err := s.collection.Upsert(key, w, &gocb.UpsertOptions{Context: ctx})
	return err
}

func (s *server) CheckBalance(ctx context.Context, req *walletpb.BalanceRequest) (*walletpb.BalanceResponse, error) {
	w, err := s.ensureWallet(ctx, req.GetUserId(), "INR")
	if err != nil {
		return nil, err
	}
	return &walletpb.BalanceResponse{UserId: req.GetUserId(), Balance: w.Available, Held: w.Held, Currency: w.Currency, HasSufficientFunds: w.Available > 0}, nil
}

func (s *server) durableIdempotency(ctx context.Context, key, fingerprint string) (*idempotencyDoc, error) {
	var result idempotencyDoc
	res, err := s.collection.Get("idempotency:::"+key, &gocb.GetOptions{Context: ctx})
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := res.Content(&result); err != nil {
		return nil, err
	}
	if result.Fingerprint != fingerprint {
		return nil, fmt.Errorf("IDEMPOTENCY_KEY_CONFLICT")
	}
	return &result, nil
}

func (s *server) cacheIdempotency(ctx context.Context, key string, result idempotentResult) {
	body, _ := json.Marshal(result)
	_ = s.redis.Set(ctx, "idempotent:"+key, body, 0).Err()
}

func (s *server) mutation(ctx context.Context, userID string, delta int64, operation, idempotencyKey, reference, currency, fingerprint string) (*walletpb.DeductResponse, error) {
	if strings.TrimSpace(userID) == "" || strings.TrimSpace(idempotencyKey) == "" {
		return &walletpb.DeductResponse{Success: false, Error: "invalid mutation request"}, nil
	}

	cbCtx, cancel := context.WithTimeout(ctx, couchbaseTimeout)
	defer cancel()
	if cached, err := s.redis.Get(cbCtx, "idempotent:"+idempotencyKey).Bytes(); err == nil {
		var result idempotentResult
		if json.Unmarshal(cached, &result) == nil {
			return &walletpb.DeductResponse{Success: result.Success, NewBalance: result.NewBalance, Error: result.Error}, nil
		}
	}
	if existing, err := s.durableIdempotency(cbCtx, idempotencyKey, fingerprint); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if existing != nil {
		s.cacheIdempotency(cbCtx, idempotencyKey, existing.Result)
		return &walletpb.DeductResponse{Success: existing.Result.Success, NewBalance: existing.Result.NewBalance, Error: existing.Result.Error}, nil
	}

	key := "wallet:::" + userID
	lockRes, err := s.collection.GetAndLock(key, lockDuration, &gocb.GetAndLockOptions{Context: cbCtx})
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		initial := walletDoc{UserID: userID, Available: 0, Held: 0, Currency: currency, UpdatedAt: time.Now().UTC()}
		_, insErr := s.collection.Insert(key, initial, &gocb.InsertOptions{Context: cbCtx})
		if insErr != nil && !errors.Is(insErr, gocb.ErrDocumentExists) {
			return nil, insErr
		}
		lockRes, err = s.collection.GetAndLock(key, lockDuration, &gocb.GetAndLockOptions{Context: cbCtx})
	}
	if err != nil {
		return nil, err
	}
	var w walletDoc
	if err := lockRes.Content(&w); err != nil {
		return nil, err
	}
	w.Available = legacyAvailable(w)
	if w.Currency == "" {
		w.Currency = currency
	}
	if w.Currency != currency {
		return &walletpb.DeductResponse{Success: false, NewBalance: w.Available, Error: "CURRENCY_MISMATCH"}, nil
	}
	if operation == "debit" && w.Available < delta {
		return &walletpb.DeductResponse{Success: false, NewBalance: w.Available, Error: "INSUFFICIENT_FUNDS"}, nil
	}
	newBalance := w.Available + delta
	if newBalance < 0 {
		return &walletpb.DeductResponse{Success: false, NewBalance: w.Available, Error: "INSUFFICIENT_FUNDS"}, nil
	}

	tx := txDoc{TxID: "tx_" + strings.ReplaceAll(uuid.NewString(), "-", ""), UserID: userID, Type: operation, AmountMinor: abs(delta), BalanceAfter: newBalance, Reference: reference, Status: "completed", CreatedAt: time.Now().UTC()}
	w.Available = newBalance
	w.Balance = 0
	w.Ledger = append(w.Ledger, tx)
	if len(w.AppliedKeys) >= 100 {
		w.AppliedKeys = w.AppliedKeys[len(w.AppliedKeys)-100:]
	}
	w.AppliedKeys = append(w.AppliedKeys, idempotencyKey)
	w.UpdatedAt = time.Now().UTC()
	if _, err := s.collection.Replace(key, w, &gocb.ReplaceOptions{Cas: lockRes.Cas(), Context: cbCtx}); err != nil {
		return nil, fmt.Errorf("CAS wallet mutation: %w", err)
	}
	// Permanent fallback guard and append-only ledger. No TTL.
	idem := idempotencyDoc{Key: idempotencyKey, Fingerprint: fingerprint, Status: "completed", Result: idempotentResult{Success: true, NewBalance: newBalance}, CreatedAt: time.Now().UTC()}
	if _, err := s.collection.Insert("idempotency:::"+idempotencyKey, idem, &gocb.InsertOptions{Context: cbCtx}); err != nil && !errors.Is(err, gocb.ErrDocumentExists) {
		return nil, err
	}
	if _, err := s.collection.Insert("transaction:::"+tx.TxID, tx, &gocb.InsertOptions{Context: cbCtx}); err != nil && !errors.Is(err, gocb.ErrDocumentExists) {
		return nil, err
	}
	s.cacheIdempotency(cbCtx, idempotencyKey, idem.Result)
	return &walletpb.DeductResponse{Success: true, NewBalance: newBalance}, nil
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func (s *server) DeductBalance(ctx context.Context, req *walletpb.DeductRequest) (*walletpb.DeductResponse, error) {
	if req.GetAmountMinor() <= 0 {
		return &walletpb.DeductResponse{Success: false, Error: "INVALID_AMOUNT"}, nil
	}
	fp := fmt.Sprintf("deduct|%s|%d|INR", req.GetUserId(), req.GetAmountMinor())
	return s.mutation(ctx, req.GetUserId(), -req.GetAmountMinor(), "debit", req.GetIdempotencyKey(), "grpc:"+req.GetIdempotencyKey(), "INR", fp)
}

func (s *server) CreditBalance(ctx context.Context, req *walletpb.CreditRequest) (*walletpb.CreditResponse, error) {
	if req.GetAmountMinor() <= 0 {
		return &walletpb.CreditResponse{Success: false, Error: "INVALID_AMOUNT"}, nil
	}
	currency := req.GetCurrency()
	if currency == "" {
		currency = "INR"
	}
	fp := fmt.Sprintf("credit|%s|%d|%s", req.GetUserId(), req.GetAmountMinor(), currency)
	resp, err := s.mutation(ctx, req.GetUserId(), req.GetAmountMinor(), "credit", req.GetIdempotencyKey(), "grpc:"+req.GetIdempotencyKey(), currency, fp)
	if err != nil {
		return nil, err
	}
	return &walletpb.CreditResponse{Success: resp.Success, NewBalance: resp.NewBalance, Error: resp.Error}, nil
}

func (s *server) ReserveFunds(ctx context.Context, req *walletpb.ReserveFundsRequest) (*walletpb.ReserveFundsResponse, error) {
	if req.GetAmountMinor() <= 0 || strings.TrimSpace(req.GetReservationId()) == "" {
		return &walletpb.ReserveFundsResponse{Reserved: false, Reason: "INVALID_REQUEST"}, nil
	}
	currency := req.GetCurrency()
	if currency == "" {
		currency = "INR"
	}
	holdKey := "hold:::" + req.GetReservationId()
	if res, err := s.collection.Get(holdKey, &gocb.GetOptions{Context: ctx}); err == nil {
		var hold holdDoc
		if err := res.Content(&hold); err == nil {
			if hold.Status == "HELD" {
				return &walletpb.ReserveFundsResponse{Reserved: true, AvailableBalance: 0}, nil
			}
			if hold.Status == "CAPTURED" || hold.Status == "RELEASED" {
				return &walletpb.ReserveFundsResponse{Reserved: false, Reason: "RESERVATION_CLOSED"}, nil
			}
		}
	}

	walletKey := "wallet:::" + req.GetUserId()
	lockRes, err := s.collection.GetAndLock(walletKey, lockDuration, &gocb.GetAndLockOptions{Context: ctx})
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return &walletpb.ReserveFundsResponse{Reserved: false, Reason: "INSUFFICIENT_FUNDS"}, nil
	}
	if err != nil {
		return nil, err
	}
	var w walletDoc
	if err := lockRes.Content(&w); err != nil {
		return nil, err
	}
	w.Available = legacyAvailable(w)
	if w.Available < req.GetAmountMinor() {
		return &walletpb.ReserveFundsResponse{Reserved: false, Reason: "INSUFFICIENT_FUNDS", AvailableBalance: w.Available}, nil
	}
	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	if ttl <= 0 {
		ttl = s.cfg.HoldTTL
	}
	now := time.Now().UTC()
	hold := holdDoc{ReservationID: req.GetReservationId(), UserID: req.GetUserId(), Amount: req.GetAmountMinor(), Currency: currency, Status: "HELD", CreatedAt: now, ExpiresAt: now.Add(ttl)}
	w.Available -= req.GetAmountMinor()
	w.Held += req.GetAmountMinor()
	w.Currency = currency
	w.UpdatedAt = now
	if _, err := s.collection.Replace(walletKey, w, &gocb.ReplaceOptions{Cas: lockRes.Cas(), Context: ctx}); err != nil {
		return nil, err
	}
	if _, err := s.collection.Insert(holdKey, hold, &gocb.InsertOptions{Context: ctx}); err != nil {
		if !errors.Is(err, gocb.ErrDocumentExists) {
			return nil, err
		}
	}
	return &walletpb.ReserveFundsResponse{Reserved: true, AvailableBalance: w.Available}, nil
}

func (s *server) CaptureReservation(ctx context.Context, req *walletpb.CaptureReservationRequest) (*walletpb.CaptureReservationResponse, error) {
	holdKey := "hold:::" + req.GetReservationId()
	res, err := s.collection.Get(holdKey, &gocb.GetOptions{Context: ctx})
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return &walletpb.CaptureReservationResponse{Captured: false, Reason: "NOT_FOUND"}, nil
	}
	if err != nil {
		return nil, err
	}
	var hold holdDoc
	if err := res.Content(&hold); err != nil {
		return nil, err
	}
	if hold.Status == "CAPTURED" {
		return &walletpb.CaptureReservationResponse{Captured: true}, nil
	}
	if hold.Status == "RELEASED" {
		return &walletpb.CaptureReservationResponse{Captured: false, Reason: "RESERVATION_EXPIRED"}, nil
	}
	if time.Now().UTC().After(hold.ExpiresAt) {
		return &walletpb.CaptureReservationResponse{Captured: false, Reason: "RESERVATION_EXPIRED"}, nil
	}
	actual := req.GetActualAmount()
	if actual < 0 || actual > hold.Amount {
		actual = hold.Amount
	}

	walletKey := "wallet:::" + hold.UserID
	lockRes, err := s.collection.GetAndLock(walletKey, lockDuration, &gocb.GetAndLockOptions{Context: ctx})
	if err != nil {
		return nil, err
	}
	var w walletDoc
	if err := lockRes.Content(&w); err != nil {
		return nil, err
	}
	w.Available = legacyAvailable(w)
	refund := hold.Amount - actual
	if w.Held < hold.Amount {
		return &walletpb.CaptureReservationResponse{Captured: false, Reason: "NOT_FOUND"}, nil
	}
	w.Held -= hold.Amount
	w.Available += refund
	w.UpdatedAt = time.Now().UTC()
	tx := txDoc{TxID: "tx_" + strings.ReplaceAll(uuid.NewString(), "-", ""), UserID: hold.UserID, Type: "capture", AmountMinor: actual, BalanceAfter: w.Available, Reference: "reservation:" + req.GetReservationId(), Status: "completed", CreatedAt: time.Now().UTC()}
	w.Ledger = append(w.Ledger, tx)
	if _, err := s.collection.Replace(walletKey, w, &gocb.ReplaceOptions{Cas: lockRes.Cas(), Context: ctx}); err != nil {
		return nil, err
	}
	hold.Status = "CAPTURED"
	hold.ExpiresAt = time.Now().UTC()
	if _, err := s.collection.Replace(holdKey, hold, &gocb.ReplaceOptions{Cas: res.Cas(), Context: ctx}); err != nil && !errors.Is(err, gocb.ErrCasMismatch) {
		return nil, err
	}
	if _, err := s.collection.Insert("transaction:::"+tx.TxID, tx, &gocb.InsertOptions{Context: ctx}); err != nil && !errors.Is(err, gocb.ErrDocumentExists) {
		return nil, err
	}
	return &walletpb.CaptureReservationResponse{Captured: true, AvailableBalance: w.Available}, nil
}

func (s *server) ReleaseReservation(ctx context.Context, req *walletpb.ReleaseReservationRequest) (*walletpb.ReleaseReservationResponse, error) {
	holdKey := "hold:::" + req.GetReservationId()
	res, err := s.collection.Get(holdKey, &gocb.GetOptions{Context: ctx})
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		return &walletpb.ReleaseReservationResponse{Released: false, Reason: "NOT_FOUND"}, nil
	}
	if err != nil {
		return nil, err
	}
	var hold holdDoc
	if err := res.Content(&hold); err != nil {
		return nil, err
	}
	if hold.Status == "RELEASED" {
		return &walletpb.ReleaseReservationResponse{Released: true, Reason: "RELEASED"}, nil
	}
	if hold.Status == "CAPTURED" {
		return &walletpb.ReleaseReservationResponse{Released: false, Reason: "RESERVATION_CAPTURED"}, nil
	}

	walletKey := "wallet:::" + hold.UserID
	lockRes, err := s.collection.GetAndLock(walletKey, lockDuration, &gocb.GetAndLockOptions{Context: ctx})
	if err != nil {
		return nil, err
	}
	var w walletDoc
	if err := lockRes.Content(&w); err != nil {
		return nil, err
	}
	w.Available = legacyAvailable(w)
	if w.Held >= hold.Amount {
		w.Held -= hold.Amount
		w.Available += hold.Amount
	}
	w.UpdatedAt = time.Now().UTC()
	if _, err := s.collection.Replace(walletKey, w, &gocb.ReplaceOptions{Cas: lockRes.Cas(), Context: ctx}); err != nil {
		return nil, err
	}
	hold.Status = "RELEASED"
	if _, err := s.collection.Replace(holdKey, hold, &gocb.ReplaceOptions{Cas: res.Cas(), Context: ctx}); err != nil && !errors.Is(err, gocb.ErrCasMismatch) {
		return nil, err
	}
	return &walletpb.ReleaseReservationResponse{Released: true, AvailableBalance: w.Available}, nil
}

func (s *server) runSweeper(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweepHeld(ctx, s)
		}
	}
}

func sweepHeld(ctx context.Context, s *server) {
	statement := fmt.Sprintf("SELECT META().id AS id, reservation_id, user_id, amount, status, expires_at FROM `%s` WHERE status = 'HELD' AND expires_at < $now", s.cfg.CouchbaseBucket)
	result, err := s.cluster.Query(statement, &gocb.QueryOptions{Context: ctx, NamedParameters: map[string]any{"now": time.Now().UTC().Format(time.RFC3339)}})
	if err != nil {
		s.logger.Error("hold sweeper query failed", slog.String("error", err.Error()))
		return
	}
	defer result.Close()
	for result.Next() {
		var row struct {
			ID            string `json:"id"`
			ReservationID string `json:"reservation_id"`
		}
		if err := result.Row(&row); err != nil {
			continue
		}
		_, err := s.ReleaseReservation(ctx, &walletpb.ReleaseReservationRequest{ReservationId: row.ReservationID})
		if err != nil {
			s.logger.Error("hold release failed", slog.String("reservation_id", row.ReservationID), slog.String("error", err.Error()))
		}
	}
}

func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gocb.ErrTimeout) ||
		errors.Is(err, gocb.ErrTemporaryFailure) ||
		errors.Is(err, gocb.ErrDocumentLocked) ||
		errors.Is(err, gocb.ErrCasMismatch) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{"timeout", "temporary", "cas", "locked", "lock contention", "network", "connection reset", "unavailable"} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

type walletEventProcessor func(context.Context, string, []byte) error
type failedChargeRecorder func(context.Context, string, []byte, uint64, error) error
type walletDLQPublisher func(context.Context, []byte) error

func processWalletDelivery(
	ctx context.Context,
	subject string,
	payload []byte,
	deliveryCount uint64,
	process walletEventProcessor,
	recordFailure failedChargeRecorder,
	publishDLQ walletDLQPublisher,
) deliveryDisposition {
	if subject != events.SubjectWalletCapture && subject != events.SubjectWalletDeduct {
		if err := recordFailure(ctx, subject, payload, deliveryCount, malformedWalletPayload{}); err != nil {
			return auditFailureDisposition(ctx, payload, deliveryCount, publishDLQ)
		}
		return deliveryTerm
	}
	if len(payload) == 0 || !json.Valid(payload) {
		if err := recordFailure(ctx, subject, payload, deliveryCount, malformedWalletPayload{}); err != nil {
			return auditFailureDisposition(ctx, payload, deliveryCount, publishDLQ)
		}
		return deliveryTerm
	}
	err := process(ctx, subject, payload)
	if err == nil {
		return deliveryAck
	}
	var malformed malformedWalletPayload
	var business walletBusinessError
	if errors.As(err, &malformed) || errors.As(err, &business) {
		if recordErr := recordFailure(ctx, subject, payload, deliveryCount, err); recordErr != nil {
			return auditFailureDisposition(ctx, payload, deliveryCount, publishDLQ)
		}
		return deliveryTerm
	}
	if !isTransientError(err) {
		if recordErr := recordFailure(ctx, subject, payload, deliveryCount, err); recordErr != nil {
			return auditFailureDisposition(ctx, payload, deliveryCount, publishDLQ)
		}
		return deliveryTerm
	}
	if deliveryCount >= walletMaxDeliver {
		if dlqErr := publishDLQ(ctx, payload); dlqErr != nil {
			return deliveryNak
		}
		return deliveryTerm
	}
	return deliveryNak
}

func auditFailureDisposition(ctx context.Context, payload []byte, deliveryCount uint64, publishDLQ walletDLQPublisher) deliveryDisposition {
	if deliveryCount >= walletMaxDeliver {
		if err := publishDLQ(ctx, payload); err != nil {
			return deliveryNak
		}
		return deliveryTerm
	}
	return deliveryNak
}

func (s *server) subscribeWalletEvents(ctx context.Context) {
	nc, js, err := events.Connect(s.cfg.NATSURL)
	if err != nil {
		s.logger.Error("wallet NATS connection failed", slog.String("error", err.Error()))
		return
	}
	defer nc.Close()
	if err := events.SetupStreams(js); err != nil {
		s.logger.Error("wallet stream setup failed", slog.String("error", err.Error()))
		return
	}
	consumer, err := js.CreateOrUpdateConsumer(ctx, "WALLET", jetstream.ConsumerConfig{
		Durable:        "wallet-deduct",
		FilterSubjects: []string{events.SubjectWalletDeduct, events.SubjectWalletCapture},
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		MaxDeliver:     walletMaxDeliver,
		BackOff:        []time.Duration{1 * time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second},
	})
	if err != nil {
		s.logger.Error("wallet consumer creation failed", slog.String("error", err.Error()))
		return
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		s.logger.Error("wallet consumer info failed", slog.String("error", err.Error()))
		return
	}
	if info.Config.MaxDeliver != walletMaxDeliver {
		s.logger.Error("wallet consumer MaxDeliver mismatch", slog.Int("max_deliver", info.Config.MaxDeliver))
		return
	}
	s.logger.Info("wallet consumer ready", slog.String("consumer", info.Name), slog.Int("max_deliver", info.Config.MaxDeliver))
	_, err = consumer.Consume(func(msg jetstream.Msg) {
		count := uint64(1)
		if meta, metaErr := msg.Metadata(); metaErr != nil {
			s.logger.Error("wallet message metadata unavailable", slog.String("error", metaErr.Error()))
		} else {
			count = meta.NumDelivered
		}
		disposition := processWalletDelivery(ctx, msg.Subject(), msg.Data(), count, s.processWalletEvent, s.recordFailedCharge,
			func(publishCtx context.Context, payload []byte) error {
				_, publishErr := js.Publish(publishCtx, events.SubjectWalletDLQ, payload)
				return publishErr
			})
		switch disposition {
		case deliveryAck:
			_ = msg.Ack()
		case deliveryTerm:
			_ = msg.Term()
		case deliveryNak:
			if count >= walletMaxDeliver {
				s.logger.Error("wallet DLQ publish or failed-charge persistence failed; original message remains unacknowledged",
					slog.String("subject", msg.Subject()), slog.Uint64("delivery_count", count))
			}
			_ = msg.NakWithDelay(walletBackoff(count))
		}
	})
	if err != nil {
		s.logger.Error("wallet consume failed", slog.String("error", err.Error()))
		return
	}
	<-ctx.Done()
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func walletBackoff(deliveryCount uint64) time.Duration {
	schedule := [...]time.Duration{time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second}
	index := minUint64(deliveryCount-1, uint64(len(schedule)-1))
	return schedule[index]
}

func (s *server) processWalletEvent(ctx context.Context, subject string, payload []byte) error {
	switch subject {
	case events.SubjectWalletCapture:
		var evt events.WalletCaptureEvent
		if err := json.Unmarshal(payload, &evt); err != nil || evt.ReservationID == "" || evt.UserID == "" {
			return malformedWalletPayload{}
		}
		resp, err := s.CaptureReservation(ctx, &walletpb.CaptureReservationRequest{ReservationId: evt.ReservationID, ActualAmount: evt.ActualAmount})
		if err != nil {
			return err
		}
		if !resp.GetCaptured() {
			return walletBusinessError{reason: resp.GetReason()}
		}
		return nil
	case events.SubjectWalletDeduct:
		var evt events.WalletDeductEvent
		if err := json.Unmarshal(payload, &evt); err != nil || evt.UserID == "" || evt.IdempotencyKey == "" || evt.AmountMinor <= 0 {
			return malformedWalletPayload{}
		}
		resp, err := s.DeductBalance(ctx, &walletpb.DeductRequest{UserId: evt.UserID, AmountMinor: evt.AmountMinor, IdempotencyKey: evt.IdempotencyKey})
		if err != nil {
			return err
		}
		if !resp.GetSuccess() {
			return walletBusinessError{reason: resp.GetError()}
		}
		return nil
	default:
		return malformedWalletPayload{}
	}
}

func (s *server) recordFailedCharge(ctx context.Context, subject string, payload []byte, deliveryCount uint64, reason error) error {
	var details struct {
		UserID        string `json:"user_id"`
		AmountMinor   int64  `json:"amount_minor"`
		Amount        int64  `json:"actual_amount"`
		ReservationID string `json:"reservation_id"`
	}
	_ = json.Unmarshal(payload, &details)
	if details.AmountMinor == 0 {
		details.AmountMinor = details.Amount
	}
	hash := sha256.Sum256(append(append([]byte(subject), 0), payload...))
	id := hex.EncodeToString(hash[:])
	doc := failedChargeDoc{
		ID:              id,
		UserID:          details.UserID,
		AmountMinor:     details.AmountMinor,
		Reason:          reason.Error(),
		OriginalPayload: string(payload),
		CreatedAt:       time.Now().UTC(),
		DeliveryCount:   deliveryCount,
	}
	_, err := s.collection.Insert("failed_charge:::"+doc.ID, doc, &gocb.InsertOptions{Context: ctx})
	if errors.Is(err, gocb.ErrDocumentExists) {
		return nil
	}
	return err
}

func (s *server) startHealthServer(port string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "service": serviceName, "uptime_seconds": int(time.Since(s.startedAt).Seconds())})
	})
	_ = http.ListenAndServe(":"+port, mux)
}

func parseDuration(raw string, fallback time.Duration) time.Duration {
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

func getEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
