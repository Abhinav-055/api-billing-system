package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/apibilling/platform/pkg/events"
	walletpb "github.com/apibilling/platform/proto/wallet"
	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

const (
	serviceName          = "wallet-service"
	serviceVersion       = "1.0.0"
	couchbaseCallTimeout = 10 * time.Second
	idempotencyTTL       = 24 * time.Hour
	lockDuration         = 10 * time.Second
)

type config struct {
	GRPCPort          string
	HealthPort        string
	RedisURL          string
	NATSURL           string
	CouchbaseHost     string
	CouchbaseUsername string
	CouchbasePassword string
	CouchbaseBucket   string
}

type walletDoc struct {
	UserID    string    `json:"user_id"`
	Balance   float64   `json:"balance"`
	Currency  string    `json:"currency"`
	UpdatedAt time.Time `json:"updated_at"`
}

type txDoc struct {
	TxID         string    `json:"tx_id"`
	UserID       string    `json:"user_id"`
	Type         string    `json:"type"`
	Amount       float64   `json:"amount"`
	BalanceAfter float64   `json:"balance_after"`
	Reference    string    `json:"reference"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"created_at"`
}

type idempotentResult struct {
	Success    bool    `json:"success"`
	NewBalance float64 `json:"new_balance"`
	Error      string  `json:"error"`
}

type server struct {
	walletpb.UnimplementedWalletServiceServer

	logger      *slog.Logger
	redisClient *redis.Client
	cluster     *gocb.Cluster
	collection  *gocb.Collection
	startedAt   time.Time
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()

	svc, err := newServer(cfg, logger)
	if err != nil {
		logger.Error("failed to initialize wallet service", slog.String("service", serviceName), slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer svc.close()

	go svc.startHealthServer(cfg.HealthPort)
	go svc.subscribeWalletDeduct(cfg.NATSURL)

	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		logger.Error("failed to listen grpc", slog.String("service", serviceName), slog.String("error", err.Error()))
		os.Exit(1)
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(svc.recoveryInterceptor))
	walletpb.RegisterWalletServiceServer(grpcServer, svc)

	logger.Info("grpc server started", slog.String("service", serviceName), slog.String("port", cfg.GRPCPort))
	if err = grpcServer.Serve(lis); err != nil {
		logger.Error("grpc server stopped", slog.String("service", serviceName), slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func loadConfig() config {
	grpcPort := getEnv("WALLET_GRPC_PORT", "9090")

	return config{
		GRPCPort:          grpcPort,
		HealthPort:        getEnv("WALLET_HTTP_PORT", "9089"),
		RedisURL:          getEnv("REDIS_URL", "redis://localhost:6379"),
		NATSURL:           getEnv("NATS_URL", "nats://localhost:4222"),
		CouchbaseHost:     getEnv("COUCHBASE_HOST", "couchbase://localhost"),
		CouchbaseUsername: getEnv("COUCHBASE_USERNAME", "Administrator"),
		CouchbasePassword: getEnv("COUCHBASE_PASSWORD", "password"),
		CouchbaseBucket:   getEnv("COUCHBASE_BUCKET", "apibilling"),
	}
}

func newServer(cfg config, logger *slog.Logger) (*server, error) {
	redisOptions, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}

	redisClient := redis.NewClient(redisOptions)
	if pingErr := redisClient.Ping(context.Background()).Err(); pingErr != nil {
		return nil, fmt.Errorf("ping redis: %w", pingErr)
	}

	cluster, err := gocb.Connect(cfg.CouchbaseHost, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{Username: cfg.CouchbaseUsername, Password: cfg.CouchbasePassword},
	})
	if err != nil {
		return nil, fmt.Errorf("connect couchbase: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), couchbaseCallTimeout)
	defer cancel()
	bucket := cluster.Bucket(cfg.CouchbaseBucket)
	if err = bucket.WaitUntilReady(couchbaseCallTimeout, &gocb.WaitUntilReadyOptions{Context: ctx}); err != nil {
		return nil, fmt.Errorf("wait bucket ready: %w", err)
	}

	return &server{
		logger:      logger,
		redisClient: redisClient,
		cluster:     cluster,
		collection:  bucket.DefaultCollection(),
		startedAt:   time.Now().UTC(),
	}, nil
}

func (s *server) close() {
	if s.cluster != nil {
		s.cluster.Close(nil)
	}
	if s.redisClient != nil {
		_ = s.redisClient.Close()
	}
}

func (s *server) recoveryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp interface{}, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logger.Error("panic recovered",
				slog.String("service", serviceName),
				slog.String("method", info.FullMethod),
				slog.Any("panic", recovered),
				slog.String("stack", string(debug.Stack())),
			)
			err = fmt.Errorf("internal server error")
		}
	}()

	return handler(ctx, req)
}

func (s *server) CheckBalance(ctx context.Context, req *walletpb.BalanceRequest) (*walletpb.BalanceResponse, error) {
	cbCtx, cancel := context.WithTimeout(ctx, couchbaseCallTimeout)
	defer cancel()

	docKey := "wallet:::" + req.GetUserId()
	result, err := s.collection.Get(docKey, &gocb.GetOptions{Context: cbCtx})
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return &walletpb.BalanceResponse{UserId: req.GetUserId(), Balance: 0, HasSufficientFunds: false}, nil
		}
		return nil, fmt.Errorf("get wallet: %w", err)
	}

	var wallet walletDoc
	if err = result.Content(&wallet); err != nil {
		return nil, fmt.Errorf("decode wallet: %w", err)
	}

	return &walletpb.BalanceResponse{
		UserId:             wallet.UserID,
		Balance:            wallet.Balance,
		HasSufficientFunds: wallet.Balance > 0,
	}, nil
}

func (s *server) DeductBalance(ctx context.Context, req *walletpb.DeductRequest) (*walletpb.DeductResponse, error) {
	return s.processDeduct(ctx, req.GetUserId(), req.GetAmount(), req.GetIdempotencyKey(), "grpc")
}

func (s *server) CreditBalance(ctx context.Context, req *walletpb.CreditRequest) (*walletpb.CreditResponse, error) {
	resp, err := s.processDeduct(ctx, req.GetUserId(), -1*req.GetAmount(), req.GetIdempotencyKey(), "credit")
	if err != nil {
		return nil, err
	}

	return &walletpb.CreditResponse{Success: resp.Success, NewBalance: resp.NewBalance, Error: resp.Error}, nil
}

func (s *server) processDeduct(ctx context.Context, userID string, amount float64, idempotencyKey string, reference string) (*walletpb.DeductResponse, error) {
	if strings.TrimSpace(userID) == "" {
		return &walletpb.DeductResponse{Success: false, Error: "user_id is required"}, nil
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		return &walletpb.DeductResponse{Success: false, Error: "idempotency_key is required"}, nil
	}

	redisKey := "idempotent:" + idempotencyKey
	cached, err := s.redisClient.Get(ctx, redisKey).Result()
	if err == nil {
		var cachedResult idempotentResult
		if unmarshalErr := json.Unmarshal([]byte(cached), &cachedResult); unmarshalErr == nil {
			return &walletpb.DeductResponse{
				Success:    cachedResult.Success,
				NewBalance: cachedResult.NewBalance,
				Error:      cachedResult.Error,
			}, nil
		}
	}
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("idempotency lookup: %w", err)
	}

	var response *walletpb.DeductResponse
	for attempt := 0; attempt < 3; attempt++ {
		response, err = s.deductWithCAS(ctx, userID, amount, idempotencyKey, reference)
		if err == nil {
			break
		}
		if !errors.Is(err, gocb.ErrCasMismatch) {
			return nil, err
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		return nil, err
	}

	cacheBody, marshalErr := json.Marshal(idempotentResult{Success: response.Success, NewBalance: response.NewBalance, Error: response.Error})
	if marshalErr == nil {
		_ = s.redisClient.Set(ctx, redisKey, string(cacheBody), idempotencyTTL).Err()
	}

	return response, nil
}

func (s *server) deductWithCAS(ctx context.Context, userID string, amount float64, idempotencyKey string, reference string) (*walletpb.DeductResponse, error) {
	cbCtx, cancel := context.WithTimeout(ctx, couchbaseCallTimeout)
	defer cancel()

	key := "wallet:::" + userID
	getRes, err := s.collection.GetAndLock(key, lockDuration, &gocb.GetAndLockOptions{Context: cbCtx})
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			// First credit for a user may arrive before a wallet document exists.
			// Bootstrap an empty wallet and continue with the same operation.
			if amount < 0 {
				initialWallet := walletDoc{
					UserID:    userID,
					Balance:   0,
					Currency:  "INR",
					UpdatedAt: time.Now().UTC(),
				}
				if _, insertErr := s.collection.Insert(key, initialWallet, &gocb.InsertOptions{Context: cbCtx}); insertErr != nil && !errors.Is(insertErr, gocb.ErrDocumentExists) {
					return nil, fmt.Errorf("bootstrap wallet: %w", insertErr)
				}

				getRes, err = s.collection.GetAndLock(key, lockDuration, &gocb.GetAndLockOptions{Context: cbCtx})
				if err != nil {
					return nil, fmt.Errorf("get and lock wallet after bootstrap: %w", err)
				}
			} else {
				return &walletpb.DeductResponse{Success: false, Error: "wallet not found"}, nil
			}
		}
		if err != nil {
			return nil, fmt.Errorf("get and lock wallet: %w", err)
		}
	}

	var wallet walletDoc
	if err = getRes.Content(&wallet); err != nil {
		return nil, fmt.Errorf("decode wallet: %w", err)
	}

	newBalance := wallet.Balance - amount
	if newBalance < 0 {
		return &walletpb.DeductResponse{Success: false, NewBalance: wallet.Balance, Error: "insufficient balance"}, nil
	}

	wallet.Balance = newBalance
	wallet.UpdatedAt = time.Now().UTC()
	if _, err = s.collection.Replace(key, wallet, &gocb.ReplaceOptions{Cas: getRes.Cas(), Context: cbCtx}); err != nil {
		if errors.Is(err, gocb.ErrCasMismatch) {
			return nil, gocb.ErrCasMismatch
		}
		return nil, fmt.Errorf("replace wallet with cas: %w", err)
	}

	txID := "tx_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	txType := "debit"
	txAmount := amount
	if amount < 0 {
		txType = "credit"
		txAmount = -1 * amount
	}

	tx := txDoc{
		TxID:         txID,
		UserID:       userID,
		Type:         txType,
		Amount:       txAmount,
		BalanceAfter: newBalance,
		Reference:    reference + ":" + idempotencyKey,
		Status:       "completed",
		CreatedAt:    time.Now().UTC(),
	}

	if _, err = s.collection.Upsert("transaction:::"+txID, tx, &gocb.UpsertOptions{Context: cbCtx}); err != nil {
		return nil, fmt.Errorf("create transaction doc: %w", err)
	}

	return &walletpb.DeductResponse{Success: true, NewBalance: newBalance}, nil
}

func (s *server) subscribeWalletDeduct(natsURL string) {
	nc, js, err := events.Connect(natsURL)
	if err != nil {
		s.logger.Error("nats connect failed", slog.String("service", serviceName), slog.String("error", err.Error()))
		return
	}
	defer nc.Close()

	if err = events.SetupStreams(js); err != nil {
		s.logger.Error("setup streams failed", slog.String("service", serviceName), slog.String("error", err.Error()))
		return
	}

	consumer, err := js.CreateOrUpdateConsumer(context.Background(), "WALLET", jetstream.ConsumerConfig{
		Durable:       "wallet-deduct",
		FilterSubject: events.SubjectWalletDeduct,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
	})
	if err != nil {
		consumer, err = js.Consumer(context.Background(), "WALLET", "wallet-deduct")
	}
	if err != nil {
		s.logger.Error("wallet consumer failed", slog.String("service", serviceName), slog.String("error", err.Error()))
		return
	}

	_, err = consumer.Consume(func(msg jetstream.Msg) {
		var evt events.WalletDeductEvent
		if unmarshalErr := json.Unmarshal(msg.Data(), &evt); unmarshalErr != nil {
			s.logger.Error("invalid wallet event", slog.String("service", serviceName), slog.String("error", unmarshalErr.Error()))
			_ = msg.Term()
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, processErr := s.processDeduct(ctx, evt.UserID, evt.Amount, evt.IdempotencyKey, evt.RequestID)
		if processErr != nil {
			s.logger.Error("wallet deduct processing failed", slog.String("service", serviceName), slog.String("error", processErr.Error()))
			_ = msg.Nak()
			return
		}

		_ = msg.Ack()
	})
	if err != nil {
		s.logger.Error("wallet consume failed", slog.String("service", serviceName), slog.String("error", err.Error()))
		return
	}

	select {}
}

func (s *server) startHealthServer(port string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":         "ok",
			"service":        serviceName,
			"version":        serviceVersion,
			"uptime_seconds": int(time.Since(s.startedAt).Seconds()),
		})
	})

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		s.logger.Error("health server failed", slog.String("service", serviceName), slog.String("error", err.Error()))
	}
}

func getEnv(key string, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}
