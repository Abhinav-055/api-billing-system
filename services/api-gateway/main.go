package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	sharedconfig "github.com/apibilling/platform/pkg/config"
	"github.com/apibilling/platform/pkg/events"
	"github.com/apibilling/platform/pkg/grpcauth"
	walletpb "github.com/apibilling/platform/proto/wallet"
	"github.com/couchbase/gocb/v2"
	"github.com/go-chi/chi/v5"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	serviceName          = "api-gateway"
	serviceVersion       = "1.0.0"
	jwtExpirationHours   = 24
	redisRateBucketSize  = 100.0
	redisRateRefillPerS  = 10.0
	grpcCallTimeout      = 5 * time.Second
	couchbaseCallTimeout = 10 * time.Second
)

type contextKey string

const (
	ctxRequestID     contextKey = "request_id"
	ctxUserID        contextKey = "user_id"
	ctxReservationID contextKey = "reservation_id"
)

type config struct {
	Port               string
	JWTSecret          string
	APIKeySalt         string
	RedisURL           string
	NATSURL            string
	CouchbaseHost      string
	CouchbaseUsername  string
	CouchbasePassword  string
	CouchbaseBucket    string
	WalletGRPCAddr     string
	InternalServerAddr string
	GRPCSecret         string
}

type app struct {
	cfg          config
	logger       *slog.Logger
	redisClient  *redis.Client
	cluster      *gocb.Cluster
	bucket       *gocb.Bucket
	collection   *gocb.Collection
	walletConn   *grpc.ClientConn
	walletClient walletpb.WalletServiceClient
	natsConn     interface{ Close() }
	publisher    *events.Publisher
	startedAt    time.Time
}

type userRecord struct {
	UserID       string    `json:"user_id"`
	Email        string    `json:"email"`
	PasswordHash string    `json:"password_hash"`
	CreatedAt    time.Time `json:"created_at"`
}

type userLoginIndex struct {
	UserID       string `json:"user_id"`
	PasswordHash string `json:"password_hash"`
	Email        string `json:"email"`
}

type apiKeyRecord struct {
	KeyID     string    `json:"key_id"`
	UserID    string    `json:"user_id"`
	APIKey    string    `json:"api_key"`
	Hash      string    `json:"hash"`
	Prefix    string    `json:"prefix"`
	Revoked   bool      `json:"revoked"`
	CreatedAt time.Time `json:"created_at"`
	RevokedAt time.Time `json:"revoked_at,omitempty"`
}

type usageLogRecord struct {
	UserID     string    `json:"user_id"`
	Endpoint   string    `json:"endpoint"`
	Method     string    `json:"method"`
	Cost       int       `json:"cost"`
	RequestID  string    `json:"request_id"`
	StatusCode int       `json:"status_code"`
	LatencyMS  int64     `json:"latency_ms"`
	Timestamp  time.Time `json:"timestamp"`
}

type apiError struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (r *responseRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()
	if err := sharedconfig.RequireSecrets(logger, map[string]string{
		"JWT_SECRET":         cfg.JWTSecret,
		"API_KEY_SALT":       cfg.APIKeySalt,
		"COUCHBASE_PASSWORD": cfg.CouchbasePassword,
		"GRPC_SHARED_SECRET": cfg.GRPCSecret,
	}); err != nil {
		logger.Error("startup secret validation failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	application, err := newApp(cfg, logger)
	if err != nil {
		logger.Error("failed to initialize app", slog.String("service", serviceName), slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer application.close()

	r := chi.NewRouter()
	r.Use(application.panicRecoveryMiddleware)
	r.Use(application.requestIDMiddleware)
	r.Use(application.structuredLoggingMiddleware)

	r.Get("/health", application.handleHealth)
	r.Post("/auth/register", application.handleRegister)
	r.Post("/auth/login", application.handleLogin)

	r.Group(func(protected chi.Router) {
		protected.Use(application.authMiddleware)

		protected.Get("/keys", application.handleListAPIKeys)
		protected.Post("/keys/generate", application.handleGenerateAPIKey)
		protected.Delete("/keys/{key_id}", application.handleRevokeAPIKey)

		protected.Get("/api/v1/wallet/balance", application.handleWalletBalance)
		protected.Get("/api/v1/usage/summary", application.handleUsageSummary)
		protected.Get("/api/v1/usage/logs", application.handleUsageLogs)

		protected.Group(func(metered chi.Router) {
			metered.Use(application.rateLimitMiddleware)
			metered.Use(application.reservationMiddleware)
			metered.Use(application.usageLogMiddleware)

			metered.Get("/api/v1/data", application.handleData)
			metered.Get("/api/v1/compute", application.handleCompute)
		})
	})

	addr := ":" + cfg.Port
	server := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info("service started", slog.String("service", serviceName), slog.String("addr", addr))
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("http server failed", slog.String("service", serviceName), slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func loadConfig() config {
	port := getEnv("API_GATEWAY_PORT", "8080")
	grpcPort := getEnv("WALLET_GRPC_PORT", "9090")

	return config{
		Port:               port,
		JWTSecret:          getEnv("JWT_SECRET", "dev-secret"),
		APIKeySalt:         getEnv("API_KEY_SALT", "dev-salt"),
		RedisURL:           getEnv("REDIS_URL", "redis://localhost:6379"),
		NATSURL:            getEnv("NATS_URL", "nats://localhost:4222"),
		CouchbaseHost:      getEnv("COUCHBASE_HOST", "couchbase://localhost"),
		CouchbaseUsername:  getEnv("COUCHBASE_USERNAME", "Administrator"),
		CouchbasePassword:  getEnv("COUCHBASE_PASSWORD", "password"),
		CouchbaseBucket:    getEnv("COUCHBASE_BUCKET", "apibilling"),
		WalletGRPCAddr:     "wallet-service:" + grpcPort,
		InternalServerAddr: "localhost:" + grpcPort,
		GRPCSecret:         getEnv("GRPC_SHARED_SECRET", "dev-grpc-secret"),
	}
}

func newApp(cfg config, logger *slog.Logger) (*app, error) {
	redisOptions, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}

	redisClient := redis.NewClient(redisOptions)
	if pingErr := redisClient.Ping(context.Background()).Err(); pingErr != nil {
		return nil, fmt.Errorf("ping redis: %w", pingErr)
	}

	cluster, err := gocb.Connect(cfg.CouchbaseHost, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: cfg.CouchbaseUsername,
			Password: cfg.CouchbasePassword,
		},
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

	walletAddr := cfg.WalletGRPCAddr
	if os.Getenv("RUN_LOCAL") == "true" {
		walletAddr = cfg.InternalServerAddr
	}

	walletConn, err := grpc.NewClient(walletAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(grpcauth.UnaryClientInterceptor(cfg.GRPCSecret)),
	)
	if err != nil {
		return nil, fmt.Errorf("connect wallet grpc: %w", err)
	}

	natsConn, js, err := events.Connect(cfg.NATSURL)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}

	if err = events.SetupStreams(js); err != nil {
		return nil, fmt.Errorf("setup nats streams: %w", err)
	}

	return &app{
		cfg:          cfg,
		logger:       logger,
		redisClient:  redisClient,
		cluster:      cluster,
		bucket:       bucket,
		collection:   bucket.DefaultCollection(),
		walletConn:   walletConn,
		walletClient: walletpb.NewWalletServiceClient(walletConn),
		natsConn:     natsConn,
		publisher:    events.NewPublisher(js),
		startedAt:    time.Now().UTC(),
	}, nil
}

func (a *app) close() {
	if a.walletConn != nil {
		_ = a.walletConn.Close()
	}

	if a.cluster != nil {
		a.cluster.Close(nil)
	}

	if a.redisClient != nil {
		_ = a.redisClient.Close()
	}

	if a.natsConn != nil {
		a.natsConn.Close()
	}
}

func (a *app) panicRecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				a.logger.Error("panic recovered",
					slog.String("service", serviceName),
					slog.Any("panic", recovered),
					slog.String("stack", string(debug.Stack())),
					slog.String("request_id", requestIDFromContext(r.Context())),
				)
				writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error")
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func (a *app) requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := uuid.NewString()
		w.Header().Set("X-Request-ID", requestID)
		ctx := context.WithValue(r.Context(), ctxRequestID, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (a *app) structuredLoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		a.logger.Info("http request",
			slog.String("service", serviceName),
			slog.String("request_id", requestIDFromContext(r.Context())),
			slog.String("user_id", userIDFromContext(r.Context())),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status_code", rec.status),
			slog.Int64("latency_ms", time.Since(start).Milliseconds()),
		)
	})
}

func (a *app) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if authorization := r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
			tokenString := strings.TrimSpace(strings.TrimPrefix(authorization, "Bearer"))
			claims := jwt.MapClaims{}
			token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
				if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, errors.New("invalid signing method")
				}
				return []byte(a.cfg.JWTSecret), nil
			})
			if err != nil || !token.Valid {
				writeJSONError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid bearer token")
				return
			}

			userID, ok := claims["sub"].(string)
			if !ok || strings.TrimSpace(userID) == "" {
				writeJSONError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid token claims")
				return
			}

			ctx := context.WithValue(r.Context(), ctxUserID, userID)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		apiKey := strings.TrimSpace(r.Header.Get("X-API-Key"))
		if apiKey == "" {
			writeJSONError(w, http.StatusUnauthorized, "UNAUTHORIZED", "missing credentials")
			return
		}

		hash := hashAPIKey(a.cfg.APIKeySalt, apiKey)
		userID, err := a.redisClient.Get(r.Context(), "apikey:"+hash).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				writeJSONError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid api key")
				return
			}

			writeJSONError(w, http.StatusInternalServerError, "REDIS_ERROR", "auth lookup failed")
			return
		}

		ctx := context.WithValue(r.Context(), ctxUserID, userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (a *app) rateLimitMiddleware(next http.Handler) http.Handler {
	script := redis.NewScript(`
local key = KEYS[1]
local now = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local refill_per_sec = tonumber(ARGV[3])

local data = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(data[1])
local ts = tonumber(data[2])

if tokens == nil then
  tokens = capacity
  ts = now
end

local elapsed = math.max(0, now - ts)
local refill = (elapsed / 1000.0) * refill_per_sec
tokens = math.min(capacity, tokens + refill)

if tokens < 1 then
  local retry_ms = math.ceil((1 - tokens) / refill_per_sec * 1000)
  redis.call("HMSET", key, "tokens", tokens, "ts", now)
  redis.call("PEXPIRE", key, 60000)
  return {0, retry_ms}
end

tokens = tokens - 1
redis.call("HMSET", key, "tokens", tokens, "ts", now)
redis.call("PEXPIRE", key, 60000)
return {1, 0}
`)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID := userIDFromContext(r.Context())
		if userID == "" {
			writeJSONError(w, http.StatusUnauthorized, "UNAUTHORIZED", "missing user")
			return
		}

		nowMillis := time.Now().UnixMilli()
		res, err := script.Run(r.Context(), a.redisClient, []string{"ratelimit:" + userID}, nowMillis, redisRateBucketSize, redisRateRefillPerS).Result()
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "RATE_LIMIT_ERROR", "rate limiter unavailable")
			return
		}

		values, ok := res.([]interface{})
		if !ok || len(values) != 2 {
			writeJSONError(w, http.StatusInternalServerError, "RATE_LIMIT_ERROR", "invalid limiter response")
			return
		}

		allowed, ok := values[0].(int64)
		if !ok {
			writeJSONError(w, http.StatusInternalServerError, "RATE_LIMIT_ERROR", "invalid limiter state")
			return
		}

		if allowed == 0 {
			retryMillis, convErr := convertInt64(values[1])
			if convErr != nil {
				writeJSONError(w, http.StatusTooManyRequests, "RATE_LIMIT_EXCEEDED", "rate limit exceeded")
				return
			}

			retryAfter := int(math.Ceil(float64(retryMillis) / 1000.0))
			if retryAfter < 1 {
				retryAfter = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			writeJSONError(w, http.StatusTooManyRequests, "RATE_LIMIT_EXCEEDED", "rate limit exceeded")
			return
		}

		next.ServeHTTP(w, r)
	})
}

func routeCost(path string) int64 {
	switch strings.TrimSpace(path) {
	case "/api/v1/data":
		return 1
	case "/api/v1/compute":
		return 3
	default:
		return 2
	}
}

func (a *app) reservationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cost := routeCost(r.URL.Path)
		reservationID := requestIDFromContext(r.Context())
		ctx, cancel := context.WithTimeout(r.Context(), grpcCallTimeout)
		resp, err := a.walletClient.ReserveFunds(ctx, &walletpb.ReserveFundsRequest{
			UserId:        userIDFromContext(r.Context()),
			AmountMinor:   cost,
			ReservationId: reservationID,
			TtlSeconds:    int64((5 * time.Minute).Seconds()),
			Currency:      "INR",
		})
		cancel()
		if err != nil {
			if status.Code(err) == codes.ResourceExhausted {
				w.Header().Set("Retry-After", "1")
				writeJSONError(w, http.StatusServiceUnavailable, "WALLET_BUSY", "wallet contention; retry shortly")
				return
			}
			writeJSONError(w, http.StatusServiceUnavailable, "WALLET_UNAVAILABLE", "wallet unavailable")
			return
		}
		if !resp.GetReserved() {
			if resp.GetReason() == "INSUFFICIENT_FUNDS" {
				writeJSONError(w, http.StatusPaymentRequired, "INSUFFICIENT_FUNDS", "insufficient wallet balance")
				return
			}
			writeJSONError(w, http.StatusConflict, "RESERVATION_FAILED", resp.GetReason())
			return
		}

		r = r.WithContext(context.WithValue(r.Context(), ctxReservationID, reservationID))
		rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if rec.status >= 500 {
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), time.Second)
			_, releaseErr := a.walletClient.ReleaseReservation(releaseCtx, &walletpb.ReleaseReservationRequest{ReservationId: reservationID})
			releaseCancel()
			if releaseErr != nil {
				a.logger.Error("failed to release reservation; sweeper will reconcile",
					slog.String("reservation_id", reservationID),
					slog.String("error", releaseErr.Error()))
			}
		}
	})
}

func (a *app) usageLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		if rec.status >= 500 {
			return
		}

		event := events.UsageEvent{
			UserID:        userIDFromContext(r.Context()),
			Endpoint:      r.URL.Path,
			Method:        r.Method,
			CostMinor:     routeCost(r.URL.Path),
			Currency:      "INR",
			StatusCode:    rec.status,
			LatencyMs:     time.Since(start).Milliseconds(),
			RequestID:     requestIDFromContext(r.Context()),
			ReservationID: requestIDFromContext(r.Context()),
			Timestamp:     time.Now().UTC(),
		}

		if err := a.publisher.PublishUsage(r.Context(), event); err != nil {
			a.logger.Error("failed to publish usage",
				slog.String("service", serviceName),
				slog.String("request_id", requestIDFromContext(r.Context())),
				slog.String("user_id", userIDFromContext(r.Context())),
				slog.String("error", err.Error()),
			)
		}
	})
}

func (a *app) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"service":        serviceName,
		"version":        serviceVersion,
		"uptime_seconds": int(time.Since(a.startedAt).Seconds()),
	})
}

func (a *app) handleRegister(w http.ResponseWriter, r *http.Request) {
	type request struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}

	var req request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid json body")
		return
	}

	req.Email = strings.TrimSpace(strings.ToLower(req.Email))
	if req.Email == "" || req.Password == "" {
		writeJSONError(w, http.StatusBadRequest, "VALIDATION_ERROR", "email and password are required")
		return
	}

	indexKey := userEmailKey(req.Email)
	ctx, cancel := context.WithTimeout(r.Context(), couchbaseCallTimeout)
	defer cancel()

	_, err := a.collection.Get(indexKey, &gocb.GetOptions{Context: ctx})
	if err == nil {
		writeJSONError(w, http.StatusConflict, "USER_EXISTS", "user already exists")
		return
	}
	if !errors.Is(err, gocb.ErrDocumentNotFound) {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to check existing user")
		return
	}

	passwordHashBytes, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "HASH_ERROR", "failed to secure password")
		return
	}

	userID := "u_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	now := time.Now().UTC()
	user := userRecord{UserID: userID, Email: req.Email, PasswordHash: string(passwordHashBytes), CreatedAt: now}
	index := userLoginIndex{UserID: userID, PasswordHash: user.PasswordHash, Email: req.Email}

	if _, err = a.collection.Insert("user:::"+userID, user, &gocb.InsertOptions{Context: ctx}); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to create user")
		return
	}

	if _, err = a.collection.Insert(indexKey, index, &gocb.InsertOptions{Context: ctx}); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to create user index")
		return
	}

	token, err := generateJWT(a.cfg.JWTSecret, userID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "JWT_ERROR", "failed to issue token")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{"user_id": userID, "token": token})
}

func (a *app) handleLogin(w http.ResponseWriter, r *http.Request) {
	type request struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}

	var req request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid json body")
		return
	}

	req.Email = strings.TrimSpace(strings.ToLower(req.Email))
	if req.Email == "" || req.Password == "" {
		writeJSONError(w, http.StatusBadRequest, "VALIDATION_ERROR", "email and password are required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), couchbaseCallTimeout)
	defer cancel()

	result, err := a.collection.Get(userEmailKey(req.Email), &gocb.GetOptions{Context: ctx})
	if err != nil {
		writeJSONError(w, http.StatusUnauthorized, "INVALID_CREDENTIALS", "invalid credentials")
		return
	}

	var index userLoginIndex
	if err = result.Content(&index); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to parse user record")
		return
	}

	if err = bcrypt.CompareHashAndPassword([]byte(index.PasswordHash), []byte(req.Password)); err != nil {
		writeJSONError(w, http.StatusUnauthorized, "INVALID_CREDENTIALS", "invalid credentials")
		return
	}

	token, err := generateJWT(a.cfg.JWTSecret, index.UserID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "JWT_ERROR", "failed to issue token")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"user_id": index.UserID, "token": token})
}

func (a *app) handleGenerateAPIKey(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromContext(r.Context())
	apiKey, keyID, hash, err := generateAPIKey(a.cfg.APIKeySalt)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "KEY_GENERATION_ERROR", "failed to generate api key")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), couchbaseCallTimeout)
	defer cancel()

	record := apiKeyRecord{
		KeyID:     keyID,
		UserID:    userID,
		APIKey:    apiKey,
		Hash:      hash,
		Prefix:    apiKey[:16],
		Revoked:   false,
		CreatedAt: time.Now().UTC(),
	}

	if err = a.redisClient.Set(r.Context(), "apikey:"+hash, userID, 0).Err(); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "REDIS_ERROR", "failed to store api key")
		return
	}

	if _, err = a.collection.Insert("apikey:::"+keyID, record, &gocb.InsertOptions{Context: ctx}); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to persist api key")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{"key_id": keyID, "api_key": apiKey})
}

func (a *app) handleRevokeAPIKey(w http.ResponseWriter, r *http.Request) {
	keyID := chi.URLParam(r, "key_id")
	if strings.TrimSpace(keyID) == "" {
		writeJSONError(w, http.StatusBadRequest, "VALIDATION_ERROR", "key_id is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), couchbaseCallTimeout)
	defer cancel()

	key := "apikey:::" + keyID
	result, err := a.collection.Get(key, &gocb.GetOptions{Context: ctx})
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "NOT_FOUND", "api key not found")
		return
	}

	var record apiKeyRecord
	if err = result.Content(&record); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to parse api key")
		return
	}

	if record.UserID != userIDFromContext(r.Context()) {
		writeJSONError(w, http.StatusForbidden, "FORBIDDEN", "api key does not belong to user")
		return
	}

	record.Revoked = true
	record.RevokedAt = time.Now().UTC()
	if _, err = a.collection.Upsert(key, record, &gocb.UpsertOptions{Context: ctx}); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to revoke api key")
		return
	}

	if err = a.redisClient.Del(r.Context(), "apikey:"+record.Hash).Err(); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "REDIS_ERROR", "failed to revoke api key")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "revoked"})
}

func (a *app) handleListAPIKeys(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), couchbaseCallTimeout)
	defer cancel()

	statement := fmt.Sprintf("SELECT key_id, user_id, prefix, revoked, created_at, revoked_at FROM `%s` WHERE META().id LIKE 'apikey:::%%' AND user_id = $uid ORDER BY created_at DESC", a.cfg.CouchbaseBucket)
	result, err := a.cluster.Query(statement, &gocb.QueryOptions{
		Context:         ctx,
		NamedParameters: map[string]any{"uid": userIDFromContext(r.Context())},
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to list api keys")
		return
	}
	defer result.Close()

	rows := make([]map[string]any, 0)
	for result.Next() {
		row := map[string]any{}
		if err = result.Row(&row); err != nil {
			writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to parse api key")
			return
		}
		rows = append(rows, row)
	}

	if err = result.Err(); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to iterate api keys")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"data": rows})
}

func (a *app) handleWalletBalance(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), grpcCallTimeout)
	defer cancel()

	resp, err := a.walletClient.CheckBalance(ctx, &walletpb.BalanceRequest{UserId: userIDFromContext(r.Context())})
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "WALLET_UNAVAILABLE", "wallet check failed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"user_id":              resp.GetUserId(),
		"balance":              resp.GetBalance(),
		"has_sufficient_funds": resp.GetHasSufficientFunds(),
	})
}

func (a *app) handleData(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"message":    "sample response",
		"request_id": requestIDFromContext(r.Context()),
		"timestamp":  time.Now().UTC(),
	})
}

func (a *app) handleCompute(w http.ResponseWriter, r *http.Request) {
	time.Sleep(150 * time.Millisecond)
	writeJSON(w, http.StatusOK, map[string]any{
		"result":     "compute complete",
		"request_id": requestIDFromContext(r.Context()),
		"timestamp":  time.Now().UTC(),
	})
}

func (a *app) handleUsageSummary(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromContext(r.Context())
	rows, err := a.fetchUsageRows(r.Context(), userID, 1000, 0, "", "", "")
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to fetch usage summary")
		return
	}

	today := time.Now().UTC().Format("2006-01-02")
	todayCost := 0
	todayRequests := 0
	totalCost := 0

	chartMap := map[string]int{}
	for i := 6; i >= 0; i-- {
		day := time.Now().UTC().AddDate(0, 0, -i).Format("2006-01-02")
		chartMap[day] = 0
	}

	for _, row := range rows {
		day := row.Timestamp.UTC().Format("2006-01-02")
		totalCost += row.Cost
		chartMap[day] += row.Cost
		if day == today {
			todayCost += row.Cost
			todayRequests++
		}
	}

	type chartPoint struct {
		Date string `json:"date"`
		Cost int    `json:"cost"`
	}

	chartData := make([]chartPoint, 0, 7)
	for i := 6; i >= 0; i-- {
		day := time.Now().UTC().AddDate(0, 0, -i).Format("2006-01-02")
		chartData = append(chartData, chartPoint{Date: day, Cost: chartMap[day]})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"today_cost":      todayCost,
		"total_cost":      totalCost,
		"today_requests":  todayRequests,
		"chart_data":      chartData,
		"summary_user_id": userID,
	})
}

func (a *app) handleUsageLogs(w http.ResponseWriter, r *http.Request) {
	page := parsePositiveInt(r.URL.Query().Get("page"), 1)
	limit := parsePositiveInt(r.URL.Query().Get("limit"), 50)
	offset := (page - 1) * limit
	endpoint := strings.TrimSpace(r.URL.Query().Get("endpoint"))
	startDate := strings.TrimSpace(r.URL.Query().Get("start_date"))
	endDate := strings.TrimSpace(r.URL.Query().Get("end_date"))

	rows, err := a.fetchUsageRows(r.Context(), userIDFromContext(r.Context()), limit, offset, endpoint, startDate, endDate)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to fetch usage logs")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"page":  page,
		"limit": limit,
		"data":  rows,
	})
}

func (a *app) fetchUsageRows(parent context.Context, userID string, limit int, offset int, endpoint string, startDate string, endDate string) ([]usageLogRecord, error) {
	ctx, cancel := context.WithTimeout(parent, couchbaseCallTimeout)
	defer cancel()

	base := fmt.Sprintf("SELECT user_id, endpoint, method, cost, request_id, status_code, latency_ms, timestamp FROM `%s` WHERE META().id LIKE $pattern", a.cfg.CouchbaseBucket)
	named := map[string]any{
		"pattern": "usage:::" + userID + ":::%",
		"limit":   limit,
		"offset":  offset,
	}

	if endpoint != "" {
		base += " AND CONTAINS(LOWER(endpoint), LOWER($endpoint))"
		named["endpoint"] = endpoint
	}
	if startDate != "" {
		start, err := time.Parse("2006-01-02", startDate)
		if err != nil {
			return nil, fmt.Errorf("invalid start_date: %w", err)
		}

		base += " AND STR_TO_MILLIS(timestamp) >= STR_TO_MILLIS($start_date)"
		named["start_date"] = start.UTC().Format(time.RFC3339)
	}
	if endDate != "" {
		end, err := time.Parse("2006-01-02", endDate)
		if err != nil {
			return nil, fmt.Errorf("invalid end_date: %w", err)
		}

		base += " AND STR_TO_MILLIS(timestamp) < STR_TO_MILLIS($end_date)"
		named["end_date"] = end.AddDate(0, 0, 1).UTC().Format(time.RFC3339)
	}

	statement := base + " ORDER BY timestamp DESC LIMIT $limit OFFSET $offset"

	result, err := a.cluster.Query(statement, &gocb.QueryOptions{
		Context:         ctx,
		NamedParameters: named,
	})
	if err != nil {
		return nil, fmt.Errorf("query usage logs: %w", err)
	}
	defer result.Close()

	rows := make([]usageLogRecord, 0)
	for result.Next() {
		var row usageLogRecord
		if err = result.Row(&row); err != nil {
			return nil, fmt.Errorf("decode usage row: %w", err)
		}
		rows = append(rows, row)
	}

	if err = result.Err(); err != nil {
		return nil, fmt.Errorf("iterate usage rows: %w", err)
	}

	return rows, nil
}

func generateJWT(secret string, userID string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": userID,
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(jwtExpirationHours * time.Hour).Unix(),
	})

	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("sign token: %w", err)
	}

	return signed, nil
}

func generateAPIKey(salt string) (string, string, string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", "", fmt.Errorf("random key bytes: %w", err)
	}

	encoded := base64.RawURLEncoding.EncodeToString(raw)
	apiKey := "sk_live_" + encoded
	keyID := "key_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	hash := hashAPIKey(salt, apiKey)

	return apiKey, keyID, hash, nil
}

func hashAPIKey(salt string, apiKey string) string {
	sum := sha256.Sum256([]byte(salt + ":" + apiKey))
	return hex.EncodeToString(sum[:])
}

func userEmailKey(email string) string {
	sum := sha256.Sum256([]byte(email))
	return "user_email:::" + hex.EncodeToString(sum[:])
}

func requestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(ctxRequestID).(string)
	return id
}

func userIDFromContext(ctx context.Context) string {
	uid, _ := ctx.Value(ctxUserID).(string)
	return uid
}

func convertInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	default:
		return 0, errors.New("unsupported number type")
	}
}

func parsePositiveInt(raw string, defaultValue int) int {
	if strings.TrimSpace(raw) == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return defaultValue
	}

	return value
}

func getEnv(key string, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && strings.TrimSpace(value) != "" {
		return value
	}

	return fallback
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, `{"error":"failed to encode response","code":"ENCODE_ERROR"}`, http.StatusInternalServerError)
	}
}

func writeJSONError(w http.ResponseWriter, statusCode int, code string, message string) {
	writeJSON(w, statusCode, apiError{Error: message, Code: code})
}
