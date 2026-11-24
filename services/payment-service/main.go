package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"github.com/razorpay/razorpay-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	serviceName          = "payment-service"
	serviceVersion       = "1.0.0"
	couchbaseCallTimeout = 10 * time.Second
	grpcCallTimeout      = 5 * time.Second
)

type contextKey string

const userIDContextKey contextKey = "user_id"
const requestIDContextKey contextKey = "request_id"

type config struct {
	Port                  string
	JWTSecret             string
	NATSURL               string
	WalletGRPCAddr        string
	RazorpayKeyID         string
	RazorpayKeySecret     string
	RazorpayWebhookSecret string
	CouchbaseHost         string
	CouchbaseUsername     string
	CouchbasePassword     string
	CouchbaseBucket       string
	GRPCSecret            string
	MinAmountMinor        int64
	MaxAmountMinor        int64
	ProcessingTimeout     time.Duration
}

type orderDoc struct {
	OrderID             string    `json:"order_id"`
	UserID              string    `json:"user_id"`
	AmountMinor         int64     `json:"amount_minor"`
	Currency            string    `json:"currency"`
	Status              string    `json:"status"`
	PaymentID           string    `json:"payment_id,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	ProcessingStartedAt time.Time `json:"processing_started_at,omitempty"`
	CompletedAt         time.Time `json:"completed_at,omitempty"`
}

func decodeOrderResult(result *gocb.GetResult) (orderDoc, error) {
	var order orderDoc
	if err := result.Content(&order); err == nil {
		return order, nil
	}
	var raw map[string]any
	if err := result.Content(&raw); err != nil {
		return orderDoc{}, err
	}
	order.OrderID, _ = raw["order_id"].(string)
	order.UserID, _ = raw["user_id"].(string)
	order.Currency, _ = raw["currency"].(string)
	order.Status, _ = raw["status"].(string)
	if value, ok := raw["amount_minor"].(float64); ok {
		order.AmountMinor = int64(math.Round(value))
	} else if value, ok := raw["amount"].(float64); ok {
		order.AmountMinor = int64(math.Round(value * 100))
	}
	if order.Currency == "" {
		order.Currency = "INR"
	}
	return order, nil
}

type service struct {
	logger       *slog.Logger
	collection   *gocb.Collection
	cluster      *gocb.Cluster
	publisher    *events.Publisher
	walletClient walletpb.WalletServiceClient
	jwtSecret    string
	rzpClient    *razorpay.Client
	cfg          config
	startedAt    time.Time
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
		"JWT_SECRET":              cfg.JWTSecret,
		"RAZORPAY_KEY_ID":         cfg.RazorpayKeyID,
		"RAZORPAY_KEY_SECRET":     cfg.RazorpayKeySecret,
		"RAZORPAY_WEBHOOK_SECRET": cfg.RazorpayWebhookSecret,
		"COUCHBASE_PASSWORD":      cfg.CouchbasePassword,
		"GRPC_SHARED_SECRET":      cfg.GRPCSecret,
	}); err != nil {
		logger.Error("startup secret validation failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	svc, err := newService(cfg, logger)
	if err != nil {
		logger.Error("failed to initialize payment service", slog.String("service", serviceName), slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer svc.close()

	r := chi.NewRouter()
	r.Use(svc.panicRecoveryMiddleware)
	r.Use(svc.requestIDMiddleware)
	r.Use(svc.structuredLoggingMiddleware)
	r.Get("/health", svc.handleHealth)
	r.Post("/payments/webhook", svc.handleWebhook)

	r.Group(func(auth chi.Router) {
		auth.Use(svc.authMiddleware)
		auth.Post("/payments/create-order", svc.handleCreateOrder)
		auth.Post("/payments/confirm", svc.handleConfirmPayment)
		auth.Get("/payments/transactions", svc.handleTransactions)
	})

	addr := ":" + cfg.Port
	logger.Info("payment service started", slog.String("service", serviceName), slog.String("addr", addr))
	if err = http.ListenAndServe(addr, r); err != nil {
		logger.Error("payment server failed", slog.String("service", serviceName), slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func (s *service) panicRecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.logger.Error("panic recovered",
					slog.String("service", serviceName),
					slog.String("request_id", requestIDFromContext(r.Context())),
					slog.Any("panic", recovered),
					slog.String("stack", string(debug.Stack())),
				)
				writeJSONError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "internal server error")
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func (s *service) requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := uuid.NewString()
		w.Header().Set("X-Request-ID", requestID)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDContextKey, requestID)))
	})
}

func (s *service) structuredLoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		s.logger.Info("http request",
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

func loadConfig() config {
	grpcPort := getEnv("WALLET_GRPC_PORT", "9090")
	return config{
		Port:                  getEnv("PAYMENT_PORT", "8083"),
		JWTSecret:             getEnv("JWT_SECRET", "dev-secret"),
		NATSURL:               getEnv("NATS_URL", "nats://localhost:4222"),
		WalletGRPCAddr:        "wallet-service:" + grpcPort,
		RazorpayKeyID:         getEnv("RAZORPAY_KEY_ID", ""),
		RazorpayKeySecret:     getEnv("RAZORPAY_KEY_SECRET", ""),
		RazorpayWebhookSecret: getEnv("RAZORPAY_WEBHOOK_SECRET", ""),
		CouchbaseHost:         getEnv("COUCHBASE_HOST", "couchbase://localhost"),
		CouchbaseUsername:     getEnv("COUCHBASE_USERNAME", "Administrator"),
		CouchbasePassword:     getEnv("COUCHBASE_PASSWORD", "password"),
		CouchbaseBucket:       getEnv("COUCHBASE_BUCKET", "apibilling"),
		GRPCSecret:            getEnv("GRPC_SHARED_SECRET", "dev-grpc-secret"),
		MinAmountMinor:        parseInt64Env("MIN_ORDER_AMOUNT_MINOR", 100),
		MaxAmountMinor:        parseInt64Env("MAX_ORDER_AMOUNT_MINOR", 10000000),
		ProcessingTimeout:     parseDurationEnv("PAYMENT_PROCESSING_TIMEOUT", 60*time.Second),
	}
}

func newService(cfg config, logger *slog.Logger) (*service, error) {
	nc, js, err := events.Connect(cfg.NATSURL)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}

	if err = events.SetupStreams(js); err != nil {
		nc.Close()
		return nil, fmt.Errorf("setup streams: %w", err)
	}

	cluster, err := gocb.Connect(cfg.CouchbaseHost, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{Username: cfg.CouchbaseUsername, Password: cfg.CouchbasePassword},
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("connect couchbase: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), couchbaseCallTimeout)
	defer cancel()
	bucket := cluster.Bucket(cfg.CouchbaseBucket)
	if err = bucket.WaitUntilReady(couchbaseCallTimeout, &gocb.WaitUntilReadyOptions{Context: ctx}); err != nil {
		nc.Close()
		cluster.Close(nil)
		return nil, fmt.Errorf("wait bucket ready: %w", err)
	}

	walletConn, err := grpc.NewClient(cfg.WalletGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(grpcauth.UnaryClientInterceptor(cfg.GRPCSecret)),
	)
	if err != nil {
		nc.Close()
		cluster.Close(nil)
		return nil, fmt.Errorf("connect wallet grpc: %w", err)
	}

	return &service{
		logger:       logger,
		collection:   bucket.DefaultCollection(),
		cluster:      cluster,
		publisher:    events.NewPublisher(js),
		walletClient: walletpb.NewWalletServiceClient(walletConn),
		jwtSecret:    cfg.JWTSecret,
		rzpClient:    razorpay.NewClient(cfg.RazorpayKeyID, cfg.RazorpayKeySecret),
		cfg:          cfg,
		startedAt:    time.Now().UTC(),
	}, nil
}

func (s *service) close() {
	if s.cluster != nil {
		s.cluster.Close(nil)
	}
}

func (s *service) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			writeJSONError(w, http.StatusUnauthorized, "UNAUTHORIZED", "missing bearer token")
			return
		}

		tokenString := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer"))
		claims := jwt.MapClaims{}
		token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("invalid signing method")
			}
			return []byte(s.jwtSecret), nil
		})
		if err != nil || !token.Valid {
			writeJSONError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid bearer token")
			return
		}

		userID, _ := claims["sub"].(string)
		if strings.TrimSpace(userID) == "" {
			writeJSONError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid token claims")
			return
		}

		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userIDContextKey, userID)))
	})
}

func (s *service) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	type request struct {
		AmountMinor int64 `json:"amount_minor"`
		Amount      int64 `json:"amount"`
	}

	var req request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid json body")
		return
	}
	amountMinor := req.AmountMinor
	if amountMinor == 0 {
		if req.Amount <= 0 || req.Amount > s.cfg.MaxAmountMinor/100 {
			writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "amount out of range")
			return
		}
		amountMinor = req.Amount * 100
	}
	if amountMinor < s.cfg.MinAmountMinor || amountMinor > s.cfg.MaxAmountMinor {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "amount out of range")
		return
	}

	userID, _ := r.Context().Value(userIDContextKey).(string)
	userSuffix := userID
	if len(userSuffix) > 8 {
		userSuffix = userSuffix[len(userSuffix)-8:]
	}
	receipt := fmt.Sprintf("ord_%d_%s", time.Now().Unix(), userSuffix)
	orderData := map[string]interface{}{
		"amount":   amountMinor,
		"currency": "INR",
		"receipt":  receipt,
	}

	body, err := s.rzpClient.Order.Create(orderData, nil)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "RAZORPAY_ERROR", "failed to create order")
		return
	}

	orderID, _ := body["id"].(string)
	if strings.TrimSpace(orderID) == "" {
		writeJSONError(w, http.StatusBadGateway, "RAZORPAY_ERROR", "missing order id")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), couchbaseCallTimeout)
	defer cancel()

	order := orderDoc{
		OrderID:     orderID,
		UserID:      userID,
		AmountMinor: amountMinor,
		Currency:    "INR",
		Status:      "pending",
		CreatedAt:   time.Now().UTC(),
	}
	if _, err = s.collection.Insert("order:::"+orderID, order, &gocb.InsertOptions{Context: ctx}); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to store order")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"order_id": orderID, "amount": amountMinor, "amount_minor": amountMinor, "currency": "INR", "key_id": s.cfg.RazorpayKeyID})
}

func (s *service) handleWebhook(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid webhook payload")
		return
	}
	signature := r.Header.Get("X-Razorpay-Signature")
	if !verifyWebhookSignature(rawBody, signature, s.cfg.RazorpayWebhookSecret) {
		writeJSONError(w, http.StatusUnauthorized, "INVALID_SIGNATURE", "signature verification failed")
		return
	}
	var payload struct {
		Event   string `json:"event"`
		Payload struct {
			Payment struct {
				Entity struct {
					ID       string `json:"id"`
					OrderID  string `json:"order_id"`
					Amount   int64  `json:"amount"`
					Currency string `json:"currency"`
				} `json:"entity"`
			} `json:"payment"`
		} `json:"payload"`
	}

	if err := json.Unmarshal(rawBody, &payload); err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid webhook payload")
		return
	}

	orderID := payload.Payload.Payment.Entity.OrderID
	paymentID := payload.Payload.Payment.Entity.ID
	if payload.Event != "payment.captured" {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ignored"})
		return
	}

	orderCtx, orderCancel := context.WithTimeout(r.Context(), couchbaseCallTimeout)
	defer orderCancel()
	orderRes, err := s.collection.Get("order:::"+orderID, &gocb.GetOptions{Context: orderCtx})
	if errors.Is(err, gocb.ErrDocumentNotFound) {
		writeJSONError(w, http.StatusBadRequest, "ORDER_NOT_FOUND", "order not found")
		return
	}
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to load order")
		return
	}
	order, err := decodeOrderResult(orderRes)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to decode order")
		return
	}
	entity := payload.Payload.Payment.Entity
	if entity.Amount != order.AmountMinor || entity.Currency != order.Currency {
		s.logger.Error("payment webhook amount/currency mismatch",
			slog.String("order_id", orderID),
			slog.String("payment_id", paymentID),
			slog.Int64("captured_amount", entity.Amount),
			slog.Int64("order_amount", order.AmountMinor),
			slog.String("captured_currency", entity.Currency),
			slog.String("order_currency", order.Currency))
		writeJSONError(w, http.StatusBadRequest, "PAYMENT_MISMATCH", "captured amount or currency mismatch")
		return
	}

	if err := s.processCapturedPayment(r.Context(), orderID, paymentID); err != nil {
		switch {
		case strings.Contains(err.Error(), "PAYMENT_PROCESSING_CONFLICT"):
			writeJSONError(w, http.StatusConflict, "PAYMENT_PROCESSING", "payment is already being processed")
		case errors.Is(err, gocb.ErrDocumentNotFound):
			writeJSONError(w, http.StatusNotFound, "ORDER_NOT_FOUND", "order not found")
		case strings.HasPrefix(err.Error(), "wallet:"):
			writeJSONError(w, http.StatusBadGateway, "WALLET_ERROR", "failed to credit wallet")
		case strings.HasPrefix(err.Error(), "nats:"):
			writeJSONError(w, http.StatusBadGateway, "NATS_ERROR", "failed to publish payment success")
		default:
			writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to process payment")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "processed"})
}

func (s *service) handleConfirmPayment(w http.ResponseWriter, r *http.Request) {
	type request struct {
		RazorpayOrderID   string `json:"razorpay_order_id"`
		RazorpayPaymentID string `json:"razorpay_payment_id"`
		RazorpaySignature string `json:"razorpay_signature"`
	}

	var req request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid json body")
		return
	}

	req.RazorpayOrderID = strings.TrimSpace(req.RazorpayOrderID)
	req.RazorpayPaymentID = strings.TrimSpace(req.RazorpayPaymentID)
	req.RazorpaySignature = strings.TrimSpace(req.RazorpaySignature)
	if req.RazorpayOrderID == "" || req.RazorpayPaymentID == "" || req.RazorpaySignature == "" {
		writeJSONError(w, http.StatusBadRequest, "VALIDATION_ERROR", "order id, payment id, and signature are required")
		return
	}

	if !verifyRazorpaySignature(req.RazorpayOrderID, req.RazorpayPaymentID, req.RazorpaySignature, s.cfg.RazorpayKeySecret) {
		writeJSONError(w, http.StatusBadRequest, "INVALID_SIGNATURE", "signature verification failed")
		return
	}

	if err := s.processCapturedPayment(r.Context(), req.RazorpayOrderID, req.RazorpayPaymentID); err != nil {
		switch {
		case strings.Contains(err.Error(), "PAYMENT_PROCESSING_CONFLICT"):
			writeJSONError(w, http.StatusConflict, "PAYMENT_PROCESSING", "payment is already being processed")
		case errors.Is(err, gocb.ErrDocumentNotFound):
			writeJSONError(w, http.StatusNotFound, "ORDER_NOT_FOUND", "order not found")
		case strings.HasPrefix(err.Error(), "wallet:"):
			writeJSONError(w, http.StatusBadGateway, "WALLET_ERROR", "failed to credit wallet")
		case strings.HasPrefix(err.Error(), "nats:"):
			writeJSONError(w, http.StatusBadGateway, "NATS_ERROR", "failed to publish payment success")
		default:
			writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to process payment")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "processed"})
}

func (s *service) processCapturedPayment(reqCtx context.Context, orderID, paymentID string) error {
	ctx, cancel := context.WithTimeout(reqCtx, couchbaseCallTimeout)
	defer cancel()
	key := "order:::" + orderID
	orderResult, err := s.collection.Get(key, &gocb.GetOptions{Context: ctx})
	if err != nil {
		return err
	}
	order, err := decodeOrderResult(orderResult)
	if err != nil {
		return err
	}
	if order.Status == "completed" {
		return nil
	}
	if order.Currency == "" {
		order.Currency = "INR"
	}
	if order.Status == "processing" {
		stale := !order.ProcessingStartedAt.IsZero() && time.Since(order.ProcessingStartedAt) > s.cfg.ProcessingTimeout
		samePayment := order.PaymentID == paymentID
		if !stale && !samePayment {
			return fmt.Errorf("PAYMENT_PROCESSING_CONFLICT")
		}
	} else if order.Status == "pending" {
		order.Status = "processing"
		order.PaymentID = paymentID
		order.ProcessingStartedAt = time.Now().UTC()
		if _, err = s.collection.Replace(key, order, &gocb.ReplaceOptions{Cas: orderResult.Cas(), Context: ctx}); err != nil {
			if errors.Is(err, gocb.ErrCasMismatch) {
				return fmt.Errorf("PAYMENT_PROCESSING_CONFLICT")
			}
			return err
		}
	} else {
		return fmt.Errorf("invalid order state %s", order.Status)
	}
	grpcCtx, grpcCancel := context.WithTimeout(context.Background(), grpcCallTimeout)
	defer grpcCancel()
	creditUnits := order.AmountMinor / 100
	if creditUnits <= 0 {
		return fmt.Errorf("wallet: invalid credit amount")
	}
	creditResp, err := s.walletClient.CreditBalance(grpcCtx, &walletpb.CreditRequest{
		UserId:         order.UserID,
		AmountMinor:    creditUnits,
		Currency:       order.Currency,
		IdempotencyKey: "payment:" + paymentID,
	})
	if err != nil || !creditResp.GetSuccess() {
		if err != nil {
			return fmt.Errorf("wallet: %w", err)
		}
		return fmt.Errorf("wallet: credit response unsuccessful")
	}
	current, err := s.collection.Get(key, &gocb.GetOptions{Context: ctx})
	if err != nil {
		return err
	}
	if err = current.Content(&order); err != nil {
		return err
	}
	order.Status = "completed"
	order.CompletedAt = time.Now().UTC()
	if _, err = s.collection.Replace(key, order, &gocb.ReplaceOptions{Cas: current.Cas(), Context: ctx}); err != nil {
		return err
	}
	evt := events.PaymentSuccessEvent{
		UserID:            order.UserID,
		AmountMinor:       order.AmountMinor,
		Currency:          order.Currency,
		RazorpayOrderID:   orderID,
		RazorpayPaymentID: paymentID,
		Timestamp:         time.Now().UTC(),
	}
	publishCtx, publishCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer publishCancel()
	if err = s.publisher.PublishPaymentSuccess(publishCtx, evt); err != nil {
		return fmt.Errorf("nats: %w", err)
	}

	_ = creditResp.GetNewBalance()

	return nil
}

func (s *service) handleTransactions(w http.ResponseWriter, r *http.Request) {
	userID, _ := r.Context().Value(userIDContextKey).(string)
	ctx, cancel := context.WithTimeout(r.Context(), couchbaseCallTimeout)
	defer cancel()

	statement := fmt.Sprintf("SELECT tx_id, user_id, type, amount, balance_after, reference, status, created_at FROM `%s` WHERE META().id LIKE 'transaction:::%%' AND user_id = $uid ORDER BY created_at DESC LIMIT 100", s.cfg.CouchbaseBucket)
	result, err := s.cluster.Query(statement, &gocb.QueryOptions{Context: ctx, NamedParameters: map[string]any{"uid": userID}})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to query transactions")
		return
	}
	defer result.Close()

	rows := make([]map[string]any, 0)
	for result.Next() {
		row := map[string]any{}
		if err = result.Row(&row); err != nil {
			writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to parse transaction")
			return
		}
		rows = append(rows, row)
	}

	writeJSON(w, http.StatusOK, map[string]any{"data": rows})
}

func (s *service) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"service":        serviceName,
		"version":        serviceVersion,
		"uptime_seconds": int(time.Since(s.startedAt).Seconds()),
	})
}

func verifyRazorpaySignature(orderID string, paymentID string, signature string, secret string) bool {
	if strings.TrimSpace(signature) == "" || strings.TrimSpace(secret) == "" {
		return false
	}

	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(orderID + "|" + paymentID))
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(signature))
}

func verifyWebhookSignature(rawBody []byte, signature, secret string) bool {
	if strings.TrimSpace(signature) == "" || strings.TrimSpace(secret) == "" {
		return false
	}
	provided, err := hex.DecodeString(strings.TrimSpace(signature))
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(rawBody)
	return hmac.Equal(mac.Sum(nil), provided)
}

func requestIDFromContext(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDContextKey).(string)
	return requestID
}

func userIDFromContext(ctx context.Context) string {
	userID, _ := ctx.Value(userIDContextKey).(string)
	return userID
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeJSONError(w http.ResponseWriter, statusCode int, code string, message string) {
	writeJSON(w, statusCode, apiError{Error: message, Code: code})
}

func getEnv(key string, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}

func parseInt64Env(key string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return fallback
	}
	return parsed
}

func parseDurationEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
