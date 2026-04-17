package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/apibilling/platform/pkg/events"
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

	walletConn, err := grpc.NewClient(cfg.WalletGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
		Amount int `json:"amount"`
	}

	var req request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Amount <= 0 {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "amount must be positive")
		return
	}

	userID, _ := r.Context().Value(userIDContextKey).(string)
	userSuffix := userID
	if len(userSuffix) > 8 {
		userSuffix = userSuffix[len(userSuffix)-8:]
	}
	receipt := fmt.Sprintf("ord_%d_%s", time.Now().Unix(), userSuffix)
	orderData := map[string]interface{}{
		"amount":   req.Amount * 100,
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

	orderDoc := map[string]any{
		"order_id":   orderID,
		"user_id":    userID,
		"amount":     req.Amount,
		"currency":   "INR",
		"status":     "pending",
		"created_at": time.Now().UTC(),
	}
	if _, err = s.collection.Upsert("order:::"+orderID, orderDoc, &gocb.UpsertOptions{Context: ctx}); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "COUCHBASE_ERROR", "failed to store order")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"order_id": orderID, "amount": req.Amount, "currency": "INR", "key_id": s.cfg.RazorpayKeyID})
}

func (s *service) handleWebhook(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Event   string `json:"event"`
		Payload struct {
			Payment struct {
				Entity struct {
					ID      string `json:"id"`
					OrderID string `json:"order_id"`
					Amount  int    `json:"amount"`
				} `json:"entity"`
			} `json:"payment"`
		} `json:"payload"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSONError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid webhook payload")
		return
	}

	orderID := payload.Payload.Payment.Entity.OrderID
	paymentID := payload.Payload.Payment.Entity.ID
	signature := r.Header.Get("X-Razorpay-Signature")
	if !verifyRazorpaySignature(orderID, paymentID, signature, s.cfg.RazorpayWebhookSecret) {
		writeJSONError(w, http.StatusBadRequest, "INVALID_SIGNATURE", "signature verification failed")
		return
	}

	if payload.Event != "payment.captured" {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ignored"})
		return
	}

	if err := s.processCapturedPayment(r.Context(), orderID, paymentID); err != nil {
		switch {
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

	orderResult, err := s.collection.Get("order:::"+orderID, &gocb.GetOptions{Context: ctx})
	if err != nil {
		return err
	}

	var orderDoc map[string]any
	if err = orderResult.Content(&orderDoc); err != nil {
		return err
	}

	userID, _ := orderDoc["user_id"].(string)
	amountFloat, _ := orderDoc["amount"].(float64)
	amount := amountFloat
	if status, _ := orderDoc["status"].(string); status == "completed" {
		return nil
	}

	orderDoc["status"] = "completed"
	orderDoc["payment_id"] = paymentID
	orderDoc["completed_at"] = time.Now().UTC()
	if _, err = s.collection.Upsert("order:::"+orderID, orderDoc, &gocb.UpsertOptions{Context: ctx}); err != nil {
		return err
	}

	grpcCtx, grpcCancel := context.WithTimeout(context.Background(), grpcCallTimeout)
	defer grpcCancel()
	creditResp, err := s.walletClient.CreditBalance(grpcCtx, &walletpb.CreditRequest{
		UserId:         userID,
		Amount:         amount,
		IdempotencyKey: "payment:" + paymentID,
	})
	if err != nil || !creditResp.GetSuccess() {
		if err != nil {
			return fmt.Errorf("wallet: %w", err)
		}
		return fmt.Errorf("wallet: credit response unsuccessful")
	}

	evt := events.PaymentSuccessEvent{
		UserID:            userID,
		Amount:            amount,
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
