package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	sharedconfig "github.com/apibilling/platform/pkg/config"
	"github.com/apibilling/platform/pkg/events"
	"github.com/couchbase/gocb/v2"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	serviceName          = "billing-service"
	serviceVersion       = "1.0.0"
	couchbaseCallTimeout = 10 * time.Second
)

type config struct {
	Port              string
	NATSURL           string
	CouchbaseHost     string
	CouchbaseUsername string
	CouchbasePassword string
	CouchbaseBucket   string
	GRPCSecret        string
}

type usageLogDocument struct {
	UserID     string    `json:"user_id"`
	Endpoint   string    `json:"endpoint"`
	Method     string    `json:"method"`
	Cost       int       `json:"cost"`
	RequestID  string    `json:"request_id"`
	StatusCode int       `json:"status_code"`
	LatencyMS  int64     `json:"latency_ms"`
	Timestamp  time.Time `json:"timestamp"`
}

type service struct {
	logger     *slog.Logger
	cluster    *gocb.Cluster
	collection *gocb.Collection
	publisher  *events.Publisher
	js         jetstream.JetStream
	startedAt  time.Time
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()
	if err := sharedconfig.RequireSecrets(logger, map[string]string{
		"COUCHBASE_PASSWORD": cfg.CouchbasePassword,
		"GRPC_SHARED_SECRET": cfg.GRPCSecret,
	}); err != nil {
		logger.Error("startup secret validation failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	svc, err := newService(cfg, logger)
	if err != nil {
		logger.Error("failed to initialize billing service", slog.String("service", serviceName), slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer svc.close()

	go svc.startHealthServer(cfg.Port)
	if err = svc.runConsumer(); err != nil {
		logger.Error("billing consumer failed", slog.String("service", serviceName), slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func loadConfig() config {
	return config{
		Port:              getEnv("BILLING_PORT", "8082"),
		NATSURL:           getEnv("NATS_URL", "nats://localhost:4222"),
		CouchbaseHost:     getEnv("COUCHBASE_HOST", "couchbase://localhost"),
		CouchbaseUsername: getEnv("COUCHBASE_USERNAME", "Administrator"),
		CouchbasePassword: getEnv("COUCHBASE_PASSWORD", "password"),
		CouchbaseBucket:   getEnv("COUCHBASE_BUCKET", "apibilling"),
		GRPCSecret:        getEnv("GRPC_SHARED_SECRET", "dev-grpc-secret"),
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

	return &service{
		logger:     logger,
		cluster:    cluster,
		collection: bucket.DefaultCollection(),
		publisher:  events.NewPublisher(js),
		js:         js,
		startedAt:  time.Now().UTC(),
	}, nil
}

func (s *service) close() {
	if s.cluster != nil {
		s.cluster.Close(nil)
	}
}

func (s *service) runConsumer() error {
	consumer, err := s.js.CreateOrUpdateConsumer(context.Background(), "USAGE", jetstream.ConsumerConfig{
		Durable:       "billing-processor",
		FilterSubject: events.SubjectUsageLogged,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    5,
	})
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}

	go s.logPendingLoop(consumer)

	_, err = consumer.Consume(func(msg jetstream.Msg) {
		if consumeErr := s.handleUsageMessage(msg); consumeErr != nil {
			meta, _ := msg.Metadata()
			if meta != nil && meta.NumDelivered >= 5 {
				_, _ = fmt.Fprintf(os.Stderr, "dropping poisoned message after retries: %v\n", consumeErr)
				_ = msg.Term()
				return
			}
			_ = msg.Nak()
			return
		}

		_ = msg.Ack()
	})
	if err != nil {
		return fmt.Errorf("consume usage stream: %w", err)
	}

	select {}
}

func (s *service) handleUsageMessage(msg jetstream.Msg) error {
	var event events.UsageEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		return fmt.Errorf("decode usage event: %w", err)
	}

	cost := calculateCost(event.Endpoint)
	if err := s.persistUsage(event, cost); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	captureEvent := events.WalletCaptureEvent{
		UserID:         event.UserID,
		ReservationID:  event.RequestID,
		ActualAmount:   int64(cost),
		Currency:       "INR",
		IdempotencyKey: "usage:" + event.RequestID,
		Timestamp:      time.Now().UTC(),
	}
	if err := s.publisher.PublishWalletCapture(ctx, captureEvent); err != nil {
		return fmt.Errorf("publish wallet capture event: %w", err)
	}

	return nil
}

func (s *service) persistUsage(evt events.UsageEvent, cost int) error {
	ctx, cancel := context.WithTimeout(context.Background(), couchbaseCallTimeout)
	defer cancel()

	key := fmt.Sprintf("usage:::%s:::%d:::%s", evt.UserID, evt.Timestamp.UTC().UnixNano(), evt.RequestID)
	doc := usageLogDocument{
		UserID:     evt.UserID,
		Endpoint:   evt.Endpoint,
		Method:     evt.Method,
		Cost:       cost,
		RequestID:  evt.RequestID,
		StatusCode: evt.StatusCode,
		LatencyMS:  evt.LatencyMs,
		Timestamp:  evt.Timestamp.UTC(),
	}

	if _, err := s.collection.Upsert(key, doc, &gocb.UpsertOptions{Context: ctx}); err != nil {
		return fmt.Errorf("persist usage doc: %w", err)
	}

	return nil
}

func (s *service) logPendingLoop(consumer jetstream.Consumer) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		info, err := consumer.Info(context.Background())
		if err != nil {
			s.logger.Error("failed to fetch consumer info", slog.String("service", serviceName), slog.String("error", err.Error()))
			continue
		}

		s.logger.Info("consumer lag",
			slog.String("service", serviceName),
			slog.String("consumer", info.Name),
			slog.Uint64("pending", info.NumPending),
		)
	}
}

func (s *service) startHealthServer(port string) {
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

func calculateCost(endpoint string) int {
	switch strings.TrimSpace(endpoint) {
	case "/api/v1/data":
		return 1
	case "/api/v1/compute":
		return 3
	default:
		return 2
	}
}

func getEnv(key string, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}
