package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func Connect(url string) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, nil, fmt.Errorf("connect nats: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("create jetstream context: %w", err)
	}

	return nc, js, nil
}

func SetupStreams(js jetstream.JetStream) error {
	ctx := context.Background()

	streams := []jetstream.StreamConfig{
		{
			Name:      "USAGE",
			Subjects:  []string{SubjectUsageLogged},
			Retention: jetstream.WorkQueuePolicy,
			Replicas:  1,
		},
		{
			Name:      "BILLING",
			Subjects:  []string{SubjectBillingProcess},
			Retention: jetstream.WorkQueuePolicy,
			Replicas:  1,
		},
		{
			Name:      "WALLET",
			Subjects:  []string{SubjectWalletDeduct},
			Retention: jetstream.WorkQueuePolicy,
			Replicas:  1,
		},
		{
			Name:      "PAYMENT",
			Subjects:  []string{SubjectPaymentSuccess},
			Retention: jetstream.WorkQueuePolicy,
			Replicas:  1,
		},
	}

	for _, cfg := range streams {
		_, err := js.Stream(ctx, cfg.Name)
		if err == nil {
			continue
		}

		if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("check stream %s: %w", cfg.Name, err)
		}

		if _, createErr := js.CreateStream(ctx, cfg); createErr != nil {
			return fmt.Errorf("create stream %s: %w", cfg.Name, createErr)
		}
	}

	return nil
}

type Publisher struct {
	js jetstream.JetStream
}

func NewPublisher(js jetstream.JetStream) *Publisher {
	return &Publisher{js: js}
}

func (p *Publisher) PublishUsage(ctx context.Context, evt UsageEvent) error {
	return p.publish(ctx, SubjectUsageLogged, evt)
}

func (p *Publisher) PublishWalletDeduct(ctx context.Context, evt WalletDeductEvent) error {
	return p.publish(ctx, SubjectWalletDeduct, evt)
}

func (p *Publisher) PublishPaymentSuccess(ctx context.Context, evt PaymentSuccessEvent) error {
	return p.publish(ctx, SubjectPaymentSuccess, evt)
}

func (p *Publisher) publish(ctx context.Context, subject string, evt any) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if _, err = p.js.Publish(ctx, subject, payload); err != nil {
		return fmt.Errorf("publish %s: %w", subject, err)
	}

	return nil
}
