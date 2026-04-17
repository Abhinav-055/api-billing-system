SHELL := /bin/sh

GO_MODULES := \
	pkg \
	proto \
	services/api-gateway \
	services/billing-service \
	services/wallet-service \
	services/payment-service

.PHONY: build up down proto test lint integration-test

build:
	@for svc in $(GO_MODULES); do \
		echo "Building $$svc"; \
		(cd $$svc && go build ./...); \
	done

up:
	docker compose up --build -d

down:
	docker compose down -v

proto:
	protoc \
		--proto_path=proto \
		--go_out=. \
		--go-grpc_out=. \
		proto/wallet.proto

test:
	@for svc in $(GO_MODULES); do \
		echo "Testing $$svc"; \
		(cd $$svc && go test ./...); \
	done

lint:
	@for svc in $(GO_MODULES); do \
		echo "Linting $$svc"; \
		(cd $$svc && golangci-lint run ./...); \
	done

integration-test:
	@echo "Running integration tests (requires docker compose up)"
	@(cd services/api-gateway && INTEGRATION_TEST=true INTEGRATION_BASE_URL=http://localhost go test ./...)
