# API Billing + Wallet Platform

A production-style API monetization and wallet platform built as a Go microservices monorepo with a Next.js dashboard.

The platform gives you:
1. JWT auth and API key lifecycle management
2. Redis-backed rate limiting
3. Usage metering and event-driven billing with NATS JetStream
4. Wallet balance tracking and atomic debit/credit via gRPC
5. Razorpay payment order creation and payment confirmation flow
6. Couchbase persistence for users, keys, usage, orders, and transactions
7. Frontend control plane for dashboard, wallet, keys, and logs

## Project Overview

Core services:
1. api-gateway (Go + Chi): auth, API keys, metering, usage APIs, wallet balance API
2. wallet-service (Go + gRPC): balance operations, idempotency, transaction persistence
3. billing-service (Go): consumes usage events and emits wallet deductions
4. payment-service (Go): creates Razorpay orders, confirms payments, supports webhook fallback
5. frontend (Next.js 14): web app + server routes for auth and proxying
6. nginx: reverse proxy for frontend and backend traffic
7. infra: Redis, NATS, Couchbase

## Eventing and Service Communication

### What NATS publishes in this project

NATS is used for asynchronous event processing through JetStream streams.

Produced subjects:
1. usage.logged
	- Published by api-gateway after metered API requests.
	- Payload includes user_id, endpoint, method, status_code, latency_ms, request_id, timestamp.
2. wallet.deduct
	- Published by billing-service after usage is costed.
	- Payload includes user_id, amount, idempotency_key, request_id, timestamp.
3. payment.success
	- Published by payment-service after payment is verified and wallet credit succeeds.
	- Payload includes user_id, amount, razorpay_order_id, razorpay_payment_id, timestamp.

Currently defined but not actively emitted by service logic:
1. billing.process

Current consumers:
1. billing-service consumes usage.logged and computes charges.
2. wallet-service consumes wallet.deduct and applies debits idempotently.
3. payment.success is currently emitted for event visibility/integration, with no active consumer in this codebase.

### What gRPC does here

gRPC is used for synchronous wallet operations between services.

Main responsibilities:
1. api-gateway -> wallet-service: CheckBalance
	- Used by wallet balance endpoint and pre-request balance checks.
2. payment-service -> wallet-service: CreditBalance
	- Called after Razorpay verification in confirm/webhook processing.
3. wallet-service owns balance mutation logic
	- DeductBalance and CreditBalance enforce idempotency and persist transaction records.

Why this split is useful:
1. NATS handles async workload and decoupling (usage -> billing -> deduction).
2. gRPC gives immediate, strongly controlled wallet state changes for request-time checks and payment credits.

### What is shown in frontend Logs

The Logs page is usage analytics, not Docker/service logs.

Data source:
1. Frontend calls GET /proxy/api/v1/usage/logs.
2. api-gateway queries Couchbase usage documents and returns paginated data.

Visible columns in the Logs table:
1. Timestamp
2. Endpoint
3. Method
4. Cost
5. Status
6. Latency (ms)

Supported filters:
1. Endpoint
2. Start date
3. End date
4. Pagination (page + limit)

Related dashboard cards/charts (usage summary):
1. Today Cost
2. Total Cost
3. Today Requests
4. 7-day spend trend chart

## Payment Flow (Important)

Current behavior:
1. Frontend checkout success calls POST /payments/confirm with order id, payment id, signature.
2. Backend verifies signature with Razorpay key secret and processes credit + transaction.
3. Webhook endpoint /payments/webhook is still live and can also process payment.captured from Razorpay.
4. If webhook arrives later for the same order, processing is idempotent (status == completed check), so no double credit.

Notes:
1. The confirm path gives immediate UX feedback after payment success.
2. Webhook is retained as resilient fallback for asynchronous provider delivery.

## Razorpay Status

Razorpay API is working in this setup.
1. Order creation is verified with test key id/secret against Razorpay Orders API.
2. App order creation endpoint returns valid order_id and key_id.
3. Confirm endpoint validates Razorpay signature and processes wallet credit.

## Environment Variables

Use `.env` at repo root.

Required for payments:
1. RAZORPAY_KEY_ID
2. RAZORPAY_KEY_SECRET
3. RAZORPAY_WEBHOOK_SECRET

Required secrets:
1. JWT_SECRET
2. API_KEY_SALT

Infra defaults (compose-ready):
1. REDIS_URL=redis://redis:6379
2. NATS_URL=nats://nats:4222
3. COUCHBASE_HOST=couchbase://couchbase
4. COUCHBASE_USERNAME=Administrator
5. COUCHBASE_PASSWORD=password
6. COUCHBASE_BUCKET=apibilling

Ports:
1. API_GATEWAY_PORT=8080
2. WALLET_GRPC_PORT=9090
3. BILLING_PORT=8082
4. PAYMENT_PORT=8083

Frontend:
1. NEXT_PUBLIC_BACKEND_URL=http://nginx

## Setup and Run (Clear Steps)

### Step 1: Start backend + infra

```powershell
docker compose up -d redis nats couchbase api-gateway wallet-service billing-service payment-service
docker compose ps
```

### Step 2: Initialize Couchbase bucket and indexes

Open `http://localhost:8091`

Login:
1. Username: Administrator
2. Password: password

Create bucket:
1. Name: apibilling

Create primary index in Query Workbench:

```sql
CREATE PRIMARY INDEX `idx_primary_apibilling` ON `apibilling`;
```

Optional indexes:

```sql
CREATE INDEX `idx_tx_user_created` ON `apibilling`(user_id, created_at DESC)
WHERE META().id LIKE "transaction:::%";

CREATE INDEX `idx_usage_user_time` ON `apibilling`(user_id, timestamp DESC)
WHERE META().id LIKE "usage:::%";
```

### Step 3: Start frontend + nginx

```powershell
docker compose up -d frontend nginx
docker compose ps
```

Open:
1. App: http://localhost
2. Frontend direct: http://localhost:3000
3. API gateway health: http://localhost:8080/health

## Webhook Setup (Razorpay Dashboard)

Set webhook URL

Set webhook secret:
1. Use the same value as RAZORPAY_WEBHOOK_SECRET

Select event:
1. payment.captured

## Key Endpoints

Health:
1. GET /health (api-gateway)
2. GET /health (payment-service)
3. GET /health (billing-service)

Auth:
1. POST /auth/register
2. POST /auth/login

Payments:
1. POST /payments/create-order
2. POST /payments/confirm
3. POST /payments/webhook
4. GET /payments/transactions

Wallet:
1. GET /api/v1/wallet/balance

Keys:
1. GET /keys
2. POST /keys/generate
3. DELETE /keys/{key_id}

Usage:
1. GET /api/v1/usage/summary
2. GET /api/v1/usage/logs

## Test and Validation

Backend tests:

```powershell
go test ./pkg/... ./proto/... ./services/api-gateway ./services/billing-service ./services/wallet-service ./services/payment-service
```

Frontend checks:

```powershell
cd frontend
npm run lint
npm run build
```

Integration smoke:

```powershell
make integration-test
```

## Troubleshooting Quick Guide

If register/login fails:
1. Ensure api-gateway is up and healthy.
2. Ensure nginx routes /api to frontend (Next.js server routes).

If payment succeeds in Razorpay but no transaction appears:
1. Check /payments/confirm response in frontend network tab.
2. Check payment-service logs for /payments/confirm or /payments/webhook status.
3. Verify webhook URL and secret in Razorpay dashboard.

If Docker services fail after restart:
1. Start Docker Desktop first.
2. Run compose up in this order: infra + backend, then frontend + nginx.

## Stop and Cleanup

```powershell
docker compose down
```

Remove volumes too:

```powershell
docker compose down -v
```
