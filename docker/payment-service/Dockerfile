FROM golang:1.23-alpine AS builder
WORKDIR /app

COPY services/payment-service/go.mod ./services/payment-service/go.mod
COPY pkg/go.mod ./pkg/go.mod
COPY proto/go.mod ./proto/go.mod
WORKDIR /app/services/payment-service
RUN go mod download

COPY services/payment-service /app/services/payment-service
COPY pkg /app/pkg
COPY proto /app/proto
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/payment-service .

FROM alpine:3.19
RUN adduser -D -g '' appuser
USER appuser
WORKDIR /home/appuser
COPY --from=builder /bin/payment-service /usr/local/bin/payment-service
EXPOSE 8083
CMD ["/usr/local/bin/payment-service"]
