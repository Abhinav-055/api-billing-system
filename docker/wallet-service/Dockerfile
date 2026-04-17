FROM golang:1.23-alpine AS builder
WORKDIR /app

COPY services/wallet-service/go.mod ./services/wallet-service/go.mod
COPY pkg/go.mod ./pkg/go.mod
COPY proto/go.mod ./proto/go.mod
WORKDIR /app/services/wallet-service
RUN go mod download

COPY services/wallet-service /app/services/wallet-service
COPY pkg /app/pkg
COPY proto /app/proto
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/wallet-service .

FROM alpine:3.19
RUN adduser -D -g '' appuser
USER appuser
WORKDIR /home/appuser
COPY --from=builder /bin/wallet-service /usr/local/bin/wallet-service
EXPOSE 9090
CMD ["/usr/local/bin/wallet-service"]
