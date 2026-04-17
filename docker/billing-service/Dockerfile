FROM golang:1.23-alpine AS builder
WORKDIR /app

COPY services/billing-service/go.mod ./services/billing-service/go.mod
COPY pkg/go.mod ./pkg/go.mod
COPY proto/go.mod ./proto/go.mod
WORKDIR /app/services/billing-service
RUN go mod download

COPY services/billing-service /app/services/billing-service
COPY pkg /app/pkg
COPY proto /app/proto
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/billing-service .

FROM alpine:3.19
RUN adduser -D -g '' appuser
USER appuser
WORKDIR /home/appuser
COPY --from=builder /bin/billing-service /usr/local/bin/billing-service
EXPOSE 8082
CMD ["/usr/local/bin/billing-service"]
