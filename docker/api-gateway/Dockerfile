FROM golang:1.23-alpine AS builder
WORKDIR /app

COPY services/api-gateway/go.mod ./services/api-gateway/go.mod
COPY pkg/go.mod ./pkg/go.mod
COPY proto/go.mod ./proto/go.mod
WORKDIR /app/services/api-gateway
RUN go mod download

COPY services/api-gateway /app/services/api-gateway
COPY pkg /app/pkg
COPY proto /app/proto
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/api-gateway .

FROM alpine:3.19
RUN adduser -D -g '' appuser
USER appuser
WORKDIR /home/appuser
COPY --from=builder /bin/api-gateway /usr/local/bin/api-gateway
EXPOSE 8080
CMD ["/usr/local/bin/api-gateway"]
