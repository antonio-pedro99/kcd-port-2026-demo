# Build:
#   docker build --target producer -t kcd-producer:latest .
#   docker build --target consumer -t kcd-consumer:latest .

FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/producer ./cmd/producer \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/consumer ./cmd/consumer

FROM gcr.io/distroless/static-debian12:nonroot AS producer
COPY --from=builder /out/producer /producer
COPY data/transactions.json /data/transactions.json
USER nonroot:nonroot
ENTRYPOINT ["/producer"]
CMD ["-data", "/data/transactions.json"]

FROM gcr.io/distroless/static-debian12:nonroot AS consumer
COPY --from=builder /out/consumer /consumer
USER nonroot:nonroot
ENTRYPOINT ["/consumer"]
