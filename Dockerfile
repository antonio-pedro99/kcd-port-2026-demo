# syntax=docker/dockerfile:1
#
# Pattern from https://docs.docker.com/guides/golang/
# and https://docs.docker.com/build/building/multi-platform/
#
# Build for the amd64 cluster from a Mac (Docker Desktop / BuildKit):
#   docker build --platform linux/amd64 --target producer \
#     -t antdev9/kafka-app-demo:producer --push .
#   docker build --platform linux/amd64 --target consumer \
#     -t antdev9/kafka-app-demo:consumer --push .
#
# Or build both platforms into one manifest list:
#   docker build --platform linux/amd64,linux/arm64 --target producer \
#     -t antdev9/kafka-app-demo:producer --push .

# Native Go toolchain on the builder (no QEMU for compile); cross-compile to TARGET*.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64

ENV CGO_ENABLED=0

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
      go build -trimpath -ldflags="-s -w" -o /out/producer ./cmd/producer \
 && GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
      go build -trimpath -ldflags="-s -w" -o /out/consumer ./cmd/consumer

# Lean runtime images (distroless), same idea as Docker's Go guide.
FROM gcr.io/distroless/static-debian12:nonroot AS producer
COPY --from=builder /out/producer /producer
COPY data/transactions.json /data/transactions.json
USER nonroot:nonroot
ENTRYPOINT ["/producer"]

FROM gcr.io/distroless/static-debian12:nonroot AS consumer
COPY --from=builder /out/consumer /consumer
USER nonroot:nonroot
ENTRYPOINT ["/consumer"]
