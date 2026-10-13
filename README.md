# KCD Port 2026 — Kafka Console Demo

Two Go console apps that simulate financial transaction traffic on Apache Kafka:

- **producer** — loads synthetic transactions from JSON, shuffles them, and publishes across multiple topics at a configurable rate
- **consumer** — consumes those topics, emits risk alerts, and reports throughput, consumer lag, and sequence numbers

Kafka is deployed separately. Point the apps at it with `KAFKA_BROKERS` (or `-brokers`).

## Layout

```text
cmd/producer/     producer console app
cmd/consumer/     consumer console app
data/             synthetic transactions JSON
Dockerfile        multi-target images (producer | consumer)
k8s/              ServiceAccount + pods
```

## Prerequisites

- Go 1.22+ (local runs)
- Docker / buildx (image builds)
- kubectl + a cluster (pod runs)
- A reachable Kafka cluster (topics such as `payments`, `transfers`, `cards`)

## Build and push images

Follows Docker’s [Go guide](https://docs.docker.com/guides/golang/) + [multi-platform builds](https://docs.docker.com/build/building/multi-platform/):

- `# syntax=docker/dockerfile:1` (BuildKit)
- Multi-stage: compile in `golang`, run in distroless
- `FROM --platform=$BUILDPLATFORM` + `TARGETOS` / `TARGETARCH` so a Mac can cross-compile for the **amd64** cluster

Publish to `antdev9/kafka-app-demo`. Use **Docker Desktop** on the Mac (BuildKit is on by default).

### From your Mac (recommended)

```bash
docker login
export DOCKER_BUILDKIT=1

# --platform selects the cluster arch; BuildKit sets TARGETARCH=amd64 for the Go build
docker build --platform linux/amd64 --target producer \
  -t antdev9/kafka-app-demo:producer --push .

docker build --platform linux/amd64 --target consumer \
  -t antdev9/kafka-app-demo:consumer --push .
```

Or: `./scripts/build-push.sh`

Check the published image (not a stale local arm64 tag):

```bash
docker buildx imagetools inspect antdev9/kafka-app-demo:consumer
# Platform should include linux/amd64
```

Without `--platform` on Apple Silicon you get `arm64` → cluster pods fail with `exec format error`.

### From the controlplane (native amd64)

```bash
docker build --target producer -t antdev9/kafka-app-demo:producer .
docker build --target consumer -t antdev9/kafka-app-demo:consumer .
docker push antdev9/kafka-app-demo:producer
docker push antdev9/kafka-app-demo:consumer
```

Then recreate pods:

```bash
kubectl -n kafka-app delete pod kafka-producer kafka-consumer
kubectl apply -f k8s/ -n kafka-app
```

Pods set `command` (and producer `args`) explicitly; Kafka settings come from the ConfigMap.

## Deploy on Kubernetes

Edit Kafka settings in [`k8s/01-configmap.yaml`](k8s/01-configmap.yaml), then:

```bash
kubectl apply -f k8s/ -n kafka-app
kubectl -n kafka-app get pods
kubectl -n kafka-app logs -f kafka-producer
kubectl -n kafka-app logs -f kafka-consumer
```

ConfigMap changes are picked up only when pods are recreated:

```bash
kubectl apply -f k8s/01-configmap.yaml -n kafka-app
kubectl -n kafka-app delete pod kafka-producer kafka-consumer
kubectl apply -f k8s/ -n kafka-app
```

Manifests:

| File | Resource |
|------|----------|
| [`k8s/00-service-account.yaml`](k8s/00-service-account.yaml) | `kcd-port-demo` ServiceAccount (`automountServiceAccountToken: false`) |
| [`k8s/01-configmap.yaml`](k8s/01-configmap.yaml) | `kafka-app-config` ConfigMap (Kafka settings) |
| [`k8s/02-consumer-pod.yaml`](k8s/02-consumer-pod.yaml) | Consumer pod → `antdev9/kafka-app-demo:consumer` |
| [`k8s/03-producer-pod.yaml`](k8s/03-producer-pod.yaml) | Producer pod → `antdev9/kafka-app-demo:producer` |

Pods load config via `envFrom` → ConfigMap. ServiceAccount token automount is disabled (no Kubernetes API access required).

### Configuration (`kafka-app-config`)

| Key | Used by | Example | Description |
|-----|---------|---------|-------------|
| `KAFKA_BROKERS` | both | `kafka:9092` | Bootstrap brokers |
| `KAFKA_TOPICS` | both | `payments,transfers,cards` | Topics (producer hashes by `account_id`) |
| `KAFKA_RATE` | producer | `5000` | Target produce rate (messages/sec) |
| `DATA_PATH` | producer | `/data/transactions.json` | Path to synthetic JSON in the image |
| `KAFKA_GROUP` | consumer | `risk-alerts` | Consumer group id |

CLI flags mirror the same settings for local runs: `-brokers`, `-topics`, `-rate`, `-data`, `-group`.

## Local run

```bash
go run ./cmd/producer -brokers localhost:9092 -rate 5000
go run ./cmd/consumer -brokers localhost:9092
```

Example status lines:

```text
[producer] rate=5000 msg/s  seq=1204331  topics=payments:4150,transfers:4100,cards:4181
[consumer] rate=4890 msg/s  lag=12440  seq=1191891  alerts=37
```

Risk alerts fire when `risk_score >= 0.8` or `amount >= 10000`.

## Synthetic data

[`data/transactions.json`](data/transactions.json) has 3,000 sample financial transactions (`txn_id`, `account_id`, `amount`, `currency`, `merchant`, `country`, `risk_score`, file `seq`, …).

The producer:

1. Loads the file once
2. Shuffles records so emit order is **not** file `seq` order
3. Reshuffles after each full pass
4. Assigns a monotonic `produce_seq` used for metrics (independent of file `seq`)
