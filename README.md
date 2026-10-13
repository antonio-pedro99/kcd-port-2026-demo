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
- Docker (image builds)
- kubectl + a cluster (pod runs)
- A reachable Kafka cluster (topics such as `payments`, `transfers`, `cards`)

## Build images

```bash
docker build --target producer -t kcd-producer:latest .
docker build --target consumer -t kcd-consumer:latest .
```

Pods use `imagePullPolicy: Never`, so the image must already exist on the **node that runs the pod** in the Kubernetes container runtime (containerd). Building with Docker alone is not enough — Docker and containerd do not share an image store.

### Make images available to the cluster (containerd)

On each node that will schedule the pods (often `controlplane` in a lab):

```bash
docker save kcd-producer:latest | ctr -n k8s.io images import -
docker save kcd-consumer:latest | ctr -n k8s.io images import -

# optional check
ctr -n k8s.io images ls | grep kcd-
```

Alternatives: `kind load docker-image kcd-producer:latest`, `minikube image load kcd-producer:latest`, or push to a registry and change the pod image + `imagePullPolicy`.

## Deploy on Kubernetes

Set `KAFKA_BROKERS` in the pod manifests to your bootstrap servers, then:

```bash
kubectl apply -f k8s/ -n kafka-app
kubectl -n kafka-app get pods
kubectl -n kafka-app logs -f kafka-producer
kubectl -n kafka-app logs -f kafka-consumer
```

If pods were already created with `ErrImagePull` / `ImagePullBackOff`, re-import the images (above), then:

```bash
kubectl -n kafka-app delete pod kafka-producer kafka-consumer
kubectl apply -f k8s/ -n kafka-app
```

Manifests:

| File | Resource |
|------|----------|
| [`k8s/00-service-account.yaml`](k8s/00-service-account.yaml) | `kcd-port-demo` ServiceAccount (`automountServiceAccountToken: false`) |
| [`k8s/01-consumer-pod.yaml`](k8s/01-consumer-pod.yaml) | Consumer pod |
| [`k8s/02-producer-pod.yaml`](k8s/02-producer-pod.yaml) | Producer pod |

Pods use that ServiceAccount and also set `automountServiceAccountToken: false` (no Kubernetes API access required).

### Configuration

| Env | Used by | Default | Description |
|-----|---------|---------|-------------|
| `KAFKA_BROKERS` | both | `localhost:9092` | Bootstrap brokers |
| `KAFKA_TOPICS` | both | `payments,transfers,cards` | Topics (producer hashes by `account_id`) |
| `KAFKA_RATE` | producer | `1000` | Target produce rate (messages/sec) |
| `DATA_PATH` | producer | `data/transactions.json` | Path to synthetic JSON (image: `/data/transactions.json`) |
| `KAFKA_GROUP` | consumer | `risk-alerts` | Consumer group id |

CLI flags mirror the same settings: `-brokers`, `-topics`, `-rate`, `-data`, `-group`.

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
