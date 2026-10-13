# KCD Port 2026 — Apache Kafka with Strimzi Demo

The goal of this demo is to demonstrate how to deploy and maintain an Apache Kafka cluster on Kubernetes using Strimzi.

## Client Applications

I have created two simple client applications in Go: a producer and a consumer. The producer generates synthetic transactions and sends them to Kafka topics, while the consumer reads messages from those topics and prints them to the console.

Both applications will run in Kubernetes pods, but in a different namespace than the Kafka cluster. The producer will send messages to three topics: `payments`, `transfers`, and `cards`. The consumer will read messages from those topics and print them to the console.

> NOTE: Explaining the code is out of scope for this demo. The code is available in the `cmd/producer` and `cmd/consumer` directories.

## Prerequisites

In this demo, we assume that you have a Kubernetes cluster running and that you have `kubectl` configured to access it.
Moreover, both client applications can be deployed in the following ways:

1. Create a namespace

```bash
kubectl create namespace kafka-app
```

2. Apply the `kafka-app.yaml` manifest to deploy the producer and consumer applications in the `kafka-app` namespace.

```bash
kubectl apply -f https://raw.githubusercontent.com/antonio-pedro99/kcd-port-2026-demo/refs/heads/main/kafka-app.yaml -n kafka-app
```

## Installing the Strimzi Operator and CRDs

Before deploying the Kafka cluster, we need to install the Strimzi operator in the `kafka` namespace. You can do this by applying the `strimzi-cluster-operator` manifest:

1. Create a namespace for Kafka

```bash
kubectl create namespace kafka
```

2. Apply the Strimzi operator manifest

```bash
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
```

This command will create the necessary resources for the Strimzi operator in the `kafka` namespace.

3. Now you should have the Strimzi operator running in the `kafka` namespace. You can check the status of the operator by running:

```bash
kubectl get pods -n kafka
```

## Deploying the Kafka Cluster

Now that the Strimzi operator is running, we can deploy a Kafka cluster. You can do this by applying the `kafka-cluster.yaml` manifest:

```bash
kubectl apply -f https://strimzi.io/examples/latest/kafka/kafka-single-node.yaml -n kafka
```

Before anything, let's check and wait for the Kafka cluster to be ready:

```bash
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=300s -n kafka 
```

Our Kafka cluster is now up and running. Our producer and consumer applications are already deployed in the `kafka-app` namespace, and they will start sending and receiving messages from the Kafka topics.

The second part of the demo is to show how we maintain the Kafka cluster and its components.