#!/usr/bin/env bash

# Part Zero: Starting the client applications
# KCD Porto 2026
set -euo pipefail

kubectl apply -f kafka-app.yaml

kubectl get pods -n kafka-app
