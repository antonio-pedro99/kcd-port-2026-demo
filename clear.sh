
#!/usr/bin/env bash

# Clear demo environment
# KCD Porto 2026
# https://github.com/antonio-pedro99/kcd-port-2026-demo

set -euo pipefail

NAMESPACE="kafka"
CLUSTER_NAME="my-cluster"
STRIMZI_VERSION="latest"

# 1. Delete Strimzi custom resources

RESOURCES=$(kubectl get strimzi -o name -n "$NAMESPACE" 2>/dev/null || true)

if [[ -n "$RESOURCES" ]]; then
  while IFS= read -r resource; do
    [[ -z "$resource" ]] && continue
    kubectl delete "$resource" -n "$NAMESPACE" --ignore-not-found
  done <<< "$RESOURCES"
fi

# 2. Delete Kafka persistent volume claims

kubectl delete pvc \
  -l "strimzi.io/name=${CLUSTER_NAME}-kafka" \
  -n "$NAMESPACE" \
  --ignore-not-found \
  --wait=true \
  --timeout=60s

# 3. Remove the Strimzi installation

kubectl delete -f \
  "https://strimzi.io/install/${STRIMZI_VERSION}?namespace=${NAMESPACE}" \
  -n "$NAMESPACE" \
  --ignore-not-found \
  --wait=true \
  --timeout=60s

# 4. Delete the namespace

kubectl delete namespace "$NAMESPACE" \
  --ignore-not-found \
  --wait=true \
  --timeout=60s
