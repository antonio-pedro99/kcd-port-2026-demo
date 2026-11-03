
#!/usr/bin/env bash

# Part One: Installing Strimzi
# KCD Porto 2026
set -e

# --------------------------------------------------
# Load demo-magic
# --------------------------------------------------

. demo-magic.sh

# --------------------------------------------------
# Demo
# --------------------------------------------------

clear

# --------------------------------------------------
# Install Strimzi
# --------------------------------------------------

pe "kubectl apply -f k8s/single-node.yml -n kafka"

pe "kubectl get pods -n kafka -w"
