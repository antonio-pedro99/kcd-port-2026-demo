
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

pe "kubectl create namespace kafka"

pe "kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka"

clear

# --------------------------------------------------
# Verify installation
# --------------------------------------------------
pe "kubectl get pods -n kafka"

pe "kubectl get deployment strimzi-cluster-operator -n kafka"

pe "kubectl get crds | grep kafka.strimzi.io"

# --------------------------------------------------
# End of Part One
# --------------------------------------------------

p "Strimzi is installed!"
