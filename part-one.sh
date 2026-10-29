
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

p "First, create a dedicated namespace for Kafka and Strimzi."

pe "kubectl create namespace kafka"

p "Now, install Strimzi's Custom Resource Definitions, RBAC resources, and Cluster Operator."

pe "kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka"

clear

# --------------------------------------------------
# Verify installation
# --------------------------------------------------

p "Let's watch Kubernetes start the Strimzi Cluster Operator."

pe "kubectl get pods -n kafka"

p "The operator should now be running."

pe "kubectl get deployment strimzi-cluster-operator -n kafka"

p "Let's verify that Strimzi's Kafka custom resource definitions are registered."

pe "kubectl get crds | grep kafka.strimzi.io"

# --------------------------------------------------
# End of Part One
# --------------------------------------------------

p "Strimzi is installed!"

p "The operator watches Kubernetes resources and reconciles the desired state."

p "Next: let's use Strimzi to deploy an Apache Kafka cluster."
