#!/bin/bash

# Ensure we're using Minikube's Docker daemon
eval $(minikube docker-env)

# Build the metrics-service image
docker build -t metrics-service:latest ./metrics_service

# Verify the image was built
docker images | grep metrics-service
