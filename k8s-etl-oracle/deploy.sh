#!/bin/bash
set -e

docker build -t etl-oracle:latest -f docker/Dockerfile .

kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/job.yaml

kubectl logs -n etl job/warehouse-etl-job
