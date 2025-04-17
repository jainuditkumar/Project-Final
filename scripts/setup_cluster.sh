#!/bin/bash
set -e

echo "Setting up RabbitMQ cluster..."

# Start RabbitMQ nodes
docker-compose -f ../docker/docker-compose.yml up -d rabbitmq-node-1 rabbitmq-node-2 rabbitmq-node-3

echo "Waiting for RabbitMQ nodes to initialize (30s)..."
sleep 30

echo "Creating cluster by joining nodes to rabbitmq-node-1..."
docker exec rabbitmq-node-2 rabbitmqctl stop_app
docker exec rabbitmq-node-2 rabbitmqctl reset
docker exec rabbitmq-node-2 rabbitmqctl join_cluster rabbit@rabbitmq-node-1
docker exec rabbitmq-node-2 rabbitmqctl start_app

docker exec rabbitmq-node-3 rabbitmqctl stop_app
docker exec rabbitmq-node-3 rabbitmqctl reset
docker exec rabbitmq-node-3 rabbitmqctl join_cluster rabbit@rabbitmq-node-1
docker exec rabbitmq-node-3 rabbitmqctl start_app

echo "Setting HA policy..."
docker exec rabbitmq-node-1 rabbitmqctl set_policy ha-all ".*" '{"ha-mode":"all"}' --apply-to queues

echo "Starting management interface..."
docker-compose -f ../docker/docker-compose.yml up -d web

echo "RabbitMQ cluster setup complete!"
echo "Management UI should be available at http://localhost:15672"
echo "Username: guest, Password: guest"