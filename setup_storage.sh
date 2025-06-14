#!/bin/bash

# Start MinIO using Docker Compose
echo "Starting MinIO S3-like storage..."
docker-compose up -d

# Wait for MinIO to be ready
echo "Waiting for MinIO to be ready..."
sleep 10

# Check if MinIO is running
if curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    echo "MinIO is running successfully!"
    echo "MinIO Console: http://localhost:9001"
    echo "MinIO API: http://localhost:9000"
    echo "Username: minioadmin"
    echo "Password: minioadmin"
else
    echo "MinIO failed to start properly"
    exit 1
fi

# Install Python dependencies
echo "Installing Python dependencies..."
pip3 install -r requirements.txt

echo "Setup completed successfully!"

