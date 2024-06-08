#!/bin/bash

echo "Starting services with docker-compose..."
docker-compose -f docker/docker-compose.yaml up -d --build

# Wait for MinIO to be ready
echo "Waiting for MinIO to be ready..."
until $(curl --output /dev/null --silent --head --fail http://localhost:9000/minio/health/live); do
  printf '.'
  sleep 5
done

# Check if a specific test file was provided as an argument
if [ -n "$1" ]; then
  TEST_FILE=$1
  echo "Running specified test file: $TEST_FILE in the jupyterlab-pyspark container..."
  docker-compose -f docker/docker-compose.yaml exec -T jupyterlab-pyspark bash -c "cd /home/jovyan/minio_datalake && export PYTHONPATH=/home/jovyan/minio_datalake && python3 -m unittest discover -s tests -p '$TEST_FILE' -v"
else
  echo "Running all tests in the jupyterlab-pyspark container..."
  docker-compose -f docker/docker-compose.yaml exec -T jupyterlab-pyspark bash -c "cd /home/jovyan/minio_datalake && export PYTHONPATH=/home/jovyan/minio_datalake && python3 -m unittest discover -s tests -p 'test*.py' -v"
fi

exit_code=$?

echo "Stopping services..."
docker-compose -f docker/docker-compose.yaml down

exit $exit_code
