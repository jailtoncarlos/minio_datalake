#!/bin/bash

# To run the tests and rebuild the Docker image:
# ./run_tests.sh
# To run the tests without rebuilding the Docker image:
# ./run_tests.sh --no-build
# To specify a test file:
# ./run_tests.sh -f test_specific_file.py
# To specify a test file without rebuilding the Docker image:
# ./run_tests.sh --no-build -f test_minio_datalake.py

# Default value for building the Docker image
BUILD_IMAGE=true

# Process command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --no-build) BUILD_IMAGE=false ;;
        -f) TEST_FILE=$2; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

if [ "$BUILD_IMAGE" = true ] ; then
    echo "Starting services with docker-compose..."
    docker-compose -f docker/docker-compose.yaml up -d --build
else
    echo "Starting services with docker-compose without building the image..."
    docker-compose -f docker/docker-compose.yaml up -d
fi

# Wait for MinIO to be ready
echo "Waiting for MinIO to be ready..."
until $(curl --output /dev/null --silent --head --fail http://localhost:9000/minio/health/live); do
  printf '.'
  sleep 5
done

# Check if a specific test file was provided as an argument
if [ -n "$TEST_FILE" ]; then
  echo "Running specified test file: $TEST_FILE in the jupyterlab-pyspark container..."
  docker-compose -f docker/docker-compose.yaml exec -T jupyterlab-pyspark bash -c "cd /home/jovyan/minio_datalake && export PYTHONPATH=/home/jovyan/minio_datalake && python3 -m unittest discover -s tests -p '$TEST_FILE' -v"
else
  echo "Running all tests in the jupyterlab-pyspark container..."
  docker-compose -f docker/docker-compose.yaml exec -T jupyterlab-pyspark bash -c "cd /home/jovyan/minio_datalake && export PYTHONPATH=/home/jovyan/minio_datalake && python3 -m unittest discover -s tests -p 'test*.py' -v"
fi
#sleep 30
exit_code=$?

echo "Stopping services..."
docker-compose -f docker/docker-compose.yaml down

exit $exit_code
