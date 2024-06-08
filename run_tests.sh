#!/bin/bash
echo "Starting services with docker-compose..."
docker-compose -f docker/docker-compose.yaml up -d --build

echo "Running tests in the jupyterlab-pyspark container..."
docker-compose -f docker/docker-compose.yaml exec -T jupyterlab-pyspark bash -c "cd /home/jovyan/minio_datalake && export PYTHONPATH=/home/jovyan/minio_datalake && python3 -m unittest discover -s tests -p 'tests.py' -v"
exit_code=$?

echo "Stopping services..."
docker-compose -f docker/docker-compose.yaml down
