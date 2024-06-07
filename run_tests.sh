#!/bin/bash
docker exec -it jupyterlab-pyspark bash -c "cd /home/jovyan/minio_datalake && export PYTHONPATH=/home/jovyan/minio_datalake && python3 -m unittest discover -s . -p 'tests.py'"