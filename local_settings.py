import os

# Main settings for MinIO DataLake
# MINIO_URL = '10.3.225.238:31100'
# ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'admin')
# SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'sample_key')

MINIO_URL = 'minio:9000'  # Use the service name defined in docker-compose.yml
ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')

RAW_BUCKET = 'raw'
STAGE_BUCKET = 'stage'