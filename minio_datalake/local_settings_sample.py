import os


# Override the settings here for the local environment
# Rename this file to local_settings.py and adjust the settings as needed
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000') # Use the service name defined in docker-compose.yml
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_USE_SSL = False

RAW_BUCKET = 'raw'
STAGE_BUCKET = 'stage'
