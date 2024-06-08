import os
import logging
# logging.basicConfig(
#     level=logging.DEBUG,  # Definir nível de log
#     format='%(levelname)s: %(message)s',  # Formato das mensagens de log
#     force=True
# )

# Main settings for MinIO DataLake
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000') # Use the service name defined in docker-compose.yml
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_USE_SSL = False

RAW_BUCKET = 'raw'
STAGE_BUCKET = 'stage'

# Load local settings if they exist
try:
    from local_settings import *
except ImportError:
    pass
