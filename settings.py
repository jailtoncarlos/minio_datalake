import os
import logging
logging.basicConfig(
    level=logging.DEBUG,  # Definir nível de log
    format='%(levelname)s: %(message)s',  # Formato das mensagens de log
    force=True
)

# Main settings for MinIO DataLake
MINIO_URL = 'minio:9000'  # Use the service name defined in docker-compose.yml
ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')

RAW_BUCKET = 'raw'
STAGE_BUCKET = 'stage'

# Enable logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load local settings if they exist
try:
    from local_settings import *
except ImportError:
    pass

logger.debug("Settings loaded successfully.")
