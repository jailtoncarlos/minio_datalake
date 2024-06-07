# local_settings_sample.py

import os

# Override the settings here for the local environment
# Rename this file to local_settings.py and adjust the settings as needed

MINIO_URL = 'http://your-local-minio-url:9000'
ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'your_local_access_key')
SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'your_local_secret_key')

RAW_BUCKET = 'local_raw'
STAGE_BUCKET = 'local_stage'
