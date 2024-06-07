# test_datalake.py
import unittest
from unittest.mock import patch
from datalake import MinIODatalake
import settings

import logging
logger = logging.getLogger(__name__)


class TestMinIODatalake(unittest.TestCase):
    @patch('minio.Minio.bucket_exists')
    def test_buckets_exist(self, mock_bucket_exists):

        # Log the settings being used
        logger.debug(f"Using settings: MINIO_URL={settings.MINIO_URL}, ACCESS_KEY={settings.ACCESS_KEY}, SECRET_KEY={settings.SECRET_KEY}")
        logger.debug(f"RAW_BUCKET: {settings.RAW_BUCKET}")
        logger.debug(f"STAGE_BUCKET: {settings.STAGE_BUCKET}")

        # Mock the return value of bucket_exists method
        mock_bucket_exists.side_effect = lambda bucket_name: bucket_name in [settings.RAW_BUCKET, settings.STAGE_BUCKET]

        datalake = MinIODatalake(endpoint=settings.MINIO_URL,
                                 access_key=settings.ACCESS_KEY,
                                 secret_key=settings.SECRET_KEY)

        # Check if 'raw' bucket exists
        raw_bucket_exists = datalake.get_bucket(settings.RAW_BUCKET).bucket_exists()
        stage_bucket_exists = datalake.get_bucket(settings.STAGE_BUCKET).bucket_exists()

        # Check if 'raw' bucket exists
        self.assertTrue(raw_bucket_exists, "Bucket 'raw' should exist")

        # Check if 'stage' bucket exists
        self.assertTrue(stage_bucket_exists, "Bucket 'stage' should exist")

        # Check if a non-existent bucket does not exist
        self.assertFalse(datalake.get_bucket('non_existent_bucket').bucket_exists(),
                         "Bucket 'non_existent_bucket' should not exist")


if __name__ == '__main__':
    unittest.main()
