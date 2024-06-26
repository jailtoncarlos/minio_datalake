import unittest
from unittest.mock import patch
import logging
from minio_datalake.datalake import MinIOSparkDatalake
from minio_datalake import settings as settings


# Configurar logging para exibir apenas mensagens de debug do próprio teste
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.handlers = [handler]


class TestMinIODatalake(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.datalake = MinIOSparkDatalake()

    def test_buckets_exist_or_create(self):
        raw_bucket = self.datalake.get_bucket(settings.MINIO_BUCKET_RAW_NAME)
        stage_bucket = self.datalake.get_bucket(settings.MINIO_BUCKET_STAGE_NAME)

        # Check if the 'raw' bucket exists, if not create it
        if not raw_bucket.exists():
            print(f"Creating bucket '{settings.MINIO_BUCKET_RAW_NAME}'...")
            raw_bucket.create()

        # Check if the 'stage' bucket exists, if not create it
        if not stage_bucket.exists():
            print(f"Creating bucket '{settings.MINIO_BUCKET_STAGE_NAME}'...")
            stage_bucket.create()

        # Verify that the buckets now exist
        self.assertTrue(raw_bucket.exists(), f"Bucket '{settings.MINIO_BUCKET_RAW_NAME}' should exist.")
        self.assertTrue(stage_bucket.exists(), f"Bucket '{settings.MINIO_BUCKET_STAGE_NAME}' should exist.")

    def test_list_buckets(self):
        # List directories in the root of the DataLake
        buckets = self.datalake.client.list_buckets()
        print("Buckets in the root of the DataLake:")
        for bucket in buckets:
            print(bucket.name)
        self.assertTrue(len(buckets) > 0, "There should be at least one bucket in the DataLake.")

if __name__ == '__main__':
    unittest.main()
