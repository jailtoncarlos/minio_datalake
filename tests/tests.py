import unittest
import logging
from minio import S3Error
from minio_spark import MinioSpark
from minio_spark import settings
from io import BytesIO

# Configurar logging para exibir apenas mensagens de debug do próprio teste
logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler()
# formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# logger.handlers = [handler]

class TestMinIODatalake(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.datalake = MinioSpark()

    @classmethod
    def tearDownClass(cls):
        raw_bucket = cls.datalake.get_bucket(settings.S3_BUCKET_RAW_NAME)
        # Remove all objects in the bucket
        objects_to_delete = [obj.object_name for obj in raw_bucket.list_objects(recursive=True)]
        for obj_name in objects_to_delete:
            raw_bucket.remove_object(obj_name)

    def test_buckets_exist_or_create(self):
        raw_bucket = self.datalake.get_bucket(settings.S3_BUCKET_RAW_NAME)
        stage_bucket = self.datalake.get_bucket(settings.S3_BUCKET_STAGE_NAME)

        # Check if the 'raw' bucket exists, if not create it
        if not raw_bucket.exists():
            print(f"Creating bucket '{settings.S3_BUCKET_RAW_NAME}'...")
            raw_bucket.make()

        # Check if the 'stage' bucket exists, if not create it
        if not stage_bucket.exists():
            print(f"Creating bucket '{settings.S3_BUCKET_STAGE_NAME}'...")
            stage_bucket.make()

        # Verify that the buckets now exist
        self.assertTrue(raw_bucket.exists(), f"Bucket '{settings.S3_BUCKET_RAW_NAME}' should exist.")
        self.assertTrue(stage_bucket.exists(), f"Bucket '{settings.S3_BUCKET_STAGE_NAME}' should exist.")

    def test_list_buckets(self):
        # List directories in the root of the DataLake
        buckets = self.datalake.client.list_buckets()
        # print("Buckets in the root of the DataLake:")
        # for bucket in buckets:
        #     print(bucket.name)
        self.assertTrue(len(buckets) > 0, "There should be at least one bucket in the DataLake.")

    def test_pagination(self):
        raw_bucket = self.datalake.get_bucket(settings.S3_BUCKET_RAW_NAME)

        # Number of test objects to create
        num_test_objects = 105  # Adjust this number to ensure pagination

        # Create test objects in the "raw" bucket
        for i in range(num_test_objects):
            object_name = f"test_object_{i}.txt"
            data = BytesIO(b"Test data for pagination")
            raw_bucket.put_object(object_name, data, len(data.getvalue()))
            logger.debug(f"Created object {object_name} in bucket {settings.S3_BUCKET_RAW_NAME}")

        # List objects using pagination
        object_names = []
        for obj in raw_bucket.list_objects(prefix="test_object_"):
            object_names.append(obj.object_name)

        # Verify that all test objects were listed
        self.assertEqual(len(object_names), num_test_objects, f"Expected {num_test_objects} objects, but found {len(object_names)}.")

if __name__ == '__main__':
    unittest.main()
