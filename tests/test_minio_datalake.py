import unittest
from io import BytesIO
import zipfile
from minio_datalake.client import MinIOClient
from minio_datalake.datalake import MinIOSparkDatalake
from minio_datalake.object import MinIOObject
import minio_datalake.settings as settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestMinIOSparkDatalake(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.datalake = MinIOSparkDatalake(settings.MINIO_ENDPOINT, settings.MINIO_ACCESS_KEY, settings.MINIO_SECRET_KEY,
                                          settings.MINIO_USE_SSL)
        cls.bucket_name = 'test-bucket'
        cls.zip_object_name = 'test.zip'
        cls.csv_object_name = 'test.csv'
        cls.bucket = cls.datalake.get_bucket(cls.bucket_name)

        if not cls.bucket.exists():
            cls.bucket.make()

        # Create a test CSV and ZIP object in MinIO
        data = BytesIO(b'col1,col2\nval1,val2\nval3,val4')
        cls.bucket.put_object(cls.csv_object_name, data, len(data.getvalue()))

        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
            zip_file.writestr(cls.csv_object_name, data.getvalue())
        zip_buffer.seek(0)
        cls.bucket.put_object(cls.zip_object_name, zip_buffer, len(zip_buffer.getvalue()))

    @classmethod
    def tearDownClass(cls):
        # Remove all objects in the bucket
        objects_to_delete = [obj.object_name for obj in cls.bucket.list_objects(recursive=True)]
        for obj_name in objects_to_delete:
            cls.bucket.remove_object(obj_name)
            logger.info('Deleted object %s', obj_name)

        logger.info('Deleted bucket %s', cls.bucket_name)
        # Remove the bucket
        cls.bucket.remove()

    def test_extract_zip_to_datalake(self):
        minio_object = MinIOObject(self.datalake.client, self.bucket_name, self.zip_object_name)
        df = self.datalake.read_csv_from_zip(minio_object)
        self.assertEqual(df.count(), 2)  # Assuming the CSV file has 2 rows

    def test_read_csv_to_dataframe(self):
        minio_object = MinIOObject(self.datalake.client, self.bucket_name, self.csv_object_name)
        df = self.datalake.read_csv_to_dataframe(minio_object)
        self.assertEqual(df.count(), 2)  # Assuming the CSV file has 2 rows

    def test_data_frame_to_parquet(self):
        minio_object = MinIOObject(self.datalake.client, self.bucket_name, self.csv_object_name)
        df = self.datalake.read_csv_to_dataframe(minio_object)
        parquet_path = self.datalake.dataframe_to_parquet(df, f'{self.bucket_name}/test.parquet')
        df_parquet = self.datalake.spark.read.parquet(f's3a://{parquet_path}')
        self.assertEqual(df_parquet.count(), 2)  # Assuming the CSV file has 2 rows

    def test_ingest_csv_to_datalake(self):
        minio_object = MinIOObject(self.datalake.client, self.bucket_name, self.csv_object_name)
        df = self.datalake.ingest_csv_to_datalake(minio_object, destination_path=self.bucket_name)
        self.assertIsNotNone(df)
        self.assertEqual(df.count(), 2)

    def test_unzip(self):
        minio_object = MinIOObject(self.datalake.client, self.bucket_name, self.zip_object_name)
        extracted_objects = self.datalake.unzip(minio_object)
        self.assertGreater(len(extracted_objects), 0)  # Ensure that files were extracted
        csv_objects = [obj for obj in extracted_objects if obj.object_name.endswith('.csv')]
        self.assertEqual(len(csv_objects), 1)  # There should be one CSV file extracted
        df = self.datalake.read_csv_to_dataframe(csv_objects[0])
        self.assertEqual(df.count(), 2)  # Assuming the CSV file has 2 rows

if __name__ == '__main__':
    unittest.main()
