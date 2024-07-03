import logging
import unittest
import zipfile
from io import BytesIO

from minio_spark import MinioSpark
from minio_spark.bucket import MinioBucket
from minio_spark.object import MinioObject

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestMinIOSparkDatalake(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Inicializar MinIOSpark com o cliente Minio (presumindo que a configuração do cliente já está definida)
        cls.datalake = MinioSpark()
        cls.bucket_name = 'bucket-datalake'
        cls.prefix = 'DIR/FILE_'
        cls.zip_objects_names = [
            'DIR/FILE_1.zip',
            'DIR/FILE_2.zip',
            'DIR/FILE_3.zip'
        ]
        cls.csv_object_name = 'test.csv'
        cls.bucket = cls.datalake.get_bucket(cls.bucket_name)

        if not cls.bucket.exists():
            cls.bucket.make()

        data = BytesIO(b'col1,col2\nval1,val2\nval3,val4')
        cls.bucket.put_object(cls.csv_object_name, data, len(data.getvalue()))

        # Create test ZIP objects in MinIO
        for i, zip_filename in enumerate(cls.zip_objects_names):
            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
                zip_file.writestr(f'{cls.csv_object_name}_{i}', data.getvalue())
            zip_buffer.seek(0)
            cls.bucket.put_object(zip_filename, zip_buffer, len(zip_buffer.getvalue()))

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
        df = self.datalake.read_csv_from_zip(self.bucket_name, self.prefix)
        self.assertEqual(df.count(), 6)  # Assuming each CSV file has 2 rows and there are 3 ZIP files

    def test_read_csv_to_dataframe(self):
        df = self.datalake.read_csv(self.bucket_name, self.csv_object_name)
        self.assertEqual(df.count(), 2)  # Assuming the CSV file has 2 rows

    def test_read_parquet_to_dataframe(self):
        df = self.datalake.read_csv(self.bucket_name, self.csv_object_name)
        parquet_object = MinioObject(self.datalake.client, self.bucket_name, 'test.parquet')
        self.datalake.to_parquet(df, parquet_object)
        df_parquet = self.datalake.read_parquet(parquet_object)
        self.assertEqual(df_parquet.count(), 2)  # Assuming the CSV file has 2 rows

    def test_data_frame_to_parquet(self):
        df = self.datalake.read_csv(self.bucket_name, self.csv_object_name)
        parquet_object = MinioObject(self.datalake.client, self.bucket_name, 'test.parquet')
        self.datalake.to_parquet(df, parquet_object)
        df_parquet = self.datalake.spark.read.parquet(f's3a://{self.bucket_name}/test.parquet')
        self.assertEqual(df_parquet.count(), 2)  # Assuming the CSV file has 2 rows

    def test_ingest_file_to_datalake(self):
        df = self.datalake.ingest_file_to_datalake(self.bucket_name, self.csv_object_name, destination_bucket_name=self.bucket_name)
        self.assertIsNotNone(df)
        self.assertEqual(df.count(), 2)

    def test_unzip(self):
        minio_object = MinioObject(self.datalake.client, self.bucket_name, self.zip_objects_names[0])
        extracted_objects = self.datalake.extract_and_upload_zip(minio_object)
        self.assertGreater(len(extracted_objects), 0)

    def test_get_bucket(self):
        bucket = self.datalake.get_bucket(self.bucket_name)
        self.assertIsInstance(bucket, MinioBucket)
        self.assertEqual(bucket.name, self.bucket_name)

    def test_get_object(self):
        minio_object = self.datalake.get_object(self.bucket_name, self.csv_object_name)
        self.assertIsInstance(minio_object, MinioObject)
        self.assertEqual(minio_object.name, self.csv_object_name)

if __name__ == '__main__':
    unittest.main()
