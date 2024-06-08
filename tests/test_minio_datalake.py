import unittest
from unittest.mock import patch, MagicMock
from io import BytesIO
from pyspark.sql import SparkSession
from minio_datalake.datalake import MinIOSparkDatalake
from minio_datalake.bucket import MinIOBucket
from minio_datalake.object import MinIOObject

class TestMinIODatalake(unittest.TestCase):

    @patch('minio_datalake.datalake.MinIOClient')
    @patch('minio_datalake.datalake.SparkSession')
    def setUp(self, MockSparkSession, MockMinIOClient):
        self.mock_spark = MockSparkSession.builder.appName().getOrCreate()
        self.mock_client = MockMinIOClient.return_value
        self.datalake = MinIOSparkDatalake()

    @patch('minio_datalake.datalake.zipfile.ZipFile')
    @patch('minio_datalake.datalake.MinIOClient.put_object')
    @patch('minio_datalake.datalake.MinIOClient.fget_object')
    def test_extract_zip_to_datalake(self, mock_fget_object, mock_put_object, MockZipFile):
        mock_fget_object.return_value = None
        mock_zip = MagicMock()
        MockZipFile.return_value.__enter__.return_value = mock_zip
        mock_zip.namelist.return_value = ['file1.txt', 'file2.txt']
        mock_zip.read.side_effect = [b'Test data 1', b'Test data 2']

        result = self.datalake.extract_zip_to_datalake('/raw/titanic-3.zip')
        mock_fget_object.assert_called_once()
        mock_put_object.assert_any_call('raw/titanic-3', 'file1.txt', BytesIO(b'Test data 1'), len(b'Test data 1'))
        mock_put_object.assert_any_call('raw/titanic-3', 'file2.txt', BytesIO(b'Test data 2'), len(b'Test data 2'))
        self.assertEqual(result, 'raw/titanic-3')

    @patch('minio_datalake.datalake.MinIOClient')
    def test_get_client(self, MockMinIOClient):
        client = self.datalake.get_client()
        self.assertEqual(client, self.mock_client)

    @patch('minio_datalake.datalake.MinIOBucket', spec=MinIOBucket)
    def test_get_bucket(self, MockMinIOBucket):
        bucket = self.datalake.get_bucket('test_bucket')
        MockMinIOBucket.assert_called_once_with(self.mock_client, 'test_bucket')
        self.assertIsInstance(bucket, MinIOBucket)

    @patch('minio_datalake.datalake.MinIOObject', spec=MinIOObject)
    def test_get_object(self, MockMinIOObject):
        obj = self.datalake.get_object('test_bucket', 'test_object')
        MockMinIOObject.assert_called_once_with(self.mock_client, 'test_bucket', 'test_object')
        self.assertIsInstance(obj, MinIOObject)

    @patch('minio_datalake.datalake.MinIOClient.fget_object')
    def test_read_csv_to_dataframe(self, mock_fget_object):
        mock_fget_object.return_value = None
        df = self.datalake.read_csv_to_dataframe('/test_bucket/test.csv')
        self.assertIsNotNone(df)

    @patch('minio_datalake.datalake.MinIOClient.put_object')
    def test_dataframe_to_parquet(self, mock_put_object):
        df = self.mock_spark.createDataFrame([(1, 'test')], ['id', 'value'])
        path = self.datalake.dataframe_to_parquet(df, '/test_bucket/test.parquet')
        self.assertEqual(path, 'test_bucket/test.parquet')
        mock_put_object.assert_called_once()

    @patch('minio_datalake.datalake.MinIOClient.fget_object')
    @patch('minio_datalake.datalake.MinIOClient.put_object')
    def test_ingest_csv_to_datalake(self, mock_put_object, mock_fget_object):
        mock_fget_object.return_value = None
        df = self.datalake.ingest_csv_to_datalake('/test_bucket/test.csv', temp_view_name='test_view')
        self.assertIsNotNone(df)
        mock_fget_object.assert_called()
        mock_put_object.assert_called_once()

if __name__ == '__main__':
    unittest.main()
