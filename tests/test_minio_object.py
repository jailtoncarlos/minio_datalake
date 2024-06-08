import unittest
from io import BytesIO
from minio_datalake.client import MinIOClient
from minio_datalake.datalake import MinIOSparkDatalake
from minio_datalake.object import MinIOObject
import minio_datalake.settings as settings

class TestMinIOObject(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.datalake = MinIOSparkDatalake(settings.MINIO_ENDPOINT, settings.MINIO_ACCESS_KEY, settings.MINIO_SECRET_KEY,
                                          settings.MINIO_USE_SSL)
        cls.bucket = cls.datalake.get_bucket('test-bucket')
        cls.object = cls.datalake.get_object('test-bucket', 'test-object')

        # Ensure the bucket exists
        if not cls.bucket.exists():
            cls.bucket.make()

        # Create a test object in MinIO
        data = BytesIO(b'Hello World')
        cls.object.put(data)

    @classmethod
    def tearDownClass(cls):
        cls.client.remove_object(cls.bucket_name, cls.object_name)
        cls.client.remove_bucket(cls.bucket_name)

    def setUp(self):
        self.obj = MinIOObject(self.client, self.bucket_name, self.object_name)

    def test_exists(self):
        with unittest.mock.patch.object(self.client, 'stat_object', wraps=self.client.stat_object) as mock_stat_object:
            self.assertTrue(self.obj.exists())
            mock_stat_object.assert_called_once_with(self.bucket_name, self.object_name)

    def test_get(self):
        with unittest.mock.patch.object(self.client, 'get', wraps=self.client.get) as mock_get:
            response = self.obj.get()
            mock_get.assert_called_once_with(self.bucket_name, self.object_name)
            self.assertIsNotNone(response)

    def test_put(self):
        data = BytesIO(b'New Data')
        with unittest.mock.patch.object(self.client, 'put', wraps=self.client.put) as mock_put:
            self.obj.put(data)
            mock_put.assert_called_once_with(self.bucket_name, self.object_name, data)

if __name__ == '__main__':
    unittest.main()
