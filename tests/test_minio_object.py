# arquivo test_minio_object.py
import unittest
from io import BytesIO

from minio_spark import MinIOSpark
from minio_spark.object import MinioObject


class TestMinIOObject(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.datalake = MinIOSpark()
        cls.bucket_name = 'test-bucket'
        cls.object_name = 'test-object'
        cls.bucket = cls.datalake.get_bucket(cls.bucket_name)
        cls.object = cls.datalake.get_object(cls.bucket_name, cls.object_name)

        # Ensure the bucket exists
        if not cls.bucket.exists():
            cls.bucket.make()

        # Create a test object in MinIO
        data = BytesIO(b'Hello World')
        cls.object.put(data, length=len(data.getvalue()))

    @classmethod
    def tearDownClass(cls):
        cls.object.remove()
        cls.bucket.remove()

    def test_exists(self):
        self.assertTrue(self.object.exists())

    def test_put(self):
        data = BytesIO(b'New Data')
        result = self.object.put(data, length=len(data.getvalue()))
        self.assertIsNotNone(result)

    def test_remove(self):
        data = BytesIO(b'Temporary Data')
        temp_object = MinioObject(self.object.client, self.bucket_name, 'temp-object')
        temp_object.put(data, length=len(data.getvalue()))
        self.assertTrue(temp_object.exists())
        temp_object.remove()
        self.assertFalse(temp_object.exists())

    def test_fget_fput(self):
        with open('/tmp/testfile.txt', 'w') as f:
            f.write('This is a test file.')

        result = self.object.fput('/tmp/testfile.txt')
        self.assertIsNotNone(result)

        self.object.fget('/tmp/downloaded_testfile.txt')

        with open('/tmp/downloaded_testfile.txt', 'r') as f:
            content = f.read()
            self.assertEqual(content, 'This is a test file.')


if __name__ == '__main__':
    unittest.main()
