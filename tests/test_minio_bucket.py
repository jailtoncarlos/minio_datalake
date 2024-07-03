# arquivo test_minio_bucket.py
import unittest
from io import BytesIO

from minio_spark import MinIOSpark


class TestMinIOBucket(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        try:
            cls.datalake = MinIOSpark()
            cls.bucket_name = 'test-bucket'  # Ajuste o nome do bucket para estar em conformidade
            cls.bucket = cls.datalake.get_bucket(cls.bucket_name)

            # Ensure the bucket exists
            if not cls.bucket.exists():
                cls.bucket.make()

            # Create test objects in MinIO
            data1 = BytesIO(b'Hello')
            cls.bucket.put_object('obj1', data1, length=len(data1.getvalue()))
            data2 = BytesIO(b'World')
            cls.bucket.put_object('obj2', data2, length=len(data2.getvalue()))
        except Exception as e:
            print(f"Error setting up the test class: {e}")
            raise

    @classmethod
    def tearDownClass(cls):
        try:
            cls.bucket.remove_object('obj1')
            cls.bucket.remove_object('obj2')
            cls.bucket.remove()
        except Exception as e:
            print(f"Error tearing down the test class: {e}")
            raise

    def test_bucket_exists(self):
        """Test if the bucket exists."""
        self.assertTrue(self.bucket.exists())

    def test_put_object(self):
        """Test putting an object in the bucket."""
        data = BytesIO(b'Test data')
        result = self.bucket.put_object('test_put_object', data, length=len(data.getvalue()))
        self.assertIsNotNone(result)
        self.bucket.remove_object('test_put_object')

    def test_list_objects(self):
        """Test listing objects in the bucket."""
        objects = list(self.bucket.list_objects())
        self.assertEqual(len(objects), 2)
        object_names = [obj.object_name for obj in objects]
        self.assertIn('obj1', object_names)
        self.assertIn('obj2', object_names)

    def test_list_object_names(self):
        """Test listing object names in the bucket."""
        object_names = self.list_object_names()
        self.assertEqual(len(object_names), 2)
        self.assertIn('obj1', object_names)
        self.assertIn('obj2', object_names)

    def test_remove_object(self):
        """Test removing an object from the bucket."""
        data = BytesIO(b'Test remove data')
        self.bucket.put_object('test_remove_object', data, length=len(data.getvalue()))
        self.assertIn('test_remove_object', self.list_object_names())
        self.bucket.remove_object('test_remove_object')
        self.assertNotIn('test_remove_object', self.list_object_names())

    def list_object_names(self):
        objects = self.bucket.list_objects()
        return [obj.object_name for obj in objects]

if __name__ == '__main__':
    unittest.main()
