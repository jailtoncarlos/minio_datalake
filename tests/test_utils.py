import unittest
from minio_datalake.utils import split_minio_path

class TestUtils(unittest.TestCase):

    def test_split_minio_path(self):
        bucket_name, object_name = split_minio_path('/test_bucket/test_object')
        self.assertEqual(bucket_name, 'test_bucket')
        self.assertEqual(object_name, 'test_object')

if __name__ == '__main__':
    unittest.main()
