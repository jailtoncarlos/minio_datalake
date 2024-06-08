import unittest
from unittest.mock import patch, MagicMock
from minio_datalake.client import MinIOClient
from urllib3.response import HTTPResponse
from minio.datatypes import Object

class TestMinIOClient(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = MinIOClient('localhost:9000', 'access_key', 'secret_key')

    @patch('minio.Minio.bucket_exists')
    def test_bucket_exists(self, mock_bucket_exists):
        mock_bucket_exists.return_value = True
        self.assertTrue(self.client.bucket_exists('test_bucket'))

    @patch('minio.Minio.fget_object')
    def test_fget_object(self, mock_fget_object):
        self.client.fget_object('test_bucket', 'test_object', '/path/to/file')
        mock_fget_object.assert_called_once_with('test_bucket', 'test_object', '/path/to/file')

    @patch('minio.Minio.fput_object')
    def test_fput_object(self, mock_fput_object):
        self.client.fput_object('test_bucket', 'test_object', '/path/to/file')
        mock_fput_object.assert_called_once_with('test_bucket', 'test_object', '/path/to/file')

    @patch('minio.Minio.get_object')
    def test_get_object(self, mock_get_object):
        mock_get_object.return_value = MagicMock(spec=HTTPResponse)
        response = self.client.get_object('test_bucket', 'test_object')
        mock_get_object.assert_called_once_with('test_bucket', 'test_object')
        self.assertIsInstance(response, HTTPResponse)

    @patch('minio.Minio.stat_object')
    def test_stat_object(self, mock_stat_object):
        mock_stat_object.return_value = MagicMock(spec=Object)
        response = self.client.stat_object('test_bucket', 'test_object')
        mock_stat_object.assert_called_once_with('test_bucket', 'test_object')
        self.assertIsNotNone(response)

    @patch('minio.Minio.list_objects')
    def test_list_objects(self, mock_list_objects):
        mock_list_objects.return_value = [
            MagicMock(object_name='obj1'),
            MagicMock(object_name='obj2')
        ]
        objects = self.client.list_objects('test_bucket')
        mock_list_objects.assert_called_once_with('test_bucket', prefix=None, recursive=False)
        self.assertEqual(len(list(objects)), 2)

if __name__ == '__main__':
    unittest.main()
