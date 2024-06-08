from typing import BinaryIO

import urllib3
from minio import Minio
from minio.helpers import ObjectWriteResult
from urllib3.response import HTTPResponse
from minio_datalake.bucket import MinIOBucket


class MinIOObject:
    """
    Class to represent a MinIO object.

    Attributes:
    client: Instance of the MinIO client.
    bucket_name: Name of the bucket.
    object_name: Name of the object.
    """
    def __init__(self, client: Minio, bucket_name: str, object_name: str) -> None:
        self._client = client
        self._bucket_name = bucket_name
        self._object_name = object_name

    @property
    def client(self):
        return self.client

    @property
    def bucket_name(self):
        return self._bucket_name

    @property
    def object_name(self):
        return self._object_name

    def put(
        self,
        data: BinaryIO,
        length: int, *args, **kwargs) -> ObjectWriteResult:
        """
        :param data: An object having callable read() returning bytes object.
        :param length: Data size; -1 for unknown size and set valid part_size.

        :return: :class:`ObjectWriteResult` object.
        """
        return self.client.put_object(self.bucket_name, self._object_name, data, length, *args, **kwargs)

    def fget(self, file_path: str, *args, **kwargs):
        """
        Downloads data of an object to file.

        :param file_path: Name of file to download.

        """
        return self.client.fget_object(self.bucket_name, self.object_name, file_path, *args, **kwargs)

    def fput(self, file_path: str, *args, **kwargs) -> ObjectWriteResult:
        """
        Uploads data from a file to an object in a bucket.

        :param:
        file_path (str): Path to the file.

        :return: :class:`ObjectWriteResult` object.
        """
        return self.client.fput_object(self.bucket_name, self.object_name, file_path, *args, **kwargs)

    def exists(self) -> bool:
        """
        Check if the object exists in the bucket.

        Returns:
        bool: True if the object exists, False otherwise.
        """
        try:
            self.client.stat_object(self.bucket_name, self.object_name)
            return True
        except:
            return False
