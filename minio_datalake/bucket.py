# arquivo bucket.py

import re
from typing import Iterator, Optional, List, BinaryIO

from minio import Minio
from minio.datatypes import Object
from minio.helpers import ObjectWriteResult

from minio_datalake.utils import MinIOUtils


class MinIOBucket:
    """
    Class to represent a MinIO bucket.

    Attributes:
    client: Instance of the MinIO client.
    bucket_name: Name of the bucket.
    """
    def __init__(self, client: Minio, bucket_name: str):
        self._client = client
        self._bucket_name = bucket_name

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    def make(self, *args, **kwargs):
        """
        Create a bucket if it does not exist, ensuring the name is valid.
        """
        if not MinIOUtils.validate_bucket_name(self._bucket_name):
            raise ValueError(f"Invalid bucket name: {self._bucket_name}")

        self._client.make_bucket(self._bucket_name, *args, **kwargs)

    def remove(self):
        self._client.remove_bucket(self._bucket_name)

    def put_object(
        self,
        object_name: str,
        data: BinaryIO,
        length: int, *args, **kwargs)-> ObjectWriteResult:

        return self._client.put_object(self._bucket_name, object_name, data, length, *args, **kwargs)

    def remove_object(
        self,
        object_name: str, *args, **kwargs
    ):
        self._client.remove_object(self._bucket_name, object_name, *args, **kwargs)

    def exists(self) -> bool:
        """
        Check if the bucket exists.

        Returns:
        bool: True if the bucket exists, False otherwise.
        """
        return self._client.bucket_exists(self._bucket_name)

    def list_objects(self, prefix: Optional[str] = None, recursive: bool = False) -> Iterator[Object]:
        """
        List objects in the bucket.

        Parameters:
        prefix (str): Prefix of the objects.
        recursive (bool): If True, list objects recursively.

        Returns:
        Iterator[Object]: Iterator of objects.
        """
        return self._client.list_objects(self._bucket_name, prefix=prefix, recursive=recursive)

    def list_object_names(self, prefix: Optional[str] = None, recursive: bool = False) -> List[str]:
        """
        List names of objects in the bucket.

        Parameters:
        prefix (str): Prefix of the objects.
        recursive (bool): If True, list objects recursively.

        Returns:
        List[str]: List of object names.
        """
        objects = self.list_objects(prefix=prefix, recursive=recursive)
        return [obj.object_name for obj in objects]
