from typing import BinaryIO, Iterator, Optional
from datetime import datetime
from minio.datatypes import Bucket
from minio.helpers import ObjectWriteResult
from minio.time import from_iso8601utc

from minio_spark.object import MinioObject
from minio_spark.utils import MinIOUtils


class MinioBucket(Bucket):
    """
    Class to represent a MinIO bucket.

    Attributes:
    client: Instance of the MinIO client.
    bucket_name: Name of the bucket.
    """

    def __init__(self, client, name: str, creation_date: Optional[datetime] = None):
        from minio_spark.client import MinioClient  # Local import to avoid circular import
        if not isinstance(client, MinioClient):
            raise ValueError("client must be an instance of MinioClient")

        self._client = client
        super().__init__(name, creation_date or datetime.now())

    def __str__(self):
        return f"MinioBucket({self._name}, {self._creation_date})"

    def make(self, *args, **kwargs):
        """
        Create a bucket if it does not exist, ensuring the name is valid.
        """
        if not MinIOUtils.validate_bucket_name(self._name):
            raise ValueError(f"Invalid bucket name: {self._name}")

        self._client.make_bucket(self._name, *args, **kwargs)

    def remove(self):
        self._client.remove_bucket(self._name)

    def put_object(
            self,
            object_name: str,
            data: BinaryIO,
            length: int, *args, **kwargs) -> ObjectWriteResult:
        return self._client.put_object(self._name, object_name, data, length, *args, **kwargs)

    def remove_object(
            self,
            object_name: str, *args, **kwargs
    ):
        self._client.remove_object(self._name, object_name, *args, **kwargs)

    def exists(self) -> bool:
        """
        Check if the bucket exists.

        Returns:
        bool: True if the bucket exists, False otherwise.
        """
        return self._client.bucket_exists(self._name)

    def list_objects(self, prefix: Optional[str] = None, recursive: bool = False) -> Iterator[MinioObject]:
        """
        List objects in the bucket.

        Parameters:
        prefix (str): Prefix of the objects.
        recursive (bool): If True, list objects recursively.

        Returns:
        Iterator[MinioObject]: Iterator of objects.
        """
        return self._client.list_objects(self._name, prefix=prefix, recursive=recursive)


def get_object(self, object_name: str) -> MinioObject:
    """
    Get an object by its path.

    Parameters:
    object_path (str): Path of the object to retrieve.

    Returns:
    MinioObject: The retrieved object.
    """
    obj = self._client.get_object(self.name, object_name)
    return MinioObject(self._client, self.name, object_name, obj.last_modified, obj.etag, obj.length, obj.content_type)
