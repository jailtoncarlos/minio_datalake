from typing import Optional, BinaryIO
from datetime import datetime
from minio.datatypes import Object
from minio.helpers import ObjectWriteResult

class MinioObject(Object):
    """
    Class to represent a MinIO object.

    Attributes:
    client: Instance of the MinIO client.
    bucket_name: Name of the bucket.
    object_name: Name of the object.
    """

    def __init__(self, client,
                 bucket_name: str,
                 object_name: Optional[str],
                 last_modified: Optional[datetime] = None,
                 etag:  Optional[str] = None,
                 size:  Optional[int] = None,
                 content_type:  Optional[str] = None,
                 *args, **kwargs) -> None:

        from minio_spark.client import MinioClient  # Local import to avoid circular import
        if not isinstance(client, MinioClient):
            raise ValueError("client must be an instance of MinioClient")

        super().__init__(bucket_name, object_name, last_modified, etag, size, content_type, *args, **kwargs)
        self._client = client

    def __str__(self):
        return f"MinioObject({self._bucket_name}, {self._object_name}, {self._last_modified}, {self._etag}, {self._size}, {self._content_type})"

    @property
    def client(self):
        return self._client

    @property
    def name(self):
        return self._object_name

    def put(self, data: BinaryIO, length: int, *args, **kwargs) -> ObjectWriteResult:
        """
        :param data: An object having callable read() returning bytes object.
        :param length: Data size; -1 for unknown size and set valid part_size.

        :return: :class:`ObjectWriteResult` object.
        """
        return self.client.put_object(self.bucket_name, self._object_name, data, length, *args, **kwargs)

    def remove(self, *args, **kwargs):
        """
        Remove an object.
        """
        self.client.remove_object(self.bucket_name, self._object_name, *args, **kwargs)

    def fget(self, file_path: str, *args, **kwargs):
        """
        Downloads data of an object to file.

        :param file_path: Name of file to download.
        """
        self.client.fget_object(self.bucket_name, self.name, file_path, *args, **kwargs)

    def fput(self, file_path: str, *args, **kwargs) -> ObjectWriteResult:
        """
        Uploads data from a file to an object in a bucket.

        :param:
        file_path (str): Path to the file.

        :return: :class:`ObjectWriteResult` object.
        """
        return self.client.fput_object(self.bucket_name, self.name, file_path, *args, **kwargs)

    def exists(self) -> bool:
        """
        Check if the object exists in the bucket.

        Returns:
        bool: True if the object exists, False otherwise.
        """
        try:
            self.client.stat_object(self.bucket_name, self.name)
            return True
        except:
            return False

    def rename(self, new_object_name: str) -> 'MinioObject':
        """
        Rename the object in the MinIO bucket.

        Parameters:
        new_object_name (str): New name for the object.

        Returns:
        MinioObject: New MinioObject with the new name.
        """
        # Copy the current object to the new name
        copy_result = self.client.copy_object(
            self.bucket_name, new_object_name, f'/{self.bucket_name}/{self.name}'
        )

        # Remove the original object
        self.remove()

        # Return a new MinioObject representing the renamed object
        return MinioObject(self.client, self.bucket_name, new_object_name, copy_result.last_modified,
                           copy_result.etag, copy_result.size, copy_result.content_type)
