from minio import Minio
from minio.datatypes import Bucket, Object
from typing import List, Iterator
from urllib3.response import HTTPResponse


class MinIOClient:
    """
    Class to represent a MinIO client.

    Attributes:
    client: Instance of the MinIO client.
    """

    def __init__(self, endpoint: str, access_key: str, secret_key: str) -> None:
        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

    def bucket_exists(self, bucket_name: str) -> bool:
        """
        Check if a bucket exists.

        Parameters:
        bucket_name (str): Name of the bucket.

        Returns:
        bool: True if the bucket exists, False otherwise.
        """
        return self.client.bucket_exists(bucket_name)

    def make_bucket(self, bucket_name: str) -> None:
        """
        Create a bucket if it does not exist.

        Parameters:
        bucket_name (str): Name of the bucket.
        """
        if not self.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)

    def list_buckets(self) -> List[Bucket]:
        """
        List all buckets.

        Returns:
        list[Bucket]: List of buckets.
        """
        return self.client.list_buckets()

    def fput_object(self, bucket_name: str, object_name: str, file_path: str) -> None:
        """
        Upload a file to a bucket.

        Parameters:
        bucket_name (str): Name of the bucket.
        object_name (str): Name of the object.
        file_path (str): Path to the file.
        """
        self.client.fput_object(bucket_name, object_name, file_path)

    def fget_object(self, bucket_name: str, object_name: str, file_path: str) -> None:
        """
        Download a file from a bucket.

        Parameters:
        bucket_name (str): Name of the bucket.
        object_name (str): Name of the object.
        file_path (str): Path to save the file.
        """
        self.client.fget_object(bucket_name, object_name, file_path)

    def get_object(self, bucket_name: str, object_name: str) -> HTTPResponse:
        """
        Get an object from a bucket.

        Parameters:
        bucket_name (str): Name of the bucket.
        object_name (str): Name of the object.

        Returns:
        HTTPResponse: Object from the bucket.
        """
        return self.client.get_object(bucket_name, object_name)

    def stat_object(self, bucket_name: str, object_name: str) -> Object:
        """
        Get metadata of an object.

        Parameters:
        bucket_name (str): Name of the bucket.
        object_name (str): Name of the object.

        Returns:
        Object: Metadata of the object.
        """
        return self.client.stat_object(bucket_name, object_name)

    def list_objects(self, bucket_name: str, prefix: str = None, recursive: bool = False) -> Iterator[Object]:
        """
        List objects in a bucket.

        Parameters:
        bucket_name (str): Name of the bucket.
        prefix (str): Prefix of the objects to list.
        recursive (bool): Whether to list objects recursively.

        Returns:
        Iterator[Object]: Iterator of objects.
        """
        return self.client.list_objects(bucket_name, prefix=prefix, recursive=recursive)
