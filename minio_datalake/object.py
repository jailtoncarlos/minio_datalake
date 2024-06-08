import urllib3
from minio.helpers import ObjectWriteResult
from minio_datalake.bucket import MinIOBucket

try:
    from urllib3.response import BaseHTTPResponse  # type: ignore[attr-defined]
except ImportError:
    from urllib3.response import HTTPResponse as BaseHTTPResponse

class MinIOObject:
    """
    Class to represent a MinIO object.

    Attributes:
    client: Instance of the MinIO client.
    bucket_name: Name of the bucket.
    object_name: Name of the object.
    """
    def __init__(self, client: MinIOBucket, bucket_name: str, object_name: str):
        self.client = client
        self.bucket_name = bucket_name
        self.object_name = object_name

    def get(self) -> BaseHTTPResponse:
        """
        Get an object from the bucket.

        Returns:
        Object: Object from the bucket.
        """
        return self.client.get_object(self.bucket_name, self.object_name)

    def put(self, file_path: str) -> ObjectWriteResult:
        """
        Upload a file to the bucket.

        Parameters:
        file_path (str): Path to the file.
        """
        self.client.fput_object(self.bucket_name, self.object_name, file_path)

    def exists(self) -> bool:
        """
        Check if the object exists in the bucket.

        Returns:
        bool: True if the object exists, False otherwise.
        """
        try:
            self.client.get_object(self.bucket_name, self.object_name)
            return True
        except:
            return False
