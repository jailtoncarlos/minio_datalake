from typing import Optional, Iterator
from minio import Minio
from minio_spark.bucket import MinioBucket
from minio_spark.object import MinioObject


class MinioClient(Minio):
    def __init__(
            self,
            endpoint: str,
            access_key: Optional[str] = None,
            secret_key: Optional[str] = None,
            secure: bool = True,
            *args, **kargs
    ):
        super().__init__(endpoint=endpoint,
                         access_key=access_key,
                         secret_key=secret_key,
                         secure=secure,
                         *args, **kargs,
                         )

    def __str__(self):
        return f"MinioClient({self._base_url}, {self._provider.retrieve.access_key})"

    def list_buckets(self) -> list[MinioBucket]:
        """Overrides the list_buckets method to return MinioBucket."""
        buckets = super().list_buckets()
        return [MinioBucket(self, bucket.name, bucket.creation_date) for bucket in buckets]

    def list_objects(self, bucket_name: str, prefix: Optional[str] = None, recursive: bool = False) -> Iterator[MinioObject]:
        """Lists objects in a bucket and returns MinioObject."""
        objects = super().list_objects(bucket_name, prefix=prefix, recursive=recursive)
        for obj in objects:
            yield MinioObject(self, obj.bucket_name, obj.object_name, obj.last_modified, obj.etag, obj.size, obj.content_type)