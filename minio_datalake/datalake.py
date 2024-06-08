import os
import zipfile
from io import BytesIO

from minio import Minio
from pyspark.sql import SparkSession
from .bucket import MinIOBucket
from .object import MinIOObject
from .utils import split_minio_path
import minio_datalake.settings as settings

import logging

logger = logging.getLogger(__name__)

class MinIOSparkDatalake:
    """
    Main class for interacting with the MinIO DataLake.

    Attributes:
    client: Instance of the MinIO client.
    spark: Spark session.
    """

    def __init__(self, endpoint=settings.MINIO_ENDPOINT, access_key=settings.MINIO_ACCESS_KEY, secret_key=settings.MINIO_SECRET_KEY, use_ssl=settings.MINIO_USE_SSL) -> None:
        self._client = Minio(endpoint, access_key, secret_key, secure=use_ssl)
        self._spark = SparkSession.builder.appName("MinIODatalake").getOrCreate()

    @property
    def client(self) -> Minio:
        """
        Get the MinIO client instance.

        Returns:
        MinIOClient: MinIO client.
        """
        return self._client

    @property
    def spark(self) -> SparkSession:
        return self._spark

    def get_bucket(self, bucket_name: str) -> MinIOBucket:
        """
        Get a MinIO bucket instance.

        Parameters:
        bucket_name (str): Name of the bucket.

        Returns:
        MinIOBucket: Bucket instance.
        """
        return MinIOBucket(self.client, bucket_name)

    def get_object(self, bucket_name: str, object_name: str) -> MinIOObject:
        """
        Get a MinIO object instance.

        Parameters:
        bucket_name (str): Name of the bucket.
        object_name (str): Name of the object.

        Returns:
        MinIOObject: Object instance.
        """
        return MinIOObject(self.client, bucket_name, object_name)

    def extract_zip_to_datalake(self, path: str, extract_to_bucket: bool = False) -> str:
        """
        Extract a zip file from MinIO and upload its contents to the destination path in MinIO.

        Parameters:
        path (str): Path to the zip file in MinIO.
        extract_to_bucket (bool): If True, extract to the root of the bucket. If False, extract to a subdirectory with the name of the zip file.

        Returns:
        str: Path to the extracted files in MinIO.
        """
        bucket_name, object_name = split_minio_path(path)
        local_zip_path = BytesIO()
        self.client.fget_object(bucket_name, object_name, local_zip_path)
        local_zip_path.seek(0)  # Reset buffer position to the beginning

        destination_path = f'{bucket_name}/{os.path.splitext(object_name)[0]}' if not extract_to_bucket else bucket_name

        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                file_data = zip_ref.read(file_name)
                file_path = f'{destination_path}/{file_name}'
                self.client.put_object(bucket_name, file_path, BytesIO(file_data), len(file_data))

        return destination_path

    def read_csv_to_dataframe(self, path: str, delimiter=','):
        """
        Read a CSV file from MinIO and return a Spark DataFrame.

        Parameters:
        path (str): Path to the CSV file.
        delimiter (str): CSV delimiter.

        Returns:
        DataFrame: Spark DataFrame.
        """
        bucket_name, object_name = split_minio_path(path)
        csv_data = BytesIO()
        self.client.fget_object(bucket_name, object_name, csv_data)
        csv_data.seek(0)  # Reset buffer position to the beginning
        return self.spark.read.options(delimiter=delimiter, header=True).csv(csv_data)

    def dataframe_to_parquet(self, df, path: str) -> str:
        """
        Convert a Spark DataFrame to Parquet and save it to MinIO.

        Parameters:
        df (DataFrame): Spark DataFrame.
        path (str): Path to save the Parquet file.

        Returns:
        str: Path to the Parquet file in MinIO.
        """
        bucket_name, object_name = split_minio_path(path)
        local_parquet_path = BytesIO()
        df.write.parquet(local_parquet_path)
        local_parquet_path.seek(0)  # Reset buffer position to the beginning
        self.client.put_object(bucket_name, object_name, local_parquet_path, len(local_parquet_path.getvalue()))
        return f'{bucket_name}/{object_name}'

    def ingest_csv_to_datalake(self, path: str, destination_path: str = 'stage', temp_view_name: str = None, delimiter=','):
        """
        Ingest a CSV file to the MinIO DataLake, converting it to Parquet and creating a temporary view in Spark.

        Parameters:
        path (str): Path to the CSV file.
        destination_path (str): Path to the destination directory in MinIO.
        temp_view_name (str): Name of the temporary view in Spark.
        delimiter (str): CSV delimiter.

        Returns:
        DataFrame: Spark DataFrame.
        """
        bucket_name, object_name = split_minio_path(path)
        parquet_path = f'{destination_path}/{os.path.splitext(object_name)[0]}.parquet'

        bucket = self.get_bucket(destination_path)
        if not bucket.exists():
            bucket.create()

        df = self.read_csv_to_dataframe(path, delimiter)
        self.dataframe_to_parquet(df, f'/{parquet_path}')

        df = self.spark.read.parquet(parquet_path)

        if temp_view_name is None:
            temp_view_name = os.path.splitext(object_name)[0]
        df.createOrReplaceTempView(temp_view_name)

        return df
