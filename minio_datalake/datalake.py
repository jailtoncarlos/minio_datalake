import os
import zipfile
from io import BytesIO
from typing import List
from minio import Minio
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
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

        # Configure Spark to use MinIO
        conf = SparkConf().setAppName("MinIODatalake")
        for key, value in settings.SPARK_CONF.items():
            conf.set(key, value)
        self._spark = SparkSession.builder.config(conf=conf).getOrCreate()

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

    def read_csv_from_zip(self, minio_object: MinIOObject) -> DataFrame:
        """
        Read a CSV file from a ZIP archive in MinIO and return a Spark DataFrame.

        Parameters:
        minio_object (MinIOObject): MinIOObject representing the ZIP file.

        Returns:
        DataFrame: Spark DataFrame.
        """
        extracted_objects = self.unzip(minio_object, extract_to_bucket=False)
        csv_files = [obj for obj in extracted_objects if obj.object_name.endswith('.csv')]

        if not csv_files:
            raise ValueError("No CSV files found in the ZIP archive.")

        df = self.spark.read.format("csv").option("header", "true").load([f"s3a://{obj.bucket_name}/{obj.object_name}" for obj in csv_files])
        return df

    def read_csv_to_dataframe(self, minio_object: MinIOObject) -> DataFrame:
        """
        Read a CSV file from MinIO and return a Spark DataFrame.

        Parameters:
        minio_object (MinIOObject): MinIOObject representing the CSV file.

        Returns:
        DataFrame: Spark DataFrame.
        """
        csv_path = f"s3a://{minio_object.bucket_name}/{minio_object.object_name}"
        df = self.spark.read.options(header='true').csv(csv_path)
        return df

    def read_parquet_to_dataframe(self, minio_object: MinIOObject) -> DataFrame:
        """
        Read a Parquet file from MinIO and return a Spark DataFrame.

        Parameters:
        minio_object (MinIOObject): MinIOObject representing the Parquet file.

        Returns:
        DataFrame: Spark DataFrame.
        """
        parquet_path = f"s3a://{minio_object.bucket_name}/{minio_object.object_name}"
        df = self.spark.read.parquet(parquet_path)
        return df

    def dataframe_to_parquet(self, df: DataFrame, path: str) -> str:
        """
        Convert a Spark DataFrame to Parquet and save it to MinIO.

        Parameters:
        df (DataFrame): Spark DataFrame.
        path (str): Path to save the Parquet file.

        Returns:
        str: Path to the Parquet file in MinIO.
        """
        parquet_path = f"s3a://{path}"
        df.write.mode('overwrite').parquet(parquet_path)
        return path

    def ingest_csv_to_datalake(self, minio_object: MinIOObject, destination_path: str = 'stage', temp_view_name: str = None, delimiter=',') -> DataFrame:
        """
        Ingest a CSV file to the MinIO DataLake, converting it to Parquet and creating a temporary view in Spark.

        Parameters:
        minio_object (MinIOObject): MinIOObject representing the CSV file.
        destination_path (str): Path to the destination directory in MinIO.
        temp_view_name (str): Name of the temporary view in Spark.
        delimiter (str): CSV delimiter.

        Returns:
        DataFrame: Spark DataFrame.
        """
        parquet_path = f'{destination_path}/{os.path.splitext(minio_object.object_name)[0]}.parquet'

        bucket = self.get_bucket(destination_path)
        if not bucket.exists():
            bucket.make()

        df = self.read_csv_to_dataframe(minio_object)
        self.dataframe_to_parquet(df, f'{parquet_path}')

        df = self.spark.read.parquet(f's3a://{parquet_path}')

        if temp_view_name is None:
            temp_view_name = os.path.splitext(minio_object.object_name)[0]
        df.createOrReplaceTempView(temp_view_name)

        return df

    def unzip(self, minio_object: MinIOObject, extract_to_bucket: bool = False) -> List[MinIOObject]:
        """
        Unzip a file in the MinIO bucket.

        Parameters:
        minio_object (MinIOObject): MinIOObject representing the zip file.
        extract_to_bucket (bool): If True, extract to the root of the bucket. If False, extract to a subdirectory with the name of the zip file.

        Returns:
        list: List of MinIOObjects for the extracted files.
        """
        zip_buffer = BytesIO()
        zip_data = self.client.get_object(minio_object.bucket_name, minio_object.object_name).read()
        zip_buffer.write(zip_data)
        zip_buffer.seek(0)

        destination_path = f'{minio_object.bucket_name}/{os.path.splitext(minio_object.object_name)[0]}' if not extract_to_bucket else minio_object.bucket_name

        extracted_objects = []
        with zipfile.ZipFile(zip_buffer, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                file_data = zip_ref.read(file_name)
                file_path = f'{destination_path}/{file_name}'
                self.client.put_object(minio_object.bucket_name, file_path, BytesIO(file_data), len(file_data))
                extracted_objects.append(MinIOObject(self.client, minio_object.bucket_name, file_path))

        return extracted_objects
