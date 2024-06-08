import os
import zipfile
from io import BytesIO
from typing import List
from minio import Minio
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
        self._spark = SparkSession.builder \
            .appName("MinIODatalake") \
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.access.key", access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint}") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.jars.packages", "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.1.1,org.apache.hadoop:hadoop-common:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4") \
            .getOrCreate()

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
        zip_buffer = BytesIO()
        zip_data = minio_object.client.get_object(minio_object.bucket_name, minio_object.object_name).read()
        zip_buffer.write(zip_data)
        zip_buffer.seek(0)

        with zipfile.ZipFile(zip_buffer, 'r') as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise ValueError("No CSV files found in the ZIP archive.")

            csv_file = csv_files[0]  # Assuming we want the first CSV file found
            csv_path = f"s3a://{minio_object.bucket_name}/{csv_file}"

            with zip_ref.open(csv_file) as csv_file_ref:
                csv_data = csv_file_ref.read()
                temp_csv_path = f"/tmp/{csv_file}"
                with open(temp_csv_path, 'wb') as temp_csv:
                    temp_csv.write(csv_data)

            df = self.spark.read.format("csv").option("header", "true").load(temp_csv_path)
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
