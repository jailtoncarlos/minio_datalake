import zipfile
import os
from pyspark.sql import SparkSession, DataFrame
from .client import MinIOClient
from .bucket import MinIOBucket
from .object import MinIOObject
from .utils import split_file_path
import minio_datalake.settings as settings

import logging

logger = logging.getLogger(__name__)


class MinIODatalake:
    """
    Main class for interacting with the MinIO DataLake.

    Attributes:
    client: Instance of the MinIO client.
    spark: Spark session.
    """

    def __init__(self, endpoint=settings.MINIO_ENDPOINT, access_key=settings.MINIO_ACCESS_KEY,
                 secret_key=settings.MINIO_SECRET_KEY):
        self.client = MinIOClient(endpoint, access_key, secret_key)
        self.spark = SparkSession.builder.appName("MinIODatalake").getOrCreate()

    def get_client(self):
        """
        Get the MinIO client instance.

        Returns:
        MinIOClient: MinIO client.
        """
        return self.client

    def get_bucket(self, bucket_name):
        """
        Get a MinIO bucket instance.

        Parameters:
        bucket_name (str): Name of the bucket.

        Returns:
        MinIOBucket: Bucket instance.
        """
        return MinIOBucket(self.client, bucket_name)

    def get_object(self, bucket_name, object_name):
        """
        Get a MinIO object instance.

        Parameters:
        bucket_name (str): Name of the bucket.
        object_name (str): Name of the object.

        Returns:
        MinIOObject: Object instance.
        """
        return MinIOObject(self.client, bucket_name, object_name)

    def unzip_file(self, bucket_name: str, object_name: str, destination_path: str = None) -> str:
        """
        Unzip a .zip file stored in a MinIO bucket to a specified destination.

        Parameters:
        bucket_name (str): Name of the bucket.
        object_name (str): Name of the .zip object.
        destination_path (str): Path to the destination directory.

        Returns:
        str: Path to the subdirectory where files are extracted.
        """
        base_dir, file_name_without_extension, _ = split_file_path(object_name)
        if destination_path is None:
            destination_path = f'{base_dir}/{file_name_without_extension}'

        # Download the zip file from MinIO
        zip_path = f'/tmp/{object_name}'
        self.client.fget_object(bucket_name, object_name, zip_path)

        # Unzip the file
        os.makedirs(destination_path, exist_ok=True)
        print(f'Unzipping {destination_path} ...')
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(destination_path)

        return destination_path

    def read_csv_to_dataframe(self, bucket_name: str, path: str, delimiter: str = ',') -> DataFrame:
        """
        Read a CSV file from a MinIO bucket and return a Spark DataFrame.

        Parameters:
        bucket_name (str): Name of the bucket.
        path (str): Path to the CSV file in the bucket.
        delimiter (str): CSV delimiter.

        Returns:
        DataFrame: Spark DataFrame.
        """
        csv_path = f'/tmp/{os.path.basename(path)}'
        self.client.fget_object(bucket_name, path, csv_path)
        return self.spark.read.options(delimiter=delimiter, header=True).csv(csv_path)

    def csv_to_parquet(self, dataframe: DataFrame, destination_path: str, file_name_without_extension: str):
        """
        Convert a DataFrame from CSV to Parquet and upload it to MinIO.

        Parameters:
        dataframe (DataFrame): Spark DataFrame to be converted.
        destination_path (str): Path to the destination directory in MinIO.
        file_name_without_extension (str): File name without extension for the Parquet file.
        """
        filename_parquet = f'{destination_path}/{file_name_without_extension}.parquet'
        local_parquet_path = f'/tmp/{file_name_without_extension}.parquet'

        # Save as Parquet locally
        print(f'Saving Parquet {local_parquet_path} ...')
        dataframe.write.parquet(local_parquet_path)

        # Upload the Parquet file to MinIO
        self.client.fput_object(destination_path, filename_parquet, local_parquet_path)
        print(f'Parquet file {filename_parquet} uploaded to MinIO.')

    def ingest_csv_to_datalake(self, filename_path: str, destination_path: str = 'stage', temp_view_name: str = None,
                               delimiter: str = ',') -> DataFrame:
        """
        Ingest a CSV file into the MinIO DataLake, converting it to Parquet and creating a temporary view in Spark.

        Parameters:
        filename_path (str): Path to the CSV file.
        destination_path (str): Path to the destination directory in the DataLake.
        temp_view_name (str): Name of the temporary view in Spark.
        delimiter (str): CSV delimiter.

        Returns:
        DataFrame: Spark DataFrame.
        """
        base_dir, file_name_without_extension, file_extension = split_file_path(filename_path)
        filename_parquet = f'{destination_path}/{file_name_without_extension}.parquet'

        # Create the destination bucket if it does not exist
        bucket = self.get_bucket(destination_path)
        bucket.create()

        # Unzip the file if necessary
        destination_object = self.get_object(destination_path, file_name_without_extension)
        if not destination_object.exists():
            destination_path = self.unzip_file(base_dir, filename_path, destination_path)

        # Read the CSV files from the destination path directory
        parquet_object = self.get_object(destination_path, filename_parquet)
        if not parquet_object.exists():
            print(f'Reading file {destination_path} ...')
            df = self.read_csv_to_dataframe(destination_path, filename_path, delimiter)
            self.csv_to_parquet(df, destination_path, file_name_without_extension)
        else:
            print(f'Reading Parquet {filename_parquet} ...')
            self.client.fget_object(destination_path, filename_parquet, f'/tmp/{file_name_without_extension}.parquet')
            df = self.spark.read.parquet(f'/tmp/{file_name_without_extension}.parquet')

        if temp_view_name is None:
            temp_view_name = file_name_without_extension

        df.createOrReplaceTempView(temp_view_name)

        return df
