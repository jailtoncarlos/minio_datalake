import os
import zipfile
from pyspark.sql import SparkSession
from client import MinIOClient
from bucket import MinIOBucket
from object import MinIOObject
from utils import split_file_path
import settings

import logging
logger = logging.getLogger(__name__)

class MinIODatalake:
    """
    Main class for interacting with the MinIO DataLake.

    Attributes:
    client: Instance of the MinIO client.
    spark: Spark session.
    """

    def __init__(self, endpoint=settings.MINIO_URL, access_key=settings.ACCESS_KEY, secret_key=settings.SECRET_KEY):
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

    def load_csv(self, filename_path: str, destination_path: str = 'stage', temp_view_name: str = None,
                 delimiter: str = ','):
        """
        Load a CSV file into the MinIO DataLake, converting it to Parquet and creating a temporary view in Spark.

        Parameters:
        filename_path (str): Path to the CSV file.
        destination_path (str): Path to the destination directory in the DataLake.
        temp_view_name (str): Name of the temporary view in Spark.
        delimiter (str): CSV delimiter.

        Returns:
        DataFrame: Spark DataFrame.
        """
        # Split the file path into base directory, file name without extension, and extension
        base_dir, file_name_without_extension, file_extension = split_file_path(filename_path)

        # Parquet file name
        filename_parquet = f'{os.path.join(destination_path, file_name_without_extension)}.parquet'

        # Create the destination bucket if it does not exist
        bucket = self.get_bucket(destination_path)
        bucket.create_bucket()

        # Check if the destination path directory does NOT exist in the datalake
        # If it does not exist, unzip the file
        if not os.path.exists(destination_path):
            os.makedirs(destination_path)
            print(f'Unzip {destination_path} ...')

            # If it does not exist, extract the contents of the zip file
            zip_path = f'{filename_path}.zip'
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Extract all files to the specified directory
                zip_ref.extractall(destination_path)

        # If the destination path directory exists and the parquet file does not exist
        # read the CSV files from the destination path directory
        if os.path.exists(destination_path) and not os.path.exists(filename_parquet):
            # Read the CSV file
            print(f'Read file {destination_path} ...')
            df = self.spark.read.options(delimiter=delimiter, header=True).csv(destination_path)

        if not os.path.exists(filename_parquet):
            print(f'Save parquet {filename_parquet} ...')
            df.write.parquet(filename_parquet)

        # If the Parquet file exists, load the dataframe from it
        if os.path.exists(filename_parquet):
            print(f'Read parquet {filename_parquet} ...')
            df = self.spark.read.parquet(filename_parquet)

        # Create a temporary view in Spark for SQL use
        if temp_view_name is None:
            temp_view_name = file_name_without_extension

        df.createOrReplaceTempView(temp_view_name)
        # df3 = spark.sql("select * from table")

        return df
