import logging
import os
import zipfile
from io import BytesIO
from typing import List, Optional

from minio import Minio
from pyspark.sql import SparkSession, DataFrame

from minio_spark.bucket import MinIOBucket
from minio_spark.conf import ConfSparkS3
from minio_spark.object import MinIOObject

logger = logging.getLogger(__name__)


class MinIOSpark:
    '''
    Main class for interacting with the MinIO DataLake.

    Attributes:
    client: Instance of the MinIO client.
    spark: Spark session.
    '''

    def __init__(self, conf: Optional[ConfSparkS3] = None) -> None:
        # Ensure self._conf_spark is an instance of SparkConf
        if conf is None:
            conf = ConfSparkS3()

        self._client = Minio(endpoint=conf.spark_hadoop_fs_s3a_endpoint,
                             access_key=conf.spark_hadoop_fs_s3a_access_key,
                             secret_key=conf.spark_hadoop_fs_s3a_secret_key,
                             secure=conf.spark_hadoop_fs_s3a_connection_ssl_enabled=='true')

        if not isinstance(conf, ConfSparkS3):
            raise TypeError("self._conf_spark must be an instance of SparkConf")

        self._conf = conf
        logger.debug(f'endpoint: {conf.spark_hadoop_fs_s3a_endpoint}')
        logger.debug(f'access_key: {conf.spark_hadoop_fs_s3a_access_key}')
        logger.debug(f'secret_key: {conf.spark_hadoop_fs_s3a_secret_key}')
        logger.debug(f'secure: {conf.spark_hadoop_fs_s3a_connection_ssl_enabled}')

        self._spark = None

    @property
    def client(self) -> Minio:
        '''
        Get the MinIO client instance.

        Returns:
        MinIOClient: MinIO client.
        '''
        return self._client

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            # Debugging print statements
            logger.debug(f"Type of self._conf_minio_spark: {type(self._conf)}")

            self._spark = SparkSession.builder.config(conf=self._conf).getOrCreate()
        return self._spark

    def get_bucket(self, bucket_name: str) -> MinIOBucket:
        '''
        Get a MinIO bucket instance.

        Parameters:
        bucket_name (str): Name of the bucket.

        Returns:
        MinIOBucket: Bucket instance.
        '''
        return MinIOBucket(self.client, bucket_name)

    def get_object(self, bucket_name: str, object_name: str) -> MinIOObject:
        '''
        Get a MinIO object instance.

        Parameters:
        bucket_name (str): Name of the bucket.
        object_name (str): Name of the object.

        Returns:
        MinIOObject: Object instance.
        '''
        return MinIOObject(self.client, bucket_name, object_name)

    def read_csv_from_zip(self, minio_object: MinIOObject, delimiter=',') -> DataFrame:
        '''
        Read a CSV file from a ZIP archive in MinIO and return a Spark DataFrame.

        Parameters:
        minio_object (MinIOObject): MinIOObject representing the ZIP file.
        delimiter (str): Delimiter used in the CSV file.

        Returns:
        DataFrame: Spark DataFrame.
        '''
        zip_data = self.client.get_object(minio_object.bucket_name, minio_object.object_name).read()
        zip_buffer = BytesIO(zip_data)
        zip_buffer.seek(0)

        with zipfile.ZipFile(zip_buffer, 'r') as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise ValueError('No CSV files found in the ZIP archive.')

            csv_data = zip_ref.read(csv_files[0])
            csv_buffer = BytesIO(csv_data)
            csv_buffer.seek(0)

            df = self.spark.read.format('csv') \
                .option('header', 'true') \
                .option('inferSchema', 'true') \
                .option('delimiter', delimiter) \
                .load(f's3a://{minio_object.bucket_name}/{csv_files[0]}')

        return df

    def read_csv_to_dataframe(self, minio_object: MinIOObject, delimiter=',') -> DataFrame:
        '''
        Read a CSV file from MinIO and return a Spark DataFrame.

        Parameters:
        minio_object (MinIOObject): MinIOObject representing the CSV file.
        delimiter (str): Delimiter used in the CSV file.

        Returns:
        DataFrame: Spark DataFrame.
        '''
        csv_path = f's3a://{minio_object.bucket_name}/{minio_object.object_name}'
        df = self.spark.read.format('csv') \
            .option('header', 'true') \
            .option('inferSchema', 'true') \
            .option('delimiter', delimiter) \
            .load(csv_path)
        return df

    def read_parquet_to_dataframe(self, minio_object: MinIOObject) -> DataFrame:
        '''
        Read a Parquet file from MinIO and return a Spark DataFrame.

        Parameters:
        minio_object (MinIOObject): MinIOObject representing the Parquet file.

        Returns:
        DataFrame: Spark DataFrame.
        '''
        parquet_path = f's3a://{minio_object.bucket_name}/{minio_object.object_name}'
        df = self.spark.read.parquet(parquet_path)
        return df

    def dataframe_to_parquet(self, df: DataFrame, path: str) -> str:
        '''
        Convert a Spark DataFrame to Parquet and save it to MinIO.

        Parameters:
        df (DataFrame): Spark DataFrame.
        path (str): Path to save the Parquet file.

        Returns:
        str: Path to the Parquet file in MinIO.
        '''
        parquet_path = f's3a://{path}'
        df.write.mode('overwrite').parquet(parquet_path)
        return path

    def ingest_csv_to_datalake(self, minio_object: MinIOObject, destination_path: str = 'stage',
                               temp_view_name: str = None, delimiter=',') -> DataFrame:
        '''
        Ingest a CSV file to the MinIO DataLake, converting it to Parquet and creating a temporary view in Spark.

        Parameters:
        minio_object (MinIOObject): MinIOObject representing the CSV or ZIP file.
        destination_path (str): Path to the destination directory in MinIO.
        temp_view_name (str): Name of the temporary view in Spark.
        delimiter (str): CSV delimiter.

        Returns:
        DataFrame: Spark DataFrame.
        '''
        parquet_path = f'{destination_path}/{os.path.splitext(minio_object.object_name)[0]}.parquet'

        bucket = self.get_bucket(destination_path)
        if not bucket.exists():
            bucket.make()

        # Check if Parquet file already exists
        parquet_minio_object = MinIOObject(self.client, destination_path,
                                           f'{os.path.splitext(minio_object.object_name)[0]}.parquet')
        if parquet_minio_object.exists():
            # If the parquet file exists, read from it
            df = self.read_parquet_to_dataframe(parquet_minio_object)
        else:
            # If the parquet file does not exist, process the CSV or ZIP file
            if minio_object.object_name.endswith('.zip'):
                # If it's a ZIP file, unzip and read the CSV files
                df = self.read_csv_from_zip(minio_object, delimiter)
            else:
                # If it's a CSV file, read it directly
                df = self.read_csv_to_dataframe(minio_object, delimiter)

            # Save the DataFrame to Parquet
            self.dataframe_to_parquet(df, parquet_path)
            # Read the Parquet file back into a DataFrame
            df = self.read_parquet_to_dataframe(parquet_minio_object)

        if temp_view_name is None:
            temp_view_name = os.path.splitext(minio_object.object_name)[0]
        df.createOrReplaceTempView(temp_view_name)

        return df

    def unzip(self, minio_object: MinIOObject, extract_to_bucket: bool = False) -> List[MinIOObject]:
        '''
        Unzip a file in the MinIO bucket.

        Parameters:
        minio_object (MinIOObject): MinIOObject representing the zip file.
        extract_to_bucket (bool): If True, extract to the root of the bucket. If False, extract to a subdirectory with the name of the zip file.

        Returns:
        list: List of MinIOObjects for the extracted files.
        '''
        zip_buffer = BytesIO(self.client.get_object(minio_object.bucket_name, minio_object.object_name).read())
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
