import logging
import os
import zipfile
from io import BytesIO
from typing import List, Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from minio_spark.bucket import MinioBucket
from minio_spark.client import MinioClient
from minio_spark.conf import ConfSparkS3
from minio_spark.object import MinioObject

logger = logging.getLogger(__name__)

class MinioSpark:
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

        self._client = MinioClient(endpoint = conf.get('spark.hadoop.fs.s3a.endpoint'),
                                   access_key = conf.get('spark.hadoop.fs.s3a.access.key'),
                                   secret_key = conf.get('spark.hadoop.fs.s3a.secret.key'),
                                   secure= conf.get('spark.hadoop.fs.s3a.connection.ssl.enabled') == 'true'
                                   )

        if not isinstance(conf, ConfSparkS3):
            raise TypeError("self._conf_spark must be an instance of SparkConf")

        self._conf = conf
        self._spark = None

    @property
    def conf(self):
        return self._conf

    @property
    def client(self) -> MinioClient:
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

    def get_bucket(self, bucket_name: str) -> MinioBucket:
        '''
        Get a MinIO bucket instance.

        Parameters:
        bucket_name (str): Name of the bucket.

        Returns:
        MinioBucket: Bucket instance.
        '''
        return MinioBucket(self.client, bucket_name)

    def get_object(self, bucket_name: str, object_name: str) -> MinioObject:
        '''
        Get a MinIO object instance.

        Parameters:
        bucket_name (str): Name of the bucket.
        object_name (str): Name of the object.

        Returns:
        MinioObject: Object instance.
        '''
        return MinioObject(self.client, bucket_name, object_name)

    def extract_and_upload_zip(self, minio_object: MinioObject, destination_object: Optional[MinioObject] = None,
                               extract_to_bucket: bool = False) -> List[MinioObject]:
        """
        Extracts a ZIP file from MinIO and uploads the content back to MinIO.

        Parameters:
        - minio_object (MinioObject): MinioObject representing the zip file.
        - destination_object (Optional[MinioObject]): MinioObject representing the destination for the extracted files. If None, extracts to a folder named after the ZIP file.
        - extract_to_bucket (bool): If True, extract to the root of the bucket. If False, extract to a subdirectory with the name of the zip file.

        Returns:
        list: List of MinIOObjects for the extracted files.
        """
        response = self.client.get_object(minio_object.bucket_name, minio_object.name)
        zip_buffer = BytesIO(response.read())
        zip_buffer.seek(0)

        if extract_to_bucket:
            destination_prefix = minio_object.bucket_name
        else:
            destination_prefix = destination_object.object_name if destination_object else f'{os.path.splitext(minio_object.name)[0]}'

        # Remove any trailing slash from the destination prefix
        if not destination_prefix.endswith('/'):
            destination_prefix = f'{destination_prefix}/'

        extracted_objects = []
        with zipfile.ZipFile(zip_buffer, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if not file_name.endswith('/'):  # Skip directories
                    with zip_ref.open(file_name) as source_file:
                        file_path = f'{destination_prefix}{file_name}'  # Use single slash for concatenation
                        self.client.put_object(minio_object.bucket_name, file_path, data=source_file, length=-1,
                                               part_size=10 * 1024 * 1024)
                        extracted_objects.append(MinioObject(self.client, minio_object.bucket_name, file_path))

        response.close()
        return extracted_objects

    def extract_and_upload_zip_by_prefix(self, bucket_name: str, prefix: str, destination_prefix: str,
                                         extract_to_bucket: bool = False):
        """
        Extracts all ZIP files in a given bucket with a specific prefix and uploads the content back to MinIO.

        Parameters:
        - bucket_name (str): Name of the bucket containing the ZIP files.
        - prefix (str): Prefix of the ZIP files to extract.
        - destination_prefix (str): Prefix under which to store the extracted files in MinIO.
        - extract_to_bucket (bool): If True, extract to the root of the bucket. If False, extract to subdirectories named after each ZIP file.
        """
        objects = self.client.list_objects(bucket_name, prefix=prefix)
        for obj in objects:
            if obj.object_name.endswith('.zip'):
                minio_object = MinioObject(self.client, bucket_name, obj.object_name)
                self.extract_and_upload_zip(minio_object, MinioObject(self.client, bucket_name, destination_prefix),
                                            extract_to_bucket=extract_to_bucket)


    def read(self, bucket_name: str, prefix: str, delimiter=',', format_source: str = 'csv',
             option_args: Dict[str, Any] = None) -> DataFrame:
        '''
        Read a CSV file or files from a folder in MinIO and return a Spark DataFrame.

        Parameters:
        bucket_name (str): Name of the bucket in MinIO.
        prefix (str): Prefix of the CSV file or folder.
        delimiter (str): Delimiter used in the CSV file.
        format_source (str): The format to use in the Spark reader.
        option_args (Dict[str, Any]): Additional options for the Spark reader.

        Returns:
        DataFrame: Spark DataFrame.
        '''
        path = f's3a://{bucket_name}/{prefix}/'  # Adjust the path to read all CSV files in the folder

        # Set the format to 'csv' or any other specified format
        reader = self.spark.read.format(format_source)

        # Default options if none are provided
        if option_args is None:
            option_args = {
                'header': 'true',
                'inferSchema': 'true'
            }

        # Apply additional options for the Spark reader
        for key, value in option_args.items():
            reader = reader.option(key, value)

        # Ensure the delimiter option is set
        reader = reader.option('delimiter', delimiter)
        df = reader.load(path)
        return df

    def read_from_zip(self, bucket_name: str, prefix: str, delimiter=',', format_source: str = 'csv',
                      option_args: Dict[str, Any] = None) -> DataFrame:
        '''
        Extract ZIP files from a bucket with a specific prefix and read all CSV files into a Spark DataFrame.

        Parameters:
        bucket_name (str): Name of the bucket.
        prefix (str): Prefix of the ZIP files.
        delimiter (str): Delimiter used in the CSV files.
        format_source (str): Format to use for reading the CSV files.
        option_args (Dict[str, Any]): Additional options for reading the CSV files.

        Returns:
        DataFrame: Spark DataFrame containing data from all CSV files in the ZIPs.
        '''
        # Define a destination prefix for the extracted files
        destination_prefix = f"{prefix}_extracted"

        # Extract all ZIP files with the given prefix
        self.extract_and_upload_zip_by_prefix(bucket_name, prefix, destination_prefix)

        # Define the object representing the folder where CSVs are extracted
        extracted_folder_object = MinioObject(self.client, bucket_name, destination_prefix)

        # Read the CSV files directly from the extracted folder
        df = self.read(extracted_folder_object.bucket_name, extracted_folder_object.name,
                       delimiter=delimiter, format_source=format_source, option_args=option_args)

        return df


    def to_parquet(self, df: DataFrame, minio_object: MinioObject):
        '''
        Convert a Spark DataFrame to Parquet and save it to MinIO.

        Parameters:
        df (DataFrame): Spark DataFrame.
        minio_object (MinioObject): MinioObject representing the target path in MinIO.

        Returns:
        str: Path to the Parquet file in MinIO.
        '''
        parquet_path = f's3a://{minio_object.bucket_name}/{minio_object.name}'
        df.write.mode('overwrite').parquet(parquet_path)

    def ingest_file_to_datalake(self, bucket_name: str, prefix: str, destination_bucket_name: str = 'stage',
                                temp_view_name: str = None, delimiter=',',
                                option_args: Optional[Dict[str, Any]] = None) -> DataFrame:
        '''
        Ingest a file (CSV or ZIP) from a specified bucket and prefix to the MinIO DataLake, converting it to Parquet and creating a temporary view in Spark.

        Parameters:
        bucket_name (str): Name of the bucket in MinIO.
        prefix (str): Prefix of the object in the bucket.
        destination_bucket_name (str): Name of the destination bucket in MinIO.
        temp_view_name (str): Name of the temporary view in Spark.
        delimiter (str): CSV delimiter.
        option_args (Dict[str, Any]): Additional options for reading the CSV file.

        Returns:
        DataFrame: Spark DataFrame.
        '''
        # Define the Parquet object name based on the original object name, changing the extension to .parquet
        parquet_object_name = f'{os.path.splitext(prefix)[0]}.parquet'

        # Retrieve the destination bucket
        destination_bucket = MinioBucket(self.client, destination_bucket_name)
        if not destination_bucket.exists():
            destination_bucket.make()

        # Check if Parquet file already exists
        parquet_minio_object = MinioObject(self.client, destination_bucket_name, parquet_object_name)
        if parquet_minio_object.exists():
            # If the Parquet file exists, read from it
            df = self.read(parquet_minio_object.bucket_name, parquet_minio_object.name, format_source='parquet')
        else:
            # If the Parquet file does not exist, process the CSV or ZIP file
            minio_object = self.get_object(bucket_name, prefix)
            if minio_object.name.endswith('.zip'):
                # If it's a ZIP file, extract and read the CSV files
                self.extract_and_upload_zip(minio_object, extract_to_bucket=True)
                extracted_folder_object = MinioObject(self.client, bucket_name, os.path.splitext(minio_object.name)[0])
                # Read the extracted CSV files directly from the folder
                df = self.read(extracted_folder_object.bucket_name, extracted_folder_object.name,
                               delimiter=delimiter, format_source='csv', option_args=option_args)
            else:
                # If it's a CSV file, read it directly
                df = self.read(bucket_name, prefix, delimiter=delimiter, option_args=option_args)

            # Save the DataFrame to Parquet
            self.to_parquet(df, parquet_minio_object)

            # Read the Parquet file back into a DataFrame
            df = self.read(parquet_minio_object.bucket_name, parquet_minio_object.name, format_source='parquet')

        # Create a temporary view if specified
        if temp_view_name is None:
            temp_view_name = os.path.splitext(prefix)[0]
        df.createOrReplaceTempView(temp_view_name)

        return df
