# MinIOSpark

MinIOSpark is a library that provides an abstraction layer for working with MinIO and Spark to facilitate data lake operations. This library allows users to interact with MinIO buckets and objects, read and write data in various formats, and leverage the power of Spark for big data processing.

## Key Features

- Integration with MinIO and Spark
- Handling CSV and Parquet file formats
- Extracting and processing ZIP files in MinIO
- Creating temporary views in Spark

## Installation

To install the library, use pip:

```bash
pip install git+https://github.com/jailtoncarlos/minio_spark.git
```


## Configuration

Before using MinioSpark, it is necessary to configure the MinIO credentials and endpoint. Configuration can be done in two ways: through environment variables or by defining the `ConfSparkS3` or `SparkConfS3Kubernet` configuration objects.

### Configuration via Environment Variables

```python
import os

os.environ["S3_ENDPOINT"] = "your-minio-endpoint"
os.environ["S3_ACCESS_KEY"] = "your-access-key"
os.environ["S3_SECRET_KEY"] = "your-secret-key"
os.environ["S3_USE_SSL"] = "false"
os.environ["SPARK_APP_NAME"] = "MinIOSparkApp"
os.environ["SPARK_MASTER"] = "local[*]"
os.environ["SPARK_DRIVER_HOST"] = os.environ.get("MY_POD_IP", '127.0.0.1')
os.environ["SPARK_KUBERNETES_CONTAINER_IMAGE"] = "your-spark-image"

from minio_spark import MinioSpark

datalake = MinioSpark()
```

### Configuration via `ConfSparkS3` or `SparkConfS3Kubernet` Object

```python
from minio_spark.conf import ConfSparkS3, SparkConfS3Kubernet
from minio_spark import MinioSpark

# Using ConfSparkS3
conf = ConfSparkS3(
    spark_app_name="MinIOSparkApp",
    s3_endpoint="your-minio-endpoint",
    s3_access_key="your-access-key",
    s3_secret_key="your-secret-key",
    s3_use_ssl="false"
)
datalake = MinioSpark(conf=conf)

# Using SparkConfS3Kubernet
conf_k8s = SparkConfS3Kubernet(
    spark_app_name="MinIOSparkApp",
    s3_endpoint="your-minio-endpoint",
    s3_access_key="your-access-key",
    s3_secret_key="your-secret-key",
    s3_use_ssl="false",
    spark_master="k8s://https://your-k8s-api-server:443",
    spark_driver_host="your-driver-host-ip",
    spark_kubernetes_container_image="your-spark-image"
)
datalake = MinioSpark(conf=conf_k8s)
```
## Example Usage

### Initialization

```python
from minio_spark import MinioSpark

datalake = MinioSpark(conf)
```

### Operations with Buckets

```python
bucket = datalake.get_bucket('my-bucket')
if not bucket.exists():
    bucket.make()
```

### Operations with Objects

```python
minio_object = datalake.get_object('my-bucket', 'my-file.csv')

# Check if the object exists
if minio_object.exists():
    print("The object exists")

# Upload a file
with open('local-file.csv', 'rb') as file_data:
    minio_object.put(file_data, length=os.path.getsize('local-file.csv'))

# Download a file
minio_object.fget('downloaded-file.csv')

# Remove an object
minio_object.remove()
```

### Reading CSV Files

```python
df = datalake.read('my-bucket', 'my-prefix/', delimiter=',')
df.show()
```

### Reading Parquet Files

```python
df = datalake.read('my-bucket', 'my-prefix/', format_source='parquet')
df.show()
```

### Converting DataFrames to Parquet

```python
df = ...  # Your Spark DataFrame
parquet_object = datalake.get_object('my-bucket', 'my-file.parquet')
datalake.to_parquet(df, parquet_object)
```

### Ingesting Files to the DataLake

```python
df = datalake.ingest_file_to_datalake('my-bucket', 'my-prefix', destination_bucket_name='my-destination', delimiter=',')
df.createOrReplaceTempView('my_temp_view')
```

### Extracting and Uploading ZIP Files

```python
zip_object = datalake.get_object('my-bucket', 'my-file.zip')
extracted_objects = datalake.extract_and_upload_zip(zip_object)
for obj in extracted_objects:
    print(obj.name)
```

## Available Methods

### `MinioSpark`

- `get_bucket(bucket_name: str) -> MinioBucket`: Returns an instance of `MinioBucket`.
- `get_object(bucket_name: str, object_name: str) -> MinioObject`: Returns an instance of `MinioObject`.
- `extract_and_upload_zip(minio_object: MinioObject, destination_object: Optional[MinioObject] = None, extract_to_bucket: bool = False) -> List[MinioObject]`: Extracts a ZIP file from MinIO and uploads the content back to MinIO.
- `extract_and_upload_zip_by_prefix(bucket_name: str, prefix: str, destination_prefix: str, extract_to_bucket: bool = False)`: Extracts all ZIP files in a bucket with a specific prefix and uploads the content back to MinIO.
- `read(bucket_name: str, prefix: str, delimiter=',', format_source: str = 'csv', option_args: Dict[str, Any] = None) -> DataFrame`: Reads CSV files from a folder in MinIO and returns a Spark DataFrame.
- `to_parquet(df: DataFrame, minio_object: MinioObject)`: Converts a Spark DataFrame to Parquet and saves it to MinIO.
- `ingest_file_to_datalake(bucket_name: str, prefix: str, destination_bucket_name: str = 'stage', temp_view_name: str = None, delimiter=',', option_args: Optional[Dict[str, Any]] = None) -> DataFrame`: Ingests a file (CSV or ZIP) from a specified bucket and prefix to the MinIO DataLake, converting it to Parquet and creating a temporary view in Spark.

### `MinioBucket`

- `make()`: Creates a bucket if it does not exist.
- `remove()`: Removes a bucket.
- `put_object(object_name: str, data: BinaryIO, length: int) -> ObjectWriteResult`: Uploads an object to the bucket.
- `remove_object(object_name: str)`: Removes an object from the bucket.
- `exists() -> bool`: Checks if the bucket exists.
- `list_objects(prefix: Optional[str] = None, recursive: bool = False) -> Iterator[MinioObject]`: Lists objects in the bucket.
- `get_object(object_name: str) -> MinioObject`: Returns an object from the bucket by name.

### `MinioObject`

- `put(data: BinaryIO, length: int) -> ObjectWriteResult`: Uploads data to an object.
- `remove()`: Removes an object.
- `fget(file_path: str)`: Downloads an object's data to a file.
- `fput(file_path: str) -> ObjectWriteResult`: Uploads data from a file to an object in the bucket.
- `exists() -> bool`: Checks if the object exists in the bucket.

## License

MinioSpark is licensed under the [MIT License](LICENSE).
```
