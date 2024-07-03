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

Before using the library, configure the necessary environment variables for MinIO and Spark:

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
```

## Usage

### Initialize MinIOSpark

```python
from minio_spark import MinIOSpark

datalake = MinIOSpark()
```

### Bucket Operations

#### Get a Bucket

```python
bucket = datalake.get_bucket('your-bucket-name')
```

#### Create a Bucket

```python
bucket.make()
```

#### Check if a Bucket Exists

```python
exists = bucket.exists()
print(f"Bucket exists: {exists}")
```

### Object Operations

#### Get an Object

```python
minio_object = datalake.get_object('your-bucket-name', 'your-object-name')
```

### Reading Data

#### Read CSV to DataFrame

```python
df = datalake.read_csv_to_dataframe('your-bucket-name', 'your-object-name.csv')
df.show()
```

#### Read Parquet to DataFrame

```python
df = datalake.read_parquet_to_dataframe('your-bucket-name', 'your-object-name.parquet')
df.show()
```

### Writing Data

#### DataFrame to Parquet

```python
datalake.dataframe_to_parquet(df, 'your-bucket-name', 'your-object-name.parquet')
```

### Extracting and Processing ZIP Files

#### Extract ZIP and Read CSVs

```python
df = datalake.read_csv_from_zip('your-bucket-name', 'your-zip-object-prefix')
df.show()
```

### Ingesting Data

#### Ingest CSV or ZIP to DataLake

```python
df = datalake.ingest_file_to_datalake('your-bucket-name', 'your-object-prefix')
df.show()
```

## Changelog

### Version 0.8.2

- Optimized ZIP extraction to handle large files without memory issues
- Improved handling of CSV files extracted from ZIP archives
- Fixed issues with bucket and object naming conventions
- Updated tests to cover edge cases and large datasets

## License

This project is licensed under the Mozilla Public License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Acknowledgments

- Thanks to the MinIO and Spark communities for their great tools and documentation.

