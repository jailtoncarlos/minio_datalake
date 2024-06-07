# MinIO DataLake

This project is an abstraction for working with DataLake using MinIO and PySpark. It provides classes and methods to interact with buckets and objects in MinIO, as well as to load CSV files, convert them to Parquet, and create temporary views in Spark.

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/minio-datalake.git
   cd minio-datalake
   ```

2. Create and activate a virtual environment:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use: venv\Scripts\activate
   ```

3. Install the dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Edit the `settings.py` file to include your MinIO credentials and settings:

```python
MINIO_ENDPOINT = 'YOUR_MINIO_ENDPOINT'
MINIO_ACCESS_KEY = 'YOUR_ACCESS_KEY'
MINIO_SECRET_KEY = 'YOUR_SECRET_KEY'
BUCKET_RAW = 'raw'
BUCKET_STAGE = 'stage'
```

## Usage

```python
from minio_datalake.datalake import MinIODatalake

# Create an instance of MinIODatalake
datalake = MinIODatalake()

# Load a CSV file into the DataLake
df = datalake.load_csv('/path/to/your/file.csv', destination_path='stage', temp_view_name='my_temp_view')

# Check if the 'raw' and 'stage' buckets exist
raw_bucket_exists = datalake.get_bucket('raw').bucket_exists()
stage_bucket_exists = datalake.get_bucket('stage').bucket_exists()

print(f"'raw' bucket exists: {raw_bucket_exists}")
print(f"'stage' bucket exists: {stage_bucket_exists}")
```

## Tests

To run the tests, use the following command:

```bash
python -m unittest discover
```

