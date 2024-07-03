# MinIO DataLake

This project sets up a MinIO DataLake with JupyterLab and PySpark using Docker. It provides classes and methods to interact with buckets and objects in MinIO, as well as to load CSV files, convert them to Parquet, and create temporary views in Spark. It includes a test suite for verifying the setup and functionality.

## Installation

You can install the package via pip:

```bash
pip install git+https://github.com/jailtoncarlos/minio_datalake.git
```

Alternatively, you can clone the repository and install it locally:

1. Clone the repository:

   ```bash
   git clone https://github.com/jailtoncarlos/minio_datalake.git
   cd minio_datalake
   ```

2. Install the package:

   ```bash
   pip install .
   ```

## Configuration

You can either create a copy of `local_settings_sample.py` and edit it with your MinIO credentials or define environment variables.

### Option 1: Edit `local_settings.py`

Create a copy of `local_settings_sample.py` and edit the new file:

```bash
cp minio_datalake/local_settings_sample.py minio_datalake/local_settings.py
```

Then, edit `minio_datalake/local_settings.py`:

```python
import os

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')

RAW_BUCKET = 'raw'
STAGE_BUCKET = 'stage'
```

### Option 2: Define Environment Variables

Alternatively, you can define the necessary environment variables:

```bash
export MINIO_ENDPOINT='minio:9000'
export MINIO_ACCESS_KEY='minioadmin'
export MINIO_SECRET_KEY='minioadmin'
```

## Usage

```python
from minio_spark.datalake import MinIOSparkDatalake
import minio_spark.settings as settings

# Create an instance of MinIODatalake
datalake = MinIOSparkDatalake()

# Get bucket instances
raw_bucket = datalake.get_bucket(settings.S3_BUCKET_RAW_NAME)
stage_bucket = datalake.get_bucket(settings.S3_BUCKET_STAGE_NAME)

# Check if the 'raw' bucket exists, if not create it
if not raw_bucket.bucket_exists():
    print(f"Creating bucket '{settings.S3_BUCKET_RAW_NAME}'...")
    raw_bucket.create()

# Check if the 'stage' bucket exists, if not create it
if not stage_bucket.bucket_exists():
    print(f"Creating bucket '{settings.S3_BUCKET_STAGE_NAME}'...")
    stage_bucket.create()

# List directories in the root of the DataLake
buckets = datalake.client.list_buckets()
print("Buckets in the root of the DataLake:")
for bucket in buckets:
    print(bucket.name)
```

## Running Tests

### Running Tests Inside the JupyterLab Container

The tests are configured to run inside the JupyterLab container to avoid installing additional packages on your local machine. Use the following script to run the tests:

#### `run_tests.sh`

```bash
#!/bin/bash
docker exec -it jupyterlab-pyspark bash -c "cd /home/jovyan/minio_datalake && export PYTHONPATH=/home/jovyan/minio_datalake && python3 -m unittest discover -s tests -p 'tests.py'"
```

Make sure to make the script executable:

```bash
chmod +x run_tests.sh
```

Run the tests:

```bash
./run_tests.sh
```

## Setup with Docker

### Environment Variables

Set the following environment variables in your shell or `.env` file:

```bash
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
```

### Build and Start the Services

Build and start the services using Docker Compose:

```bash
docker-compose up --build
```

### Accessing the Services

- **JupyterLab**: Open your browser and go to `http://localhost:8888`. Use the password `jupyter` to log in.
- **MinIO Console**: Open your browser and go to `http://localhost:9001`. Use the username `minioadmin` and the password `minioadmin`.

## Project Structure

```
minio_datalake/
│
├── minio_datalake/
│   ├── __init__.py
│   ├── bucket.py
│   ├── client.py
│   ├── datalake.py
│   ├── object.py
│   ├── settings.py
│   ├── utils.py
│   ├── local_settings.py
│   ├── local_settings_sample.py
│
├── sample/
├── work/
├── tests/
│   ├── __init__.py
│   ├── tests.py
│
├── minio_datalake_example.ipynb
├── requirements.txt
├── setup.py
├── Dockerfile
├── docker-compose.yaml
├── run_tests.sh
├── README.md
```

## Dockerfile

The Dockerfile for JupyterLab with PySpark:

#### `Dockerfile`

```Dockerfile
# Use the official Jupyter base image with PySpark
FROM jupyter/pyspark-notebook:latest

# Set the working directory
WORKDIR /home/jovyan

# Copy the requirements file
COPY requirements.txt /home/jovyan/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project code
COPY . /home/jovyan/minio_datalake/

# Set PYTHONPATH to include the project directory
ENV PYTHONPATH="/home/jovyan/minio_datalake:${PYTHONPATH}"
```

## Docker Compose Configuration

The Docker Compose file to set up JupyterLab, PySpark, and MinIO:

#### `docker-compose.yaml`

```yaml
version: "3.8" # Docker Engine release: 19.03.0+
services:
  jupyterlab-pyspark:
    build:
      context: .
      dockerfile: docker/Dockerfile
    container_name: jupyterlab-pyspark
    restart: always
    command: start-notebook.sh --NotebookApp.password=sha256:bb8c050b9545:c21d963d0765635c8494c787935802d4511e0411fb3d4a555f45e2ab0a776c80
    volumes:
      - ./:/home/jovyan/minio_datalake  # Mount the project directory
    environment:
      - GRANT_SUDO=yes
      - JUPYTER_ENABLE_LAB=yes
    ports:
      - 8888:8888
      - 4040:4040

  minio:
    image: minio/minio
    container_name: minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio-data:/data
    ports:
      - 9000:9000
      - 9001:9001
    restart: always

volumes:
  minio-data:
```

## Example Notebook

An example notebook `minio_datalake_example.ipynb` is provided to demonstrate how to interact with MinIO using the `MinIODatalake` class. You can open and run this notebook in JupyterLab.

## Accessing JupyterLab and MinIO

- **JupyterLab**: 
  - URL: `http://localhost:8888`
  - Password: `jupyter`
  
- **MinIO Console**:
  - URL: `http://localhost:9001`
  - Username: `minioadmin`
  - Password: `minioadmin`

## Using with Google Colab

To use this project with Google Colab, follow these steps:

1. **Open Google Colab**:
   - Go to [Google Colab](https://colab.research.google.com/).
   - Create a new notebook or open an existing one.

2. **Mount Google Drive** (if needed):

   ```python
   from google.colab import drive
   drive.mount('/content/drive')
   ```

3. **Clone the Repository**:

   ```python
   !git clone https://github.com/jailtoncarlos/minio_datalake.git
   %cd minio_datalake
   ```

4. **Install Dependencies**:

   ```python
   !pip install -r requirements.txt
   ```

5. **Set Environment Variables**:

   ```python
   import os
   os.environ['MINIO_ENDPOINT'] = 'minio:9000'
   os.environ['MINIO_ACCESS_KEY'] = 'minioadmin'
   os.environ['MINIO_SECRET_KEY'] = 'minioadmin'
   ```

6. **Edit `local_settings.py`**:

   ```python
   !cp /content/minio_datalake/local_settings_sample.py /content/minio_datalake/local_settings.py

   %%writefile /content/minio_datalake/local_settings.py
   import os

   MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
   ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
   SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')

   RAW_BUCKET = 'raw'
   STAGE_BUCKET = 'stage'
   ```

7. **Run the Project Code**:

   ```python
   from minio_datalake.datalake import MinIODatalake

   # Create an instance of MinIOD