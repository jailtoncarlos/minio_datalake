# MinIO DataLake

This project sets up a MinIO DataLake with JupyterLab and PySpark using Docker. It provides classes and methods to interact with buckets and objects in MinIO, as well as to load CSV files, convert them to Parquet, and create temporary views in Spark. It includes a test suite for verifying the setup and functionality.

## Installation

1. Clone the repository:

   ```bash
   git clone git@github.com:jailtoncarlos/minio_datalake.git
   cd minio_datalake
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

## Running Tests

### Running Tests Inside the JupyterLab Container

The tests are configured to run inside the JupyterLab container to avoid installing additional packages on your local machine. Use the following script to run the tests:

#### `run_tests.sh`

```bash
#!/bin/bash
docker exec -it jupyterlab-pyspark bash -c "cd /home/jovyan/minio_datalake && export PYTHONPATH=/home/jovyan/minio_datalake && python3 -m unittest discover -s . -p 'test_*.py'"
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
├── __init__.py
├── datalake.py
├── settings.py
├── local_settings_sample.py
├── local_settings.py
├── requirements.txt
├── test_datalake.py
├── utils.py
├── run_tests.sh
└── minio_datalake_example.ipynb
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

#### `docker-compose.yml`

```yaml
version: "3.8" # Docker Engine release: 19.03.0+
services:
  jupyterlab-pyspark:
    build:
      context: .
      dockerfile: Dockerfile
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
