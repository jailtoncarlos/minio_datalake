import os
import logging

# Configurar o logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='directory_listing.log')
logger = logging.getLogger()

def generate_spark_init_script():
    script_content = f"""
    val conf = sc.getConf
    conf.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("spark.hadoop.fs.s3a.access.key", "{MINIO_ACCESS_KEY}")
    conf.set("spark.hadoop.fs.s3a.secret.key", "{MINIO_SECRET_KEY}")
    conf.set("spark.hadoop.fs.s3a.endpoint", "{MINIO_ENDPOINT}")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    conf.set("spark.jars.packages", "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.1.1,org.apache.hadoop:hadoop-common:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4")
    """

    script_dir = '/home/jovyan/minio_datalake/scripts'
    os.makedirs(script_dir, exist_ok=True)

    script_path = os.path.join(script_dir, 'init_spark_dependencies.scala')
    with open(script_path, 'w') as f:
        f.write(script_content)

# Main settings for MinIO DataLake
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000') # Use the service name defined in docker-compose.yml
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_USE_SSL = False

RAW_BUCKET = 'raw'
STAGE_BUCKET = 'stage'

# Spark configurations to use MinIO
SPARK_CONF = {
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT, # f"http://{MINIO_ENDPOINT}",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.jars.packages": "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.1.1,org.apache.hadoop:hadoop-common:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4"
}

generate_spark_init_script()

# Load local settings if they exist
try:
    from local_settings import *
except ImportError:
    pass
