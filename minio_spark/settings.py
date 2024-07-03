import os

from minio_spark.conf_template import SPARK_CONF_S3_TEMPLATE, SPARK_CONF_KUBERNETES_TEMPLATE

S3_BUCKET_RAW_NAME = os.getenv('S3_BUCKET_RAW_NAME', 'raw')
S3_BUCKET_STAGE_NAME = os.getenv('S3_BUCKET_STAGE_NAME', 'stage')
S3_BUCKET_CONSUME_NAME = os.getenv('S3_BUCKET_CONSUME_NAME', 'consume')
S3_BUCKET_CURATED_NAME = os.getenv('S3_BUCKET_CURATED_NAME', 'curated')

# Main settings for MinIO DataLake
S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'minio:9000')  # Use the service name defined in docker-compose.yml
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY', 'minioadmin')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY', 'minioadmin')
S3_USE_SSL = 'false'

SPARK_CONF_S3_TEMPLATE.update({
    'spark.hadoop.fs.s3a.access.key': S3_ACCESS_KEY,
    'spark.hadoop.fs.s3a.secret.key': S3_SECRET_KEY,
    'spark.hadoop.fs.s3a.endpoint': S3_ENDPOINT,
    'spark.hadoop.fs.s3a.connection.ssl.enabled': S3_USE_SSL,
})

# Main settings for Kubernetes DataLake
SPARK_APP_NAME = os.getenv('SPARK_APP_NAME', 'MinIOSpark')
SPARK_MASTER = os.getenv('SPARK_MASTER', 'k8s://https://host:port')
SPARK_DRIVER_HOST = os.getenv('SPARK_DRIVER_HOST', os.environ.get("MY_POD_IP", "127.0.0.1"))
SPARK_KUBERNETES_CONTAINER_IMAGE = os.getenv('SPARK_KUBERNETES_CONTAINER_IMAGE', 'host:port/pyspark-aws-psql:2.0')
SPARK_EXECUTOR_CORES = os.getenv('SPARK_EXECUTOR_CORES', '6')
SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY', '8g')
SPARK_EXECUTOR_INSTANCES = os.getenv('SPARK_EXECUTOR_INSTANCES', '1')

SPARK_CONF_KUBERNETES_TEMPLATE.update({
    'spark.app.name': SPARK_APP_NAME,
    'spark.master': SPARK_MASTER,
    'spark.driver.host': SPARK_DRIVER_HOST,
    'spark.kubernetes.container.image': SPARK_KUBERNETES_CONTAINER_IMAGE,
    'spark.executor.cores': SPARK_EXECUTOR_CORES,
    'spark.executor.memory': SPARK_EXECUTOR_MEMORY,
    'spark.executor.instances': SPARK_EXECUTOR_INSTANCES,
})


def generate_spark_init_script():
    # Build the script string
    script_lines = ["val conf = sc.getConf"]
    for key, value in SPARK_CONF_S3_TEMPLATE.items():
        # Verificar se o valor é uma string ou outra coisa, para formatar corretamente
        if isinstance(value, str):
            script_lines.append(f'conf.set("{key}", "{value}")')
        else:
            script_lines.append(f'conf.set("{key}", {value})')

    script_content = "\n".join(script_lines)

    script_dir = '/home/jovyan/minio_spark/scripts'
    os.makedirs(script_dir, exist_ok=True)

    script_path = os.path.join(script_dir, 'init_spark_dependencies.scala')
    with open(script_path, 'w') as f:
        f.write(script_content)

# generate_spark_init_script()
