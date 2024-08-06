from minio_spark import settings

# Spark configurations to use MinIO
SPARK_CONF_S3_TEMPLATE = {
    'spark.hadoop.fs.s3.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
    'spark.hadoop.fs.s3a.access.key':  settings.S3_ACCESS_KEY,
    'spark.hadoop.fs.s3a.secret.key': settings.S3_SECRET_KEY,
    'spark.hadoop.fs.s3a.endpoint': settings.S3_ENDPOINT,  # f'http://{MINIO_ENDPOINT}',
    # 'spark.hadoop.fs.s3a.proxy.host': 'minio:9000',
    # 'spark.hadoop.fs.s3a.proxy.port': '80',
    'spark.hadoop.fs.s3a.path.style.access': True,
    'spark.hadoop.fs.s3a.connection.ssl.enabled': settings.S3_USE_SSL,
    'spark.jars.packages': 'com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.1.1,org.apache.hadoop:hadoop-common:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4'
}

SPARK_CONF_KUBERNETES_TEMPLATE = {
    'spark.master': settings.SPARK_MASTER,
    'spark.driver.host': settings.SPARK_DRIVER_HOST,
    'spark.executor.cores': settings.SPARK_EXECUTOR_CORES,
    'spark.executor.memory': settings.SPARK_EXECUTOR_MEMORY,  # 10g
    'spark.executor.instances': settings.SPARK_EXECUTOR_INSTANCES,  # 3
    'spark.kubernetes.namespace': 'spark',
    'spark.kubernetes.container.image':  settings.SPARK_KUBERNETES_CONTAINER_IMAGE,
    'spark.kubernetes.container.image.pullPolicy': 'Always',
    'spark.kubernetes.container.image.pullSecrets': 'regcred',
    'spark.kubernetes.authenticate.driver.serviceAccountName': 'spark',
    'spark.kubernetes.authenticate.serviceAccountName': 'spark',
}
