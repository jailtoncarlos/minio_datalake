# Spark configurations to use MinIO
SPARK_CONF_S3_TEMPLATE = {
    'spark.hadoop.fs.s3.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
    'spark.hadoop.fs.s3a.access.key': 'minioadmin',
    'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
    'spark.hadoop.fs.s3a.endpoint': 'minio:9000',  # f'http://{MINIO_ENDPOINT}',
    # 'spark.hadoop.fs.s3a.proxy.host': 'minio:9000',
    # 'spark.hadoop.fs.s3a.proxy.port': '80',
    'spark.hadoop.fs.s3a.path.style.access': True,
    'spark.hadoop.fs.s3a.connection.ssl.enabled': False,
    'spark.jars.packages': 'com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.1.1,org.apache.hadoop:hadoop-common:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4'
}

SPARK_CONF_KUBERNETES_TEMPLATE = {
    'spark.master': 'k8s://https://host:port',
    'spark.driver.host': 'MY_POD_IP',
    'spark.executor.cores': '6',  # 6
    'spark.executor.memory': '8g',  # 10g
    'spark.executor.instances': '1',  # 3
    'spark.kubernetes.namespace': 'spark',
    'spark.kubernetes.container.image': 'host:port/pyspark-aws-psql:2.0',
    'spark.kubernetes.container.image.pullPolicy': 'Always',
    'spark.kubernetes.container.image.pullSecrets': 'regcred',
    'spark.kubernetes.authenticate.driver.serviceAccountName': 'spark',
    'spark.kubernetes.authenticate.serviceAccountName': 'spark',
}
