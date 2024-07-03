from typing import Optional
from pyspark import SparkConf
from minio_spark.conf_template import SPARK_CONF_S3_TEMPLATE, SPARK_CONF_KUBERNETES_TEMPLATE

def _get_settings():
    from minio_spark import settings
    return settings

class ConfSparkS3(SparkConf):
    SPARK_CONF_S3 = SPARK_CONF_S3_TEMPLATE.copy()

    def __init__(self,
                 spark_app_name: str = None,
                 s3_endpoint: str = None,
                 s3_access_key: str = None,
                 s3_secret_key: str = None,
                 s3_use_ssl: str = None):

        super().__init__()

        settings = _get_settings()
        self.setAppName(spark_app_name or settings.SPARK_APP_NAME)

        # Initialize S3 settings
        self._set_initial_values_s3(s3_endpoint or settings.S3_ENDPOINT,
                                    s3_access_key or settings.S3_ACCESS_KEY,
                                    s3_secret_key or settings.S3_SECRET_KEY,
                                    s3_use_ssl or settings.S3_USE_SSL)

    def _set_initial_values_s3(self, s3_endpoint: str, s3_access_key: str, s3_secret_key: str, s3_use_ssl: str):
        # Updating default settings for S3 MinIO
        self.SPARK_CONF_S3.update({
            'spark.hadoop.fs.s3a.access.key': s3_access_key,
            'spark.hadoop.fs.s3a.secret.key': s3_secret_key,
            'spark.hadoop.fs.s3a.endpoint': s3_endpoint,
            'spark.hadoop.fs.s3a.connection.ssl.enabled': s3_use_ssl
        })

        # Configure Spark with S3 configurations
        for key, value in self.SPARK_CONF_S3.items():
            self.set(key, value)

    @staticmethod
    def get_conf_s3_template():
        """Return the S3 configuration template."""
        return SPARK_CONF_S3_TEMPLATE


class SparkConfS3Kubernet(ConfSparkS3):
    SPARK_CONF_KUBERNETES = SPARK_CONF_KUBERNETES_TEMPLATE.copy()

    def __init__(self,
                 spark_app_name: str = None,
                 s3_endpoint: str = None,
                 s3_access_key: str = None,
                 s3_secret_key: str = None,
                 s3_use_ssl: str = None,
                 spark_master: str = None,
                 spark_driver_host: str = None,
                 spark_kubernetes_container_image: str = None):

        settings = _get_settings()
        super().__init__(
                spark_app_name or settings.SPARK_APP_NAME,
                s3_endpoint or settings.S3_ENDPOINT,
                s3_access_key or settings.S3_ACCESS_KEY,
                s3_secret_key or settings.S3_SECRET_KEY,
                s3_use_ssl or settings.S3_USE_SSL
                )
        # Initialize Kubernetes settings
        self._set_initial_value_kubernetes(spark_master or settings.SPARK_MASTER,
                                           spark_driver_host or settings.SPARK_DRIVER_HOST,
                                           spark_kubernetes_container_image or settings.SPARK_KUBERNETES_CONTAINER_IMAGE
                                           )

    def _set_initial_value_kubernetes(self, spark_master: str, spark_driver_host: str,
                                      spark_kubernetes_container_image: str):
        # Updating default settings for Kubernetes
        self.SPARK_CONF_KUBERNETES.update({
            'spark.master': spark_master,
            'spark.driver.host': spark_driver_host,
            'spark.kubernetes.container.image': spark_kubernetes_container_image
        })

        # Configure Spark with Kubernetes configurations
        for key, value in self.SPARK_CONF_KUBERNETES.items():
            self.set(key, value)

    @staticmethod
    def get_conf_kubernetes_template():
        """Return the S3 configuration template."""
        return SPARK_CONF_KUBERNETES_TEMPLATE
