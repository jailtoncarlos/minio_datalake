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

        # Setting initial attribute values with updated default settings
        self.spark_hadoop_fs_s3_impl = self.SPARK_CONF_S3['spark.hadoop.fs.s3.impl']
        self.spark_hadoop_fs_s3a_aws_credentials_provider = self.SPARK_CONF_S3[
            'spark.hadoop.fs.s3a.aws.credentials.provider']
        self.spark_hadoop_fs_s3a_access_key = self.SPARK_CONF_S3['spark.hadoop.fs.s3a.access.key']
        self.spark_hadoop_fs_s3a_secret_key = self.SPARK_CONF_S3['spark.hadoop.fs.s3a.secret.key']
        self.spark_hadoop_fs_s3a_endpoint = self.SPARK_CONF_S3['spark.hadoop.fs.s3a.endpoint']
        self.spark_hadoop_fs_s3a_path_style_access = self.SPARK_CONF_S3['spark.hadoop.fs.s3a.path.style.access']
        self.spark_hadoop_fs_s3a_connection_ssl_enabled = self.SPARK_CONF_S3[
            'spark.hadoop.fs.s3a.connection.ssl.enabled']
        self.spark_jars_packages = self.SPARK_CONF_S3['spark.jars.packages']

        # Configure Spark with S3 configurations
        for key, value in self.SPARK_CONF_S3.items():
            self.set(key, value)

    @staticmethod
    def get_conf_s3_template():
        """Return the S3 configuration template."""
        return SPARK_CONF_S3_TEMPLATE

    @property
    def spark_app_name(self) -> Optional[str]:
        """Get the value of 'spark.app.name'."""
        return self.get("spark.app.name")

    @spark_app_name.setter
    def spark_app_name(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.app.name'.
        Sets a name for the application, which will be displayed on the Spark web UI.
        This name can help in identifying the application.
        """
        return self.set("spark.app.name", value)

    @property
    def spark_hadoop_fs_s3_impl(self) -> Optional[str]:
        """Get the value of 'spark.hadoop.fs.s3.impl'."""
        return self.get("spark.hadoop.fs.s3.impl")

    @spark_hadoop_fs_s3_impl.setter
    def spark_hadoop_fs_s3_impl(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.hadoop.fs.s3.impl'.
        Implementation class of the S3 filesystem. This is typically set to 'org.apache.hadoop.fs.s3a.S3AFileSystem'
        for accessing S3 using the Hadoop S3A filesystem client.
        """
        return self.set("spark.hadoop.fs.s3.impl", value)

    @property
    def spark_hadoop_fs_s3a_aws_credentials_provider(self) -> Optional[str]:
        """Get the value of 'spark.hadoop.fs.s3a.aws.credentials.provider'."""
        return self.get("spark.hadoop.fs.s3a.aws.credentials.provider")

    @spark_hadoop_fs_s3a_aws_credentials_provider.setter
    def spark_hadoop_fs_s3a_aws_credentials_provider(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.hadoop.fs.s3a.aws.credentials.provider'.
        AWS credentials provider for accessing S3. This specifies the method to obtain AWS credentials,
        typically 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider' for basic access key and secret key.
        """
        return self.set("spark.hadoop.fs.s3a.aws.credentials.provider", value)

    @property
    def spark_hadoop_fs_s3a_access_key(self) -> Optional[str]:
        """Get the value of 'spark.hadoop.fs.s3a.access.key'."""
        return self.get("spark.hadoop.fs.s3a.access.key")

    @spark_hadoop_fs_s3a_access_key.setter
    def spark_hadoop_fs_s3a_access_key(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.hadoop.fs.s3a.access.key'.
        Access key for S3. This is used as the AWS access key ID for authenticating to the S3 service.
        """
        return self.set("spark.hadoop.fs.s3a.access.key", value)

    @property
    def spark_hadoop_fs_s3a_secret_key(self) -> Optional[str]:
        """Get the value of 'spark.hadoop.fs.s3a.secret.key'."""
        return self.get("spark.hadoop.fs.s3a.secret.key")

    @spark_hadoop_fs_s3a_secret_key.setter
    def spark_hadoop_fs_s3a_secret_key(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.hadoop.fs.s3a.secret.key'.
        Secret key for S3. This is used as the AWS secret access key for authenticating to the S3 service.
        """
        return self.set("spark.hadoop.fs.s3a.secret.key", value)

    @property
    def spark_hadoop_fs_s3a_proxy_host(self) -> Optional[str]:
        """Get the value of 'spark.hadoop.fs.s3a.proxy.host'."""
        return self.get("spark.hadoop.fs.s3a.proxy.host")

    @spark_hadoop_fs_s3a_proxy_host.setter
    def spark_hadoop_fs_s3a_proxy_host(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.hadoop.fs.s3a.proxy.host'.
        Proxy host for S3. This specifies the proxy server to use for S3 connections if needed.
        """
        return self.set("spark.hadoop.fs.s3a.proxy.host", value)

    @property
    def spark_hadoop_fs_s3a_endpoint(self) -> Optional[str]:
        """Get the value of 'spark.hadoop.fs.s3a.endpoint'."""
        return self.get("spark.hadoop.fs.s3a.endpoint")

    @spark_hadoop_fs_s3a_endpoint.setter
    def spark_hadoop_fs_s3a_endpoint(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.hadoop.fs.s3a.endpoint'.
        S3 endpoint URL. This specifies the endpoint URL for connecting to the S3 service.
        """
        return self.set("spark.hadoop.fs.s3a.endpoint", value)

    @property
    def spark_hadoop_fs_s3a_proxy_host(self) -> Optional[str]:
        """Get the value of 'spark.hadoop.fs.s3a.proxy.host'."""
        return self.get("spark.hadoop.fs.s3a.proxy.host")

    @spark_hadoop_fs_s3a_proxy_host.setter
    def spark_hadoop_fs_s3a_proxy_host(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.hadoop.fs.s3a.proxy.host'.
        Proxy port for S3. This specifies the proxy server host to use for S3 connections if needed.
        """
        return self.set("spark.hadoop.fs.s3a.proxy.host", value)

    @property
    def spark_hadoop_fs_s3a_proxy_port(self) -> Optional[str]:
        """Get the value of 'spark.hadoop.fs.s3a.proxy.port'."""
        return self.get("spark.hadoop.fs.s3a.proxy.port")

    @spark_hadoop_fs_s3a_proxy_port.setter
    def spark_hadoop_fs_s3a_proxy_port(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.hadoop.fs.s3a.proxy.port'.
        Proxy port for S3. This specifies the proxy server port to use for S3 connections if needed.
        """
        return self.set("spark.hadoop.fs.s3a.proxy.port", value)

    @property
    def spark_hadoop_fs_s3a_path_style_access(self) -> Optional[str]:
        """Get the value of 'spark.hadoop.fs.s3a.path.style.access'."""
        return self.get("spark.hadoop.fs.s3a.path.style.access")

    @spark_hadoop_fs_s3a_path_style_access.setter
    def spark_hadoop_fs_s3a_path_style_access(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.hadoop.fs.s3a.path.style.access'.
        Whether to use path style access for S3. Path style access means the bucket name is part of the URL path
        rather than the hostname. This can be useful for some S3-compatible services like MinIO.
        """
        return self.set("spark.hadoop.fs.s3a.path.style.access", value)

    @property
    def spark_hadoop_fs_s3a_connection_ssl_enabled(self) -> Optional[str]:
        """Get the value of 'spark.hadoop.fs.s3a.connection.ssl.enabled'."""
        return self.get("spark.hadoop.fs.s3a.connection.ssl.enabled")

    @spark_hadoop_fs_s3a_connection_ssl_enabled.setter
    def spark_hadoop_fs_s3a_connection_ssl_enabled(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.hadoop.fs.s3a.connection.ssl.enabled'.
        Whether to use SSL for S3 connections. This determines if connections to S3 should be encrypted using SSL.
        """
        return self.set("spark.hadoop.fs.s3a.connection.ssl.enabled", value)

    @property
    def spark_jars_packages(self) -> Optional[str]:
        """Get the value of 'spark.jars.packages'."""
        return self.get("spark.jars.packages")

    @spark_jars_packages.setter
    def spark_jars_packages(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.jars.packages'.
        Comma-separated list of Maven coordinates of jars to include on the driver and executor classpaths.
        This can include dependencies like the AWS SDK, PostgreSQL JDBC driver, and Hadoop common and AWS modules.
        """
        return self.set("spark.jars.packages", value)


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

        # Setting initial attribute values with updated default settings
        self.spark_master = self.SPARK_CONF_KUBERNETES['spark.master']
        self.spark_driver_host = self.SPARK_CONF_KUBERNETES['spark.driver.host']
        self.spark_kubernetes_container_image = self.SPARK_CONF_KUBERNETES['spark.kubernetes.container.image']
        self.spark_executor_cores = self.SPARK_CONF_KUBERNETES['spark.executor.cores']
        self.spark_executor_memory = self.SPARK_CONF_KUBERNETES['spark.executor.memory']
        self.spark_executor_instances = self.SPARK_CONF_KUBERNETES['spark.executor.instances']
        self.spark_kubernetes_container_image_pull_policy = self.SPARK_CONF_KUBERNETES[
            'spark.kubernetes.container.image.pullPolicy']
        self.spark_kubernetes_container_image_pull_secrets = self.SPARK_CONF_KUBERNETES[
            'spark.kubernetes.container.image.pullSecrets']
        self.spark_kubernetes_authenticate_driver_service_account_name = self.SPARK_CONF_KUBERNETES[
            'spark.kubernetes.authenticate.driver.serviceAccountName']
        self.spark_kubernetes_authenticate_service_account_name = self.SPARK_CONF_KUBERNETES[
            'spark.kubernetes.authenticate.serviceAccountName']

        # Configure Spark with Kubernetes configurations
        for key, value in self.SPARK_CONF_KUBERNETES.items():
            self.set(key, value)

    @staticmethod
    def get_conf_kubernetes_template():
        """Return the S3 configuration template."""
        return SPARK_CONF_KUBERNETES_TEMPLATE

    @property
    def spark_master(self) -> Optional[str]:
        """Get the value of 'spark.master'."""
        return self.get("spark.master")

    @spark_master.setter
    def spark_master(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.master'.
        Specifies the master URL for a distributed cluster. This can be a Mesos master,
        a YARN ResourceManager, or a Kubernetes API endpoint.
        """
        return self.set("spark.master", value)

    @property
    def spark_driver_host(self) -> Optional[str]:
        """Get the value of 'spark.driver.host'."""
        return self.get("spark.driver.host")

    @spark_driver_host.setter
    def spark_driver_host(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.driver.host'.
        Hostname or IP address for the driver. This is used for communicating with the
        executors and the Spark cluster.
        """
        return self.set("spark.driver.host", value)

    @property
    def spark_executor_cores(self) -> Optional[str]:
        """Get the value of 'spark.executor.cores'."""
        return self.get("spark.executor.cores")

    @spark_executor_cores.setter
    def spark_executor_cores(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.executor.cores'.
        Number of cores to use on each executor. This determines the amount of CPU resources
        allocated for each executor.
        """
        return self.set("spark.executor.cores", value)

    @property
    def spark_executor_memory(self) -> Optional[str]:
        """Get the value of 'spark.executor.memory'."""
        return self.get("spark.executor.memory")

    @spark_executor_memory.setter
    def spark_executor_memory(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.executor.memory'.
        Amount of memory to use per executor process. This determines the amount of RAM
        allocated for each executor.
        """
        return self.set("spark.executor.memory", value)

    @property
    def spark_executor_instances(self) -> Optional[str]:
        """Get the value of 'spark.executor.instances'."""
        return self.get("spark.executor.instances")

    @spark_executor_instances.setter
    def spark_executor_instances(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.executor.instances'.
        Number of executors to launch for this session. This determines the number of
        parallel tasks that can be run.
        """
        return self.set("spark.executor.instances", value)

    @property
    def spark_kubernetes_namespace(self) -> Optional[str]:
        """Get the value of 'spark.kubernetes.namespace'."""
        return self.get("spark.kubernetes.namespace")

    @spark_kubernetes_namespace.setter
    def spark_kubernetes_namespace(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.kubernetes.namespace'.
        The namespace for Kubernetes resources. This determines the Kubernetes namespace
        where Spark driver and executor pods will be created.
        """
        return self.set("spark.kubernetes.namespace", value)

    @property
    def spark_kubernetes_container_image(self) -> Optional[str]:
        """Get the value of 'spark.kubernetes.container.image'."""
        return self.get("spark.kubernetes.container.image")

    @spark_kubernetes_container_image.setter
    def spark_kubernetes_container_image(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.kubernetes.container.image'.
        Docker image to use for the Spark driver and executors. This image should have all
        necessary dependencies for running Spark applications.
        """
        return self.set("spark.kubernetes.container.image", value)

    @property
    def spark_kubernetes_container_image_pull_policy(self) -> Optional[str]:
        """Get the value of 'spark.kubernetes.container.image.pullPolicy'."""
        return self.get("spark.kubernetes.container.image.pullPolicy")

    @spark_kubernetes_container_image_pull_policy.setter
    def spark_kubernetes_container_image_pull_policy(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.kubernetes.container.image.pullPolicy'.
        The image pull policy for Kubernetes. This determines whether the image should always
        be pulled, never be pulled, or pulled if not present.
        """
        return self.set("spark.kubernetes.container.image.pullPolicy", value)

    @property
    def spark_kubernetes_container_image_pull_secrets(self) -> Optional[str]:
        """Get the value of 'spark.kubernetes.container.image.pullSecrets'."""
        return self.get("spark.kubernetes.container.image.pullSecrets")

    @spark_kubernetes_container_image_pull_secrets.setter
    def spark_kubernetes_container_image_pull_secrets(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.kubernetes.container.image.pullSecrets'.
        Specify the secret for pulling images from private registries. This is used to
        authenticate against the Docker registry.
        """
        return self.set("spark.kubernetes.container.image.pullSecrets", value)

    @property
    def spark_kubernetes_authenticate_driver_service_account_name(self) -> Optional[str]:
        """Get the value of 'spark.kubernetes.authenticate.driver.serviceAccountName'."""
        return self.get("spark.kubernetes.authenticate.driver.serviceAccountName")

    @spark_kubernetes_authenticate_driver_service_account_name.setter
    def spark_kubernetes_authenticate_driver_service_account_name(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.kubernetes.authenticate.driver.serviceAccountName'.
        Service account for the Spark driver pod. This account is used to access the Kubernetes
        API and manage resources.
        """
        return self.set("spark.kubernetes.authenticate.driver.serviceAccountName", value)

    @property
    def spark_kubernetes_authenticate_service_account_name(self) -> Optional[str]:
        """Get the value of 'spark.kubernetes.authenticate.serviceAccountName'."""
        return self.get("spark.kubernetes.authenticate.serviceAccountName")

    @spark_kubernetes_authenticate_service_account_name.setter
    def spark_kubernetes_authenticate_service_account_name(self, value: str) -> SparkConf:
        """
        Set the value of 'spark.kubernetes.authenticate.serviceAccountName'.
        Service account for the Spark executor pods. This account is used to access the Kubernetes
        API and manage resources.
        """
        return self.set("spark.kubernetes.authenticate.serviceAccountName", value)
