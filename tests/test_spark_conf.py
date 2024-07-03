import logging
import os
import unittest
import importlib
from minio_spark.conf import SparkConfS3Kubernet
from minio_spark import settings

logger = logging.getLogger()
class TestSparkConf(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["SPARK_APP_NAME"] = "SparkAppName"

        os.environ["S3_ENDPOINT"] = "minio-svc.minio.svc.cluster.local"
        os.environ["S3_ACCESS_KEY"] = "user"
        os.environ["S3_SECRET_KEY"] = "password"
        os.environ["S3_USE_SSL"] = "false"

        os.environ["SPARK_MASTER"] = "k8s://https://127.0.0.1:6443"
        os.environ["SPARK_DRIVER_HOST"] = os.environ.get("MY_POD_IP", '127.0.0.1')
        os.environ["SPARK_KUBERNETES_CONTAINER_IMAGE"] = "127.0.0.1:30800/pyspark-aws-psql:2.0"

        # Reload the settings module to ensure all variables are read
        importlib.reload(settings)


    @classmethod
    def tearDownClass(cls):
        os.environ["SPARK_APP_NAME"] = "MinIOSpark"

        os.environ["S3_ENDPOINT"] = "minio:9000"
        os.environ["S3_ACCESS_KEY"] = "minioadmin"
        os.environ["S3_SECRET_KEY"] = "minioadmin"
        os.environ["S3_USE_SSL"] = "false"

        os.environ["SPARK_MASTER"] = "k8s://https://host:port"
        os.environ["SPARK_DRIVER_HOST"] = os.environ.get("MY_POD_IP", '127.0.0.1')
        os.environ["SPARK_KUBERNETES_CONTAINER_IMAGE"] = "host:port/pyspark-aws-psql:2.0"

        # Reload the settings module to ensure all variables are read
        importlib.reload(settings)


    def test_spark_conf(self):
        conf = SparkConfS3Kubernet()

        # Verifica se as configurações do Spark foram aplicadas corretamente
        self.assertEqual(conf.get("spark.app.name"), "SparkAppName")
        self.assertEqual(conf.get("spark.hadoop.fs.s3a.endpoint"), "minio-svc.minio.svc.cluster.local")
        self.assertEqual(conf.get("spark.hadoop.fs.s3a.access.key"), "user")
        self.assertEqual(conf.get("spark.hadoop.fs.s3a.secret.key"), "password")
        self.assertEqual(conf.get("spark.hadoop.fs.s3a.connection.ssl.enabled"), "false")

        self.assertEqual(conf.get("spark.master"), "k8s://https://127.0.0.1:6443")
        self.assertEqual(conf.get("spark.driver.host"), os.environ.get("MY_POD_IP", '127.0.0.1'))
        self.assertEqual(conf.get("spark.kubernetes.container.image"), "127.0.0.1:30800/pyspark-aws-psql:2.0")

if __name__ == "__main__":
    unittest.main()
