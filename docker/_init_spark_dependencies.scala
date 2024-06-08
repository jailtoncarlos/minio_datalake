import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("InitSparkDependencies")
  .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
  .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
  .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
  .config("spark.hadoop.fs.s3a.endpoint", "minio:9000")
  .config("spark.hadoop.fs.s3a.path.style.access", "true")
  .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
  .config("spark.jars.packages", "com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.1.1,org.apache.hadoop:hadoop-common:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4")
  .getOrCreate()

spark.stop()
