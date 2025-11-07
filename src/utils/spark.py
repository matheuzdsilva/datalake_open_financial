from pyspark.sql import SparkSession
import os

def get_spark(app_name: str = "openmacrolake"):
    
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER", "admin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "password123")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        .config("spark.hadoop.hadoop.security.authentication", "simple")
        .config("spark.hadoop.security.authentication", "simple")
        .config("spark.hadoop.security.authorization", "false")

        .config("spark.hadoop.fs.defaultFS", "file:/")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")

        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    return spark
