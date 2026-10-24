"""Spark session factory configured for Apache Iceberg, Kafka, and MinIO S3."""

import os

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "spark-streaming-engine") -> SparkSession:
    """Build and return a SparkSession configured with Iceberg REST catalog and MinIO S3."""
    iceberg_uri = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password123")

    builder = (
        SparkSession.builder.appName(app_name)
        # Iceberg SQL extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Configure 'lakehouse' catalog as Iceberg REST Catalog
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", iceberg_uri)
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", "lakehouse")
        # S3A FileSystem configurations for checkpoints & raw storage
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Structured Streaming checkpoint & RocksDB state store settings
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
        )
        # Tuning shuffle partitions for local streaming micro-batches
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
    )

    return builder.getOrCreate()


if __name__ == "__main__":
    spark = get_spark_session("test-iceberg-connection")
    print(f"Spark Version: {spark.version}")
    print("Testing Iceberg catalog namespaces...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.fraud_detection")
    namespaces = spark.sql("SHOW NAMESPACES IN lakehouse").collect()
    print(f"Namespaces in Iceberg: {[row[0] for row in namespaces]}")
    spark.stop()
