"""
Helper tạo SparkSession với Iceberg REST catalog + MinIO config.
Import module này trong mọi Spark job.
"""

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        # Iceberg extension cho MERGE INTO, time travel, v.v.
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Catalog "iceberg" → Iceberg REST catalog
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/")
        # S3FileIO — dùng AWS SDK v2 để đọc/ghi MinIO
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.iceberg.s3.access-key-id", "minio")
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minio123")
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
        .getOrCreate()
    )
