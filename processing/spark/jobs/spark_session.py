"""
Helper tạo SparkSession với Iceberg REST catalog + MinIO config.
Đọc config từ environment variables — không hardcode credential.
Import module này trong mọi Spark job.
"""

import os

from pyspark.sql import SparkSession

ICEBERG_REST_URI      = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
MINIO_ENDPOINT        = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET          = os.environ.get("MINIO_BUCKET", "warehouse")
AWS_ACCESS_KEY_ID     = os.environ.get("AWS_ACCESS_KEY_ID", "minio")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minio123")
AWS_REGION            = os.environ.get("AWS_REGION", "us-east-1")


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg",           "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type",      "rest")
        .config("spark.sql.catalog.iceberg.uri",       ICEBERG_REST_URI)
        .config("spark.sql.catalog.iceberg.warehouse", f"s3://{MINIO_BUCKET}/")
        .config("spark.sql.catalog.iceberg.io-impl",   "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint",          MINIO_ENDPOINT)
        .config("spark.sql.catalog.iceberg.s3.access-key-id",     AWS_ACCESS_KEY_ID)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", AWS_SECRET_ACCESS_KEY)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.region",            AWS_REGION)
        .getOrCreate()
    )
