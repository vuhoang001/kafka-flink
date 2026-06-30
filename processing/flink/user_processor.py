"""
Flink Streaming Job — ghi raw data vào Bronze layer (Medallion Architecture).

Catalog : Iceberg REST catalog (iceberg-rest:8181)
Storage : MinIO s3://warehouse/ (S3FileIO từ iceberg-aws-bundle)
Layer   : Bronze — lưu nguyên xi từ Kafka, thêm ingested_at để trace

Pipeline 1 (API):
  Kafka [users_created]        → iceberg.bronze.api_users_raw

Pipeline 2 (CDC):
  Kafka [postgres.public.users] → iceberg.bronze.cdc_users_raw

Cách chạy:
  docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/user_processor.py
"""

import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

ICEBERG_REST_URI       = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
MINIO_ENDPOINT         = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID      = os.environ.get("AWS_ACCESS_KEY_ID", "minio")
AWS_SECRET_ACCESS_KEY  = os.environ.get("AWS_SECRET_ACCESS_KEY", "minio123")
AWS_REGION             = os.environ.get("AWS_REGION", "us-east-1")
KAFKA_BOOTSTRAP        = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(30000)

    t_env = StreamTableEnvironment.create(env)

    # ── Iceberg REST catalog ──────────────────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE CATALOG iceberg WITH (
            'type'                 = 'iceberg',
            'catalog-type'         = 'rest',
            'uri'                  = '{ICEBERG_REST_URI}',
            'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint'          = '{MINIO_ENDPOINT}',
            's3.access-key-id'     = '{AWS_ACCESS_KEY_ID}',
            's3.secret-access-key' = '{AWS_SECRET_ACCESS_KEY}',
            's3.path-style-access' = 'true',
            'client.region'        = '{AWS_REGION}'
        )
    """)
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg.bronze")

    # ── Kafka sources ─────────────────────────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS api_source (
            first_name STRING,
            last_name  STRING,
            gender     STRING,
            postcode   STRING,
            email      STRING,
            username   STRING,
            dob        STRING,
            phone      STRING,
            picture    STRING
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'users_created',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'          = 'flink-api-group',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS cdc_source (
            after ROW<id INT, name STRING, email STRING, department STRING>,
            op     STRING,
            ts_ms  BIGINT
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'postgres.public.users',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'          = 'flink-cdc-group',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json',
            'json.ignore-parse-errors'     = 'true'
        )
    """)

    # ── Bronze sink tables ────────────────────────────────────────────────────
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS iceberg.bronze.api_users_raw (
            first_name  STRING,
            last_name   STRING,
            gender      STRING,
            postcode    STRING,
            email       STRING,
            username    STRING,
            dob         STRING,
            phone       STRING,
            picture     STRING,
            ingested_at TIMESTAMP(3)
        ) WITH (
            'format-version'       = '2',
            'write.format.default' = 'parquet'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS iceberg.bronze.cdc_users_raw (
            id           INT,
            name         STRING,
            email        STRING,
            department   STRING,
            op           STRING,
            source_ts_ms BIGINT,
            ingested_at  TIMESTAMP(3)
        ) WITH (
            'format-version'       = '2',
            'write.format.default' = 'parquet'
        )
    """)

    # ── Pipelines ─────────────────────────────────────────────────────────────
    stmt_set = t_env.create_statement_set()

    stmt_set.add_insert_sql("""
        INSERT INTO iceberg.bronze.api_users_raw
        SELECT
            first_name, last_name, gender, postcode,
            email, username, dob, phone, picture,
            CURRENT_TIMESTAMP
        FROM default_catalog.default_database.api_source
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO iceberg.bronze.cdc_users_raw
        SELECT
            after.id, after.name, after.email, after.department,
            op, ts_ms,
            CURRENT_TIMESTAMP
        FROM default_catalog.default_database.cdc_source
        WHERE after IS NOT NULL
    """)

    stmt_set.execute()


if __name__ == "__main__":
    main()
