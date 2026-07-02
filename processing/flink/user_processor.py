"""
Flink Streaming Job — ghi RAW payload vào Bronze layer (schema-on-read).

Catalog : Iceberg REST catalog (iceberg-rest:8181)
Storage : MinIO s3://warehouse/ (S3FileIO từ iceberg-aws-bundle)
Layer   : Bronze — KHÔNG parse JSON. Mỗi Kafka message giữ nguyên xi trong cột
          `payload`, kèm metadata Kafka (topic/partition/offset/timestamp) để
          trace + dedup. Việc parse & chuẩn hoá schema là việc của Silver (Spark).

Ưu điểm của thiết kế này:
  - Producer thêm/bớt field thoải mái — Bronze không bao giờ vỡ schema.
  - Không mất data: field nào cũng nằm trong payload, kể cả field chưa ai dùng.
  - Thêm nguồn mới = thêm 1 dòng vào TOPICS, không phải viết pipeline mới.

Pipeline:
  Kafka [users_created]         → iceberg.bronze.api_users_raw
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

# topic Kafka → (tên bảng source tạm trong Flink, bảng Bronze, consumer group)
TOPICS = {
    "users_created":         ("api_source", "api_users_raw", "flink-api-group"),
    "postgres.public.users": ("cdc_source", "cdc_users_raw", "flink-cdc-group"),
}


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

    stmt_set = t_env.create_statement_set()

    for topic, (source_table, bronze_table, group_id) in TOPICS.items():
        # Source: 'format' = 'raw' → cả message thành 1 cột STRING, không parse.
        # Các cột METADATA VIRTUAL do Kafka connector cung cấp, không nằm trong message.
        t_env.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS {source_table} (
                payload         STRING,
                kafka_topic     STRING           METADATA FROM 'topic'     VIRTUAL,
                kafka_partition INT              METADATA FROM 'partition' VIRTUAL,
                kafka_offset    BIGINT           METADATA FROM 'offset'    VIRTUAL,
                kafka_ts        TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
            ) WITH (
                'connector'                    = 'kafka',
                'topic'                        = '{topic}',
                'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
                'properties.group.id'          = '{group_id}',
                'scan.startup.mode'            = 'earliest-offset',
                'format'                       = 'raw'
            )
        """)

        t_env.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS iceberg.bronze.{bronze_table} (
                payload         STRING,
                kafka_topic     STRING,
                kafka_partition INT,
                kafka_offset    BIGINT,
                kafka_ts        TIMESTAMP(3),
                ingested_at     TIMESTAMP(3)
            ) WITH (
                'format-version'       = '2',
                'write.format.default' = 'parquet'
            )
        """)

        # Không WHERE, không transform — Bronze nhận tất cả, kể cả event DELETE
        # của CDC (after=null) hay message "lạ". Lọc là việc của Silver.
        stmt_set.add_insert_sql(f"""
            INSERT INTO iceberg.bronze.{bronze_table}
            SELECT
                payload,
                kafka_topic,
                kafka_partition,
                kafka_offset,
                CAST(kafka_ts AS TIMESTAMP(3)),
                CURRENT_TIMESTAMP
            FROM default_catalog.default_database.{source_table}
        """)

    stmt_set.execute()


if __name__ == "__main__":
    main()
