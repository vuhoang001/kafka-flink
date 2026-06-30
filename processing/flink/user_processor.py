"""
Flink Streaming Job — ghi raw data vào Bronze layer (Medallion Architecture).

Catalog : Nessie REST catalog (http://nessie:19120/iceberg)
Storage : MinIO s3://warehouse/ (S3FileIO từ iceberg-aws-bundle)
Layer   : Bronze — lưu nguyên xi từ Kafka, thêm ingested_at để trace

Pipeline 1 (API):
  Kafka [users_created]        → iceberg.bronze.api_users_raw

Pipeline 2 (CDC):
  Kafka [postgres.public.users] → iceberg.bronze.cdc_users_raw

Silver/Gold được xử lý bởi dbt chạy trên Trino (query/dbt/).

Cách chạy:
  docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/user_processor.py

Query operational (dữ liệu tươi, 30s latency):
  docker exec -it trino trino
  > SELECT * FROM iceberg.bronze.api_users_raw ORDER BY ingested_at DESC LIMIT 20;

Query kết quả đã xử lý:
  > SELECT * FROM iceberg.gold.users_enriched LIMIT 20;
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # Iceberg commit mỗi 30s khi checkpoint hoàn thành
    env.enable_checkpointing(30000)

    t_env = StreamTableEnvironment.create(env)

    # ── Iceberg REST catalog (tabulario/iceberg-rest) ────────────────────
    t_env.execute_sql("""
        CREATE CATALOG iceberg WITH (
            'type'                 = 'iceberg',
            'catalog-type'         = 'rest',
            'uri'                  = 'http://iceberg-rest:8181',
            'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint'          = 'http://minio:9000',
            's3.access-key-id'     = 'minio',
            's3.secret-access-key' = 'minio123',
            's3.path-style-access' = 'true',
            's3.region'            = 'us-east-1'
        )
    """)
    # Bronze: raw data nguyên xi từ Kafka
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg.bronze")

    # ── Kafka sources ────────────────────────────────────────────────────
    t_env.execute_sql("""
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
            'properties.bootstrap.servers' = 'broker:29092',
            'properties.group.id'          = 'flink-api-group',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS cdc_source (
            after ROW<id INT, name STRING, email STRING, department STRING>,
            op     STRING,
            ts_ms  BIGINT
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'postgres.public.users',
            'properties.bootstrap.servers' = 'broker:29092',
            'properties.group.id'          = 'flink-cdc-group',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json',
            'json.ignore-parse-errors'     = 'true'
        )
    """)

    # ── Bronze sink tables (giữ nguyên raw, chỉ thêm ingested_at) ───────
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
            id          INT,
            name        STRING,
            email       STRING,
            department  STRING,
            op          STRING,
            source_ts_ms BIGINT,
            ingested_at TIMESTAMP(3)
        ) WITH (
            'format-version'       = '2',
            'write.format.default' = 'parquet'
        )
    """)

    # ── Pipelines ────────────────────────────────────────────────────────
    stmt_set = t_env.create_statement_set()

    # Pipeline 1: API → Bronze (giữ tất cả fields gốc)
    stmt_set.add_insert_sql("""
        INSERT INTO iceberg.bronze.api_users_raw
        SELECT
            first_name,
            last_name,
            gender,
            postcode,
            email,
            username,
            dob,
            phone,
            picture,
            CURRENT_TIMESTAMP
        FROM default_catalog.default_database.api_source
    """)

    # Pipeline 2: CDC → Bronze (giữ op + ts_ms để trace lịch sử)
    stmt_set.add_insert_sql("""
        INSERT INTO iceberg.bronze.cdc_users_raw
        SELECT
            after.id,
            after.name,
            after.email,
            after.department,
            op,
            ts_ms,
            CURRENT_TIMESTAMP
        FROM default_catalog.default_database.cdc_source
        WHERE after IS NOT NULL
    """)

    stmt_set.execute()


if __name__ == "__main__":
    main()
