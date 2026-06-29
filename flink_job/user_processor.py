"""
Flink Streaming Job — 2 pipeline chạy song song trong 1 job:

Pipeline 1 (API):
  Kafka [users_created]  →  MinIO s3a://warehouse/api/

Pipeline 2 (CDC):
  Kafka [postgres.public.users]  →  MinIO s3a://warehouse/cdc/
  (format Debezium JSON: Flink tự hiểu INSERT/UPDATE/DELETE)

Cách chạy:
  docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/user_processor.py

Xem kết quả:
  Flink Web UI  : http://localhost:18081
  MinIO Console : http://localhost:9001  (user: minio / pass: minio123)
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def create_api_tables(t_env):
    # SOURCE: Kafka topic users_created, format JSON thuần
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS api_source (
            first_name  STRING,
            last_name   STRING,
            gender      STRING,
            postcode    STRING,
            email       STRING,
            username    STRING,
            dob         STRING,
            phone       STRING,
            picture     STRING
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'users_created',
            'properties.bootstrap.servers' = 'broker:29092',
            'properties.group.id'          = 'flink-api-group',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """)

    # SINK: ghi file JSON vào MinIO, cuộn file sau mỗi 5 phút
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS api_sink (
            full_name  STRING,
            gender     STRING,
            email      STRING,
            username   STRING,
            birth_year STRING
        ) WITH (
            'connector'                              = 'filesystem',
            'path'                                   = 's3a://warehouse/api/',
            'format'                                 = 'json',
            'sink.rolling-policy.rollover-interval'  = '5 min',
            'sink.rolling-policy.check-interval'     = '1 min'
        )
    """)


def create_cdc_tables(t_env):
    # SOURCE: đọc raw Debezium JSON, chỉ lấy field after + op
    # filesystem sink chỉ hỗ trợ append-only nên không dùng debezium-json changelog
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS cdc_source (
            after ROW<id INT, name STRING, email STRING, department STRING>,
            op    STRING
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

    # SINK: ghi vào MinIO, mỗi partition có folder riêng theo department
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS cdc_sink (
            id         INT,
            name       STRING,
            email      STRING,
            department STRING
        ) WITH (
            'connector'                             = 'filesystem',
            'path'                                  = 's3a://warehouse/cdc/',
            'format'                                = 'json',
            'sink.rolling-policy.rollover-interval' = '5 min',
            'sink.rolling-policy.check-interval'    = '1 min'
        )
    """)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(30000)  # checkpoint mỗi 30 giây để flush file xuống MinIO

    t_env = StreamTableEnvironment.create(env)

    create_api_tables(t_env)
    create_cdc_tables(t_env)

    # StatementSet cho phép chạy nhiều INSERT trong cùng 1 Flink job
    stmt_set = t_env.create_statement_set()

    stmt_set.add_insert_sql("""
        INSERT INTO api_sink
        SELECT
            CONCAT(first_name, ' ', last_name) AS full_name,
            gender,
            email,
            username,
            SUBSTRING(dob, 1, 4)               AS birth_year
        FROM api_source
    """)

    stmt_set.add_insert_sql("""
        INSERT INTO cdc_sink
        SELECT
            after.id,
            after.name,
            after.email,
            after.department
        FROM cdc_source
        WHERE op = 'c' OR op = 'r'
    """)

    stmt_set.execute()


if __name__ == "__main__":
    main()
