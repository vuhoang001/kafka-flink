"""
Flink Streaming Job: Kafka → Transform → Kafka

Flow:
  Kafka topic [users_created]
      ↓  (Flink reads, parse JSON)
  Transform: ghép tên, tính tuổi
      ↓
  Kafka topic [users_processed]

Cách chạy (sau khi docker-compose up):
  docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/user_processor.py

Xem kết quả:
  Vào http://localhost:18081 để xem Flink Web UI
  Hoặc consume topic users_processed:
  docker exec broker kafka-console-consumer \
    --bootstrap-server broker:29092 \
    --topic users_processed --from-beginning
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    t_env = StreamTableEnvironment.create(env)

    # ---------- SOURCE ----------
    # Đọc dữ liệu từ Kafka topic users_created, parse dưới dạng JSON
    t_env.execute_sql("""
        CREATE TABLE users_created (
            first_name      STRING,
            last_name       STRING,
            gender          STRING,
            postcode        STRING,
            email           STRING,
            username        STRING,
            dob             STRING,
            registered_date STRING,
            phone           STRING,
            picture         STRING
        ) WITH (
            'connector'                     = 'kafka',
            'topic'                         = 'users_created',
            'properties.bootstrap.servers'  = 'broker:29092',
            'properties.group.id'           = 'flink-consumer-group',
            'scan.startup.mode'             = 'earliest-offset',
            'format'                        = 'json'
        )
    """)

    # ---------- SINK ----------
    # Ghi kết quả đã xử lý vào Kafka topic users_processed
    t_env.execute_sql("""
        CREATE TABLE users_processed (
            full_name   STRING,
            gender      STRING,
            email       STRING,
            username    STRING,
            birth_year  STRING
        ) WITH (
            'connector'                     = 'kafka',
            'topic'                         = 'users_processed',
            'properties.bootstrap.servers'  = 'broker:29092',
            'format'                        = 'json'
        )
    """)

    # ---------- TRANSFORM ----------
    # Flink SQL transform: ghép tên và cắt năm sinh từ ISO timestamp
    t_env.execute_sql("""
        INSERT INTO users_processed
        SELECT
            CONCAT(first_name, ' ', last_name)  AS full_name,
            gender,
            email,
            username,
            SUBSTRING(dob, 1, 4)                AS birth_year
        FROM users_created
    """)


if __name__ == "__main__":
    main()
