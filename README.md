# Realtime Data Streaming Pipeline

Pipeline xử lý dữ liệu real-time theo **Medallion Architecture** (Bronze → Silver → Gold).

```
Kafka → Flink → Bronze ──(dbt 15 phút)──► Silver ──► Gold
                  │                                    │
            query real-time                    query business
            (30s latency)                      (processed)
```

---

## Tài liệu

| Tài liệu | Mô tả |
|----------|-------|
| [Kiến trúc](docs/architecture.md) | Sơ đồ tổng quan, luồng data, lý do chọn công nghệ |
| [Cài đặt & Khởi động](docs/setup.md) | Hướng dẫn chạy từng bước, verify, rebuild |
| **Layers** | |
| [Bronze](docs/layers/bronze.md) | Raw data — schema, query operational data |
| [Silver](docs/layers/silver.md) | Clean data — incremental merge, deduplication |
| [Gold](docs/layers/gold.md) | Business data — join, aggregation, lineage |
| **Components** | |
| [Flink](docs/components/flink.md) | JAR cần thiết, pipeline config, tuning |
| [Kafka & Debezium](docs/components/kafka.md) | Topics, CDC format, quản lý connector |
| [Trino](docs/components/trino.md) | Query guide, time travel, metadata tables |
| [dbt](docs/components/dbt.md) | Materialization, incremental, troubleshooting |
| [Ingestion](docs/components/ingestion.md) | FastAPI, Airflow DAG, CDC |

---

## Cấu trúc thư mục

```
realtime-data-streaming/
├── ingestion/
│   ├── api/                    # FastAPI — HTTP push vào Kafka
│   └── dags/
│       ├── kafka_stream.py     # Airflow: pull randomuser.me → Kafka (hàng ngày)
│       └── dbt_pipeline.py     # Airflow: chạy dbt Bronze→Silver→Gold (15 phút)
├── cdc/
│   └── postgres-connector.json # Debezium CDC config
├── processing/
│   └── flink/
│       └── user_processor.py   # Flink job: Kafka → Bronze Iceberg
├── storage/
│   └── postgres/
│       └── init.sql            # Schema + CREATE DATABASE nessiedb
├── query/
│   ├── trino/etc/              # Trino config + catalog
│   ├── dbt/
│   │   └── models/
│   │       ├── bronze/         # View → Bronze tables
│   │       ├── silver/         # Incremental merge (clean)
│   │       └── gold/           # Full rebuild (business)
│   └── duckdb/
│       └── query_minio.py      # Query MinIO trực tiếp (không cần Trino)
└── docs/
    ├── architecture.md
    ├── setup.md
    ├── layers/
    │   ├── bronze.md
    │   ├── silver.md
    │   └── gold.md
    └── components/
        ├── flink.md
        ├── kafka.md
        ├── trino.md
        ├── dbt.md
        └── ingestion.md
```

---

## Quick Start

```bash
# 1. Khởi động
docker compose up -d

# 2. Submit Flink job (ghi vào Bronze)
docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/user_processor.py

# 3. Đẩy data
curl -X POST http://localhost:8000/users -H "Content-Type: application/json" \
  -d '{"first_name":"Test","last_name":"User","gender":"male","postcode":"100000",
       "email":"test@example.com","username":"testuser",
       "dob":"1995-01-01T00:00:00Z","phone":"0901234567"}'

# 4. Đợi 30s, query Bronze (operational)
docker exec -it trino trino \
  --execute "SELECT * FROM iceberg.bronze.api_users_raw ORDER BY ingested_at DESC LIMIT 5"

# 5. Chạy dbt → Silver + Gold
cd query/dbt && dbt run --profiles-dir .

# 6. Query Gold (processed)
docker exec -it trino trino \
  --execute "SELECT * FROM iceberg.gold.users_enriched LIMIT 5"
```

---

## UI

| Service       | URL                        |
|---------------|----------------------------|
| API Swagger   | http://localhost:8000/docs |
| Flink Web UI  | http://localhost:18081     |
| MinIO Console | http://localhost:9001      |
| Trino Web UI  | http://localhost:8080      |
| Nessie        | http://localhost:19120     |
