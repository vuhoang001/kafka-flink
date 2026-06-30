# Realtime Data Streaming — Medallion Architecture

Pipeline xử lý dữ liệu realtime theo Medallion Architecture (Bronze → Silver → Gold) trên Apache Flink, Apache Spark, Apache Iceberg, và Trino.

## Kiến trúc tổng quan

```
┌──────────────────────────────────────────────────────────────────┐
│                        INGESTION SOURCES                         │
│                                                                  │
│   ┌──────────────┐            ┌────────────────────────────┐    │
│   │   FastAPI    │            │       PostgreSQL 15         │    │
│   │  POST /users │            │  WAL → Debezium CDC         │    │
│   └──────┬───────┘            └────────────┬───────────────┘    │
└──────────┼─────────────────────────────────┼────────────────────┘
           │ users_created                   │ postgres.public.users
           ▼                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                      Apache Kafka (KRaft)                        │
└──────────────────────────────┬───────────────────────────────────┘
                               │ stream liên tục
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Apache Flink 1.17.2                           │
│            checkpoint 30s → commit Iceberg files                 │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                  BRONZE  (raw, append-only)                      │
│  iceberg.bronze.api_users_raw    iceberg.bronze.cdc_users_raw   │
│                 MinIO  s3://warehouse/  (Parquet)                │
└──────────────────────────────┬───────────────────────────────────┘
                               │ Spark batch (15 phút)
                               │ MERGE INTO  (incremental upsert)
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                  SILVER  (clean, deduplicated)                   │
│  iceberg.silver.api_users        iceberg.silver.cdc_users       │
└──────────────────────────────┬───────────────────────────────────┘
                               │ Spark batch (sau Silver)
                               │ Full rebuild
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                  GOLD  (business-ready)                          │
│  iceberg.gold.users_enriched     iceberg.gold.user_stats        │
└──────────────────────────────┬───────────────────────────────────┘
                               │ SQL
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                       Trino 435                                  │
│         đọc tất cả layers qua Iceberg REST catalog              │
└──────────────────────────────────────────────────────────────────┘
```

## Stack công nghệ

| Thành phần | Version | Vai trò |
|------------|---------|---------|
| Apache Flink | 1.17.2 | Stream processing, ghi Bronze |
| Apache Kafka | 7.4.0 (Confluent) | Message broker (KRaft, không ZooKeeper) |
| Debezium | 2.4 | PostgreSQL CDC → Kafka |
| Apache Iceberg | 1.5.2 | Table format (ACID, schema evolution, time-travel) |
| Apache Spark | 3.5.1 | Batch transform: Bronze→Silver→Gold |
| tabulario/iceberg-rest | latest | Iceberg REST catalog server (metadata store) |
| MinIO | latest | Object storage S3-compatible |
| Trino | 435 | Distributed SQL query engine |
| FastAPI | — | HTTP ingestion API |
| PostgreSQL | 15 | Source DB (CDC) |

## Ports

| Service | Port | URL |
|---------|------|-----|
| FastAPI | 8888 | http://localhost:8888/docs |
| Flink Web UI | 18081 | http://localhost:18081 |
| Spark Web UI | 8090 | http://localhost:8090 |
| Trino Web UI | 8080 | http://localhost:8080 |
| MinIO Console | 9001 | http://localhost:9001 (minio / minio123) |
| Iceberg REST Catalog | 8181 | http://localhost:8181/v1/config |
| PostgreSQL | 5555 | localhost:5555/mydb (postgres/postgres) |
| Kafka | 9092 | localhost:9092 |

## Tài liệu

| Tài liệu | Mô tả |
|----------|-------|
| [Kiến trúc chi tiết](docs/architecture.md) | Data flow, lý do chọn công nghệ, quyết định thiết kế |
| [Setup & khởi động](docs/setup.md) | Cài đặt từng bước, requirements, verify |
| [Test end-to-end](docs/testing.md) | Hướng dẫn test toàn bộ luồng có kèm lệnh |
| [Bronze layer](docs/layers/bronze.md) | Schema, Flink pipeline, query operational data |
| [Silver layer](docs/layers/silver.md) | Incremental MERGE, dedup, Spark job |
| [Gold layer](docs/layers/gold.md) | JOIN + aggregation, full rebuild, Spark job |
| [Flink](docs/components/flink.md) | JARs, catalog config, submit job |
| [Kafka & Debezium](docs/components/kafka.md) | Topics, CDC format, connector |
| [Trino](docs/components/trino.md) | Query guide, catalog, time-travel |
| [Ingestion API](docs/components/ingestion.md) | FastAPI endpoints, schema |

## Cấu trúc thư mục

```
realtime-data-streaming/
├── docker-compose.yaml          # Toàn bộ services
├── cdc/
│   └── postgres-connector.json  # Debezium connector config
├── ingestion/
│   ├── api/                     # FastAPI HTTP ingestion
│   │   ├── main.py
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   └── dags/
│       └── spark_pipeline.py    # Airflow DAG: chạy Spark mỗi 15 phút
├── processing/
│   ├── flink/
│   │   ├── user_processor.py    # Flink job: Kafka → Bronze Iceberg
│   │   └── Dockerfile
│   └── spark/
│       ├── jobs/
│       │   ├── spark_session.py         # Spark+Iceberg session builder
│       │   ├── silver_transform.py      # Bronze → Silver (incremental MERGE)
│       │   └── gold_transform.py        # Silver → Gold (full rebuild)
│       └── Dockerfile
├── storage/
│   └── postgres/
│       └── init.sql             # Schema users (source cho Debezium CDC)
├── query/
│   ├── trino/etc/               # Trino config + iceberg catalog
│   └── dbt/                     # dbt-trino (không dùng cho transform chính)
└── docs/
    ├── architecture.md
    ├── setup.md
    ├── testing.md
    ├── layers/
    │   ├── bronze.md
    │   ├── silver.md
    │   └── gold.md
    └── components/
        ├── flink.md
        ├── kafka.md
        ├── trino.md
        └── ingestion.md
```

## Quick start

```bash
# 1. Khởi động toàn bộ stack
docker compose up -d

# 2. Submit Flink job
docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/user_processor.py

# 3. Gửi thử dữ liệu qua API
curl -X POST http://localhost:8888/users \
  -H "Content-Type: application/json" \
  -d '{"first_name":"Hoang","last_name":"Nguyen","gender":"male","postcode":"100000",
       "email":"hoang@example.com","username":"hoangnv",
       "dob":"1995-01-15","phone":"0901234567"}'

# 4. Đợi ~30s, query Bronze
docker exec -it trino trino \
  --execute "SELECT * FROM iceberg.bronze.api_users_raw ORDER BY ingested_at DESC LIMIT 5"

# 5. Chạy Spark Silver + Gold
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/spark_session.py \
  /opt/spark/jobs/silver_transform.py

docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/spark_session.py \
  /opt/spark/jobs/gold_transform.py

# 6. Query Gold
docker exec -it trino trino \
  --execute "SELECT * FROM iceberg.gold.users_enriched LIMIT 5"
```

Chi tiết từng bước xem tại [docs/testing.md](docs/testing.md).
