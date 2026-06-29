# Kiến trúc hệ thống

## Tổng quan

Pipeline xử lý dữ liệu real-time theo mô hình **Medallion Architecture**, gồm 5 tầng:

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ TẦNG 1 — INGESTION                                                           │
│                                                                              │
│   [FastAPI]           [Airflow DAG]           [PostgreSQL]                   │
│   HTTP push           Scheduled pull          Transactional DB               │
│   ingestion/api/      ingestion/dags/         storage/postgres/              │
└────────┬──────────────────────┬─────────────────────┬────────────────────────┘
         │                      │                     │ (Debezium CDC)
         ▼                      ▼                     ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ TẦNG 2 — MESSAGE BUS                                                         │
│                                                                              │
│   [Kafka - KRaft mode]                                                       │
│   topic: users_created          ← API + Airflow ghi vào                     │
│   topic: postgres.public.users  ← Debezium (CDC) ghi vào                   │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ TẦNG 3 — STREAM PROCESSING                                                   │
│                                                                              │
│   [Apache Flink 1.17]                                                        │
│   Pipeline 1: users_created        → bronze.api_users_raw                   │
│   Pipeline 2: postgres.public.users → bronze.cdc_users_raw                  │
│                                                                              │
│   Commit xuống storage mỗi 30 giây (checkpoint interval)                    │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ TẦNG 4 — MEDALLION STORAGE (Apache Iceberg on MinIO)                        │
│                                                                              │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────────────┐   │
│  │ BRONZE          │   │ SILVER          │   │ GOLD                    │   │
│  │─────────────────│   │─────────────────│   │─────────────────────────│   │
│  │ api_users_raw   │──►│ api_users       │──►│ users_enriched          │   │
│  │ cdc_users_raw   │──►│ cdc_users       │   │ user_stats              │   │
│  │                 │   │                 │   │                         │   │
│  │ Raw, immutable  │   │ Clean, deduped  │   │ Joined, aggregated      │   │
│  │ Flink writes    │   │ dbt incremental │   │ dbt full rebuild        │   │
│  └─────────────────┘   └─────────────────┘   └─────────────────────────┘   │
│                                                                              │
│  Metadata catalog: Nessie (REST Catalog)                                    │
│  Physical storage: MinIO S3  s3://warehouse/                                │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ TẦNG 5 — QUERY & TRANSFORM                                                   │
│                                                                              │
│   [Trino 435]                         [dbt-trino]                           │
│   SQL query engine                    Transformation layer                  │
│   Đọc mọi layer qua Nessie            Chạy Silver + Gold models             │
│   localhost:8080                      Airflow DAG mỗi 15 phút               │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Luồng dữ liệu theo thời gian

```
t=0s      Data được đẩy vào Kafka (API / Airflow / CDC)
t=30s     Flink checkpoint → Iceberg commit → data có trong Bronze
t=15m     Airflow kích hoạt dbt → Silver được cập nhật (incremental)
t=15m+    dbt chạy Gold → users_enriched + user_stats rebuild
```

---

## Lý do chọn từng công nghệ

| Công nghệ | Vai trò | Lý do chọn |
|-----------|---------|------------|
| **Kafka** | Message bus | Decoupling ingestion ↔ processing, replay được |
| **Debezium** | CDC | Đọc PostgreSQL WAL không cần sửa app |
| **Flink** | Stream processing | Stateful, exactly-once, Java ecosystem |
| **Iceberg** | Table format | ACID, time travel, schema evolution trên S3 |
| **Nessie** | Catalog | REST catalog nhẹ, hỗ trợ cả Flink và Trino |
| **MinIO** | Object storage | S3-compatible, chạy local |
| **Trino** | Query engine | Đọc Iceberg nhanh, ANSI SQL, multi-user |
| **dbt** | Transform | SQL-first, lineage, incremental, test |

---

## Latency theo layer

| Layer  | Cập nhật bởi        | Latency từ lúc ingest |
|--------|---------------------|-----------------------|
| Bronze | Flink (checkpoint)  | ~30 giây              |
| Silver | dbt (incremental)   | ~15 phút              |
| Gold   | dbt (full rebuild)  | ~15–16 phút           |

---

## Sơ đồ dependency giữa các service

```
postgres ──────────────────────────► debezium (kafka-connect)
    │                                        │
    └──► nessie (catalog metadata)           │
                                             ▼
broker (kafka) ◄───────────────── api / airflow
    │
    ▼
flink-jobmanager + flink-taskmanager
    │
    ▼ (Iceberg via Nessie)
minio ◄───────────────────────── nessie
    │
    ▼
trino ──────────────────────────► dbt
```
