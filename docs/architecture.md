# Kiến trúc hệ thống

## Tổng quan

Hệ thống xây dựng theo **Medallion Architecture** — phân tầng dữ liệu Bronze → Silver → Gold theo chuẩn Data Lakehouse.

Dữ liệu nhận từ 2 nguồn:
- **FastAPI**: người dùng/ứng dụng đẩy HTTP request
- **PostgreSQL CDC**: Debezium đọc WAL, phát hiện INSERT/UPDATE/DELETE tự động

## Data flow chi tiết

```
[FastAPI :8000]
      │  POST /users (JSON)
      ▼
[Kafka topic: users_created]
      │
      ├──────────────────────────────────────────────────────────┐
      │                                                          │
      ▼  stream liên tục                                         │
[Flink 1.17]                                                    │
      │  SELECT * + CURRENT_TIMESTAMP AS ingested_at            │
      ▼                                                          │
[Bronze: iceberg.bronze.api_users_raw]                          │
  Parquet trên MinIO s3://warehouse/                            │
  Visible sau mỗi Flink checkpoint (30s)                        │
                                                                 │
[PostgreSQL :5555]                                              │
      │  WAL, wal_level=logical                                  │
      ▼                                                          │
[Debezium :8083]                                                │
      │  pgoutput plugin                                         │
      ▼                                                          │
[Kafka topic: postgres.public.users]                            │
  {"after":{id,name,email,dept},"op":"c/u/d","ts_ms":...}      │
      │                                                          │
      └────────────────────────────────────────────►[Flink 1.17]┘
                                                         │
                                                         ▼
                                          [Bronze: iceberg.bronze.cdc_users_raw]

         │
         │ Spark batch  (mỗi 15 phút, incremental MERGE INTO)
         ▼
[Silver: iceberg.silver.api_users]    [Silver: iceberg.silver.cdc_users]
  - trim, lower, full_name concat       - Bỏ op='d' (DELETE)
  - dedup by username (row_number)      - dedup by id (row_number by source_ts)
  - watermark: ingested_at > max_ts     - watermark: ingested_at > max_ts

         │
         │ Spark batch  (sau Silver, full rebuild)
         ▼
[Gold: iceberg.gold.users_enriched]   [Gold: iceberg.gold.user_stats]
  - LEFT JOIN api + cdc ON email        - GROUP BY gender, dept, birth_year
  - hợp nhất thông tin 2 nguồn          - COUNT, MIN/MAX ingested_at

         │  SQL query
         ▼
[Trino :8080]  đọc tất cả layers qua Nessie REST catalog
```

## Catalog & Storage

```
Nessie REST Catalog (:19120)
  ├── Lưu Iceberg metadata (snapshots, manifests, partition spec, schema)
  ├── Backing store: PostgreSQL database "nessiedb" (tách riêng khỏi "mydb")
  └── URI: http://nessie:19120/iceberg  (Flink RESTCatalog tự thêm /v1)

MinIO Object Storage (:9000 API / :9001 Console)
  └── Bucket: warehouse/
        ├── bronze/api_users_raw/data/*.parquet
        ├── bronze/cdc_users_raw/data/*.parquet
        ├── silver/api_users/data/*.parquet
        ├── silver/cdc_users/data/*.parquet
        ├── gold/users_enriched/data/*.parquet
        └── gold/user_stats/data/*.parquet

I/O impl: S3FileIO (iceberg-aws-bundle)
  s3.path-style-access = true  → vd: http://minio:9000/warehouse/bronze/...
  warehouse prefix: s3://      → KHÔNG dùng s3a://
```

## Quyết định thiết kế

### Iceberg 1.5.2 (không phải 1.6.x)
Iceberg 1.6.x đã drop support Flink 1.17. Version 1.5.2 là bản cuối cùng hỗ trợ đồng thời Flink 1.17 và Spark 3.5.

### S3FileIO thay vì s3a:// (HadoopFileIO)
Flink dùng plugin classloader riêng cho `flink-s3-fs-hadoop`. Nếu Iceberg cùng dùng `s3a://`, 2 classloader conflict → runtime error khi load class. Dùng `S3FileIO` từ `iceberg-aws-bundle` chạy trong classloader riêng, tránh hoàn toàn conflict.

### Nessie thay vì HadoopCatalog
HadoopCatalog không safe với concurrent write (nhiều Flink task slot ghi cùng lúc). Nessie cung cấp:
- REST API chuẩn (Iceberg REST Catalog Spec)
- Snapshot isolation cho concurrent write
- Metadata durable trong PostgreSQL
- Git-like branching (mặc định branch `main`)

### Flink checkpoint 30 giây
Iceberg là file-based format — data chỉ visible sau khi Flink commit file Parquet hoàn chỉnh. Commit xảy ra sau mỗi checkpoint. 30s là trade-off giữa latency (~30s delay) và số lượng small files.

### Spark cho Silver/Gold thay vì dbt
dbt-trino chạy SQL qua Trino HTTP — Trino chỉ đọc Iceberg, không hỗ trợ MERGE INTO. PySpark dùng Iceberg Spark Extensions hỗ trợ MERGE INTO đầy đủ, chạy native trên Spark cluster.

### Silver incremental, Gold full rebuild
**Silver** MERGE INTO: Bronze có thể có duplicate (retry API, CDC initial snapshot). Watermark đảm bảo chỉ xử lý data mới, idempotent khi chạy lại.

**Gold** full rebuild: Cần nhìn toàn bộ Silver để JOIN + aggregate. Silver sau dedup đã gọn, rebuild Gold nhanh và luôn nhất quán.

## Thứ tự khởi động services

```
postgres ──────────────────────────────► nessie
         └─► broker ──► kafka-connect ──► connector-init
                    └─► minio ──► minio-init

nessie + minio ──► spark-master ──► spark-worker
nessie + minio ──► trino
broker + minio ──► flink-jobmanager ──► flink-taskmanager
broker         ──► api
```
