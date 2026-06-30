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
[Trino :8080]  đọc tất cả layers qua Iceberg REST catalog (:8181)
```

## Catalog & Storage

```
Iceberg REST Catalog (:8181)  —  tabulario/iceberg-rest
  ├── Lưu Iceberg metadata (snapshots, manifests, partition spec, schema)
  ├── Backing store: SQLite in-memory (embedded, không cần DB ngoài)
  └── URI: http://iceberg-rest:8181

MinIO Object Storage (:9000 API / :9001 Console)
  └── Bucket: warehouse/
        ├── bronze/api_users_raw/data/*.parquet
        ├── bronze/cdc_users_raw/data/*.parquet
        ├── silver/api_users/data/*.parquet
        ├── silver/cdc_users/data/*.parquet
        ├── gold/users_enriched/data/*.parquet
        └── gold/user_stats/data/*.parquet

I/O impl   : S3FileIO (từ iceberg-aws-bundle)
scheme     : s3://warehouse/  (KHÔNG phải s3a://)
path-style : true  (MinIO yêu cầu path-style, không hỗ trợ virtual-hosted)
endpoint   : http://minio:9000
```

## Quyết định thiết kế

### Iceberg 1.5.2 (không phải 1.6.x)
Iceberg 1.6.x đã drop support Flink 1.17. Version 1.5.2 là bản cuối cùng hỗ trợ đồng thời Flink 1.17 và Spark 3.5.

### S3FileIO thay vì s3a:// (HadoopFileIO)
Flink dùng plugin classloader riêng cho `flink-s3-fs-hadoop`. Nếu Iceberg cùng dùng `s3a://`, 2 classloader conflict → runtime error khi load class. Dùng `S3FileIO` từ `iceberg-aws-bundle` chạy trong classloader riêng, tránh hoàn toàn conflict.

### tabulario/iceberg-rest thay vì HadoopCatalog
HadoopCatalog không safe với concurrent write (nhiều Flink task slot ghi cùng lúc). `tabulario/iceberg-rest` cung cấp:
- REST API chuẩn (Iceberg REST Catalog Spec v1)
- Snapshot isolation cho concurrent write
- Không cần cấu hình database ngoài — nhẹ hơn nhiều so với Nessie
- Tương thích với `catalog-type = 'rest'` trong cả Flink, Spark, Trino

### security.delegation.tokens.enabled: false (Flink)
Flink 1.17 có `HadoopFSDelegationTokenProvider` — gọi `UserGroupInformation` khi khởi động. Nếu thiếu Kerberos config, UGI static init fail → `NoClassDefFoundError`. Tắt bằng `security.delegation.tokens.enabled: false` để vô hiệu hóa hoàn toàn `DefaultDelegationTokenManager`.

### Flink checkpoint 30 giây
Iceberg là file-based format — data chỉ visible sau khi Flink commit file Parquet hoàn chỉnh. Commit xảy ra sau mỗi checkpoint. 30s là trade-off giữa latency (~30s delay) và số lượng small files.

### Spark cho Silver/Gold thay vì dbt
dbt-trino chạy SQL qua Trino HTTP — Trino chỉ đọc Iceberg, không hỗ trợ MERGE INTO. PySpark dùng Iceberg Spark Extensions hỗ trợ MERGE INTO đầy đủ, chạy native trên Spark cluster.

### Silver incremental, Gold full rebuild
**Silver** MERGE INTO: Bronze có thể có duplicate (retry API, CDC initial snapshot). Watermark đảm bảo chỉ xử lý data mới, idempotent khi chạy lại.

**Gold** full rebuild: Cần nhìn toàn bộ Silver để JOIN + aggregate. Silver sau dedup đã gọn, rebuild Gold nhanh và luôn nhất quán.

## Thứ tự khởi động services

```
minio ──────────────────────────────► minio-init
      └─► iceberg-rest
      └─► broker ──► kafka-connect ──► connector-init
                                  └─► postgres ──► kafka-connect

minio + iceberg-rest ──► spark-master ──► spark-worker
minio + iceberg-rest ──► trino
broker + minio       ──► flink-jobmanager ──► flink-taskmanager
broker               ──► api
```
