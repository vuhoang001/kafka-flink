# Trino

## Vai trò

Trino là **distributed SQL query engine** — đọc Iceberg tables từ MinIO qua Iceberg REST catalog bằng ANSI SQL. Không lưu data, chỉ đọc.

## Kết nối

```bash
# CLI interactive
docker exec -it trino trino

# Chạy query 1 lần
docker exec -it trino trino --execute "SELECT COUNT(*) FROM iceberg.bronze.api_users_raw"

# Kết nối từ ngoài (JDBC)
# URL: jdbc:trino://localhost:8080/iceberg
# User: trino (không cần password)
```

Web UI: http://localhost:8080 — xem query history, execution plan, worker status.

## Catalog config

File: `query/trino/etc/catalog/iceberg.properties`

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
iceberg.rest-catalog.warehouse=s3://warehouse/
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.aws-access-key=minio
s3.aws-secret-key=minio123
s3.path-style-access=true
```

## Queries theo layer

### Bronze — operational data (30s latency)

```sql
-- API users theo thời gian thực
SELECT username, email, gender, ingested_at
FROM iceberg.bronze.api_users_raw
ORDER BY ingested_at DESC
LIMIT 20;

-- CDC events gần nhất
SELECT id, name, op, ingested_at
FROM iceberg.bronze.cdc_users_raw
ORDER BY ingested_at DESC
LIMIT 20;

-- Tần suất events theo phút
SELECT
    DATE_TRUNC('minute', ingested_at) AS minute,
    COUNT(*) AS event_count
FROM iceberg.bronze.cdc_users_raw
GROUP BY 1
ORDER BY 1 DESC;
```

### Silver — clean data

```sql
-- Users đã normalize
SELECT username, full_name, email, gender, birth_year
FROM iceberg.silver.api_users
ORDER BY username;

-- CDC users (không có DELETE)
SELECT id, name, email, department
FROM iceberg.silver.cdc_users
ORDER BY id;
```

### Gold — business data

```sql
-- Enriched users
SELECT username, full_name, email, department, birth_year
FROM iceberg.gold.users_enriched
WHERE department IS NOT NULL
ORDER BY username;

-- Stats theo department
SELECT department, SUM(total_users) as total
FROM iceberg.gold.user_stats
GROUP BY department
ORDER BY total DESC;

-- Match rate giữa API và DB
SELECT
    COUNT(*) as total_api_users,
    COUNT(db_id) as matched_with_db,
    ROUND(100.0 * COUNT(db_id) / COUNT(*), 1) AS match_pct
FROM iceberg.gold.users_enriched;
```

### Metadata & Time-travel

```sql
-- Xem danh sách tables
SHOW TABLES FROM iceberg.bronze;
SHOW TABLES FROM iceberg.silver;
SHOW TABLES FROM iceberg.gold;

-- Schema của table
DESCRIBE iceberg.bronze.api_users_raw;

-- Lịch sử snapshots
SELECT snapshot_id, committed_at, operation, summary
FROM iceberg.bronze."api_users_raw$snapshots"
ORDER BY committed_at DESC;

-- Time-travel: query dữ liệu tại thời điểm cụ thể
SELECT COUNT(*)
FROM iceberg.bronze.api_users_raw
FOR TIMESTAMP AS OF TIMESTAMP '2026-06-29 10:00:00 UTC';

-- Time-travel theo snapshot ID
SELECT COUNT(*)
FROM iceberg.bronze.api_users_raw
FOR VERSION AS OF 1234567890;

-- Files trong table
SELECT file_path, record_count, file_size_in_bytes
FROM iceberg.bronze."api_users_raw$files"
ORDER BY file_size_in_bytes DESC;

-- Partitions
SELECT partition, record_count
FROM iceberg.bronze."api_users_raw$partitions";
```

## Lưu ý

- Trino đọc Iceberg **read-only** — không hỗ trợ MERGE INTO hay DELETE trên Iceberg (dùng Spark cho transform)
- Query Bronze sẽ chỉ thấy data đã được Flink commit (sau checkpoint)
- Time-travel chỉ hoạt động trên dữ liệu còn trong MinIO (chưa bị expire/delete)
