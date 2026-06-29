# Trino

## Vai trò

Trino là **distributed SQL query engine** — đọc Iceberg tables từ MinIO qua Nessie catalog và cho phép query bằng ANSI SQL chuẩn. Không lưu data, chỉ đọc.

---

## Kết nối

```bash
# CLI trong Docker
docker exec -it trino trino

# Chỉ định catalog và schema
docker exec -it trino trino --catalog iceberg --schema bronze
```

---

## Catalog và Schema

Trino dùng hệ thống 3 cấp: `catalog.schema.table`

| Catalog | Schema  | Tables |
|---------|---------|--------|
| `iceberg` | `bronze` | `api_users_raw`, `cdc_users_raw` |
| `iceberg` | `silver` | `api_users`, `cdc_users` |
| `iceberg` | `gold`   | `users_enriched`, `user_stats` |

```sql
-- Xem tất cả schemas
SHOW SCHEMAS IN iceberg;

-- Xem tất cả tables trong bronze
SHOW TABLES IN iceberg.bronze;

-- Xem schema của bảng
DESCRIBE iceberg.bronze.api_users_raw;
SHOW COLUMNS FROM iceberg.gold.users_enriched;
```

---

## Query mẫu theo từng nhu cầu

### Operational data (Bronze — gần real-time)

```sql
-- Data mới nhất
SELECT * FROM iceberg.bronze.api_users_raw
ORDER BY ingested_at DESC LIMIT 20;

-- Monitoring: records per minute
SELECT DATE_TRUNC('minute', ingested_at) AS minute,
       COUNT(*) AS cnt
FROM iceberg.bronze.api_users_raw
WHERE ingested_at > (CURRENT_TIMESTAMP - INTERVAL '1' HOUR)
GROUP BY 1 ORDER BY 1;

-- Phát hiện data quality issues trong Bronze
SELECT
    COUNT(*) AS total,
    COUNT(email) AS has_email,
    COUNT(CASE WHEN email LIKE '%@%' THEN 1 END) AS valid_email
FROM iceberg.bronze.api_users_raw;
```

### Clean data (Silver)

```sql
-- Users đã clean
SELECT * FROM iceberg.silver.api_users LIMIT 10;

-- Xem CDC history của 1 user
SELECT op, name, department, source_ts, ingested_at
FROM iceberg.silver.cdc_users
WHERE id = 1;
```

### Business results (Gold)

```sql
-- Full profile
SELECT * FROM iceberg.gold.users_enriched LIMIT 10;

-- Users chưa match với DB
SELECT username, email FROM iceberg.gold.users_enriched
WHERE db_id IS NULL;

-- Dashboard metrics
SELECT gender, department,
       SUM(total_users) AS total,
       SUM(matched_db_users) AS in_db
FROM iceberg.gold.user_stats
GROUP BY gender, department
ORDER BY total DESC;
```

### Cross-layer queries (so sánh Bronze vs Silver)

```sql
-- Bao nhiêu records Bronze chưa vào Silver (lag)
SELECT
    (SELECT COUNT(*) FROM iceberg.bronze.api_users_raw) AS bronze_total,
    (SELECT COUNT(*) FROM iceberg.silver.api_users)     AS silver_total;

-- Data trong Bronze nhưng chưa có trong Silver
SELECT b.username, b.ingested_at
FROM iceberg.bronze.api_users_raw b
LEFT JOIN iceberg.silver.api_users s ON b.username = s.username
WHERE s.username IS NULL
ORDER BY b.ingested_at DESC LIMIT 10;
```

---

## Tính năng Iceberg qua Trino

### Time Travel

```sql
-- Xem data tại snapshot thứ N
SELECT * FROM iceberg.bronze.api_users_raw FOR VERSION AS OF 3;

-- Xem data tại thời điểm cụ thể
SELECT * FROM iceberg.bronze.api_users_raw
FOR TIMESTAMP AS OF TIMESTAMP '2026-06-29 10:00:00';
```

### Metadata tables

```sql
-- Xem tất cả snapshots của bảng
SELECT * FROM iceberg.bronze."api_users_raw$snapshots";

-- Xem danh sách files Parquet
SELECT * FROM iceberg.bronze."api_users_raw$files";

-- Xem lịch sử thay đổi manifest
SELECT * FROM iceberg.bronze."api_users_raw$manifests";
```

---

## Web UI

URL: http://localhost:8080

Từ UI có thể thấy:
- Query đang chạy và đã hoàn thành
- Query plan (execution graph)
- Resource usage (CPU, memory, I/O)
- Worker nodes

---

## Cấu hình (`query/trino/etc/`)

| File | Mô tả |
|------|-------|
| `config.properties` | Coordinator settings, port, memory limits |
| `node.properties` | Node ID và data directory |
| `jvm.config` | JVM heap size (2GB mặc định) |
| `catalog/iceberg.properties` | Kết nối Nessie + MinIO |

### Tăng memory cho query lớn

Sửa `jvm.config`:
```
-Xmx4G   # tăng từ 2G lên 4G
```

Sửa `config.properties`:
```
query.max-memory=4GB
query.max-memory-per-node=2GB
```
