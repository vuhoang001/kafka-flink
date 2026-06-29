# Bronze Layer

## Mục đích

Bronze là lớp **raw data** — lưu nguyên xi dữ liệu từ Kafka vào Iceberg Parquet, không transform. Chỉ thêm `ingested_at` để trace thời điểm nhận được.

Nguyên tắc:
- **Append-only**: không UPDATE, không DELETE
- **Schema fidelity**: giữ đúng cấu trúc gốc từ producer
- **Replay-safe**: nếu Flink crash, khởi động lại từ checkpoint — Bronze có thể có duplicate, tầng Silver xử lý dedup

## Tables

### `iceberg.bronze.api_users_raw`

Dữ liệu từ FastAPI → Kafka topic `users_created` → Flink → Bronze.

| Column | Type | Nguồn | Mô tả |
|--------|------|-------|-------|
| first_name | STRING | API request | |
| last_name | STRING | API request | |
| gender | STRING | API request | "male" / "female" |
| postcode | STRING | API request | |
| email | STRING | API request | |
| username | STRING | API request | |
| dob | STRING | API request | Chuỗi ngày, ví dụ "1995-01-15" |
| phone | STRING | API request | |
| picture | STRING | API request | URL ảnh, có thể rỗng |
| ingested_at | TIMESTAMP(3) | Flink `CURRENT_TIMESTAMP` | Thời điểm Flink nhận được |

### `iceberg.bronze.cdc_users_raw`

Dữ liệu từ PostgreSQL → Debezium → Kafka topic `postgres.public.users` → Flink → Bronze.

| Column | Type | Nguồn | Mô tả |
|--------|------|-------|-------|
| id | INT | `after.id` | Primary key của bảng PostgreSQL |
| name | STRING | `after.name` | |
| email | STRING | `after.email` | |
| department | STRING | `after.department` | |
| op | STRING | Debezium field | `c`=create, `u`=update, `d`=delete, `r`=read/snapshot |
| source_ts_ms | BIGINT | Debezium `ts_ms` | Epoch ms khi event xảy ra trong DB |
| ingested_at | TIMESTAMP(3) | Flink `CURRENT_TIMESTAMP` | Thời điểm Flink nhận được |

## Storage

- **Format**: Apache Iceberg (format-version 2) + Parquet
- **Location**: `s3://warehouse/bronze/` (MinIO)
- **Commit cycle**: sau mỗi Flink checkpoint (30 giây)
- **Visibility**: data visible sau khi Flink commit Parquet file hoàn chỉnh

## Query Bronze (Trino)

```bash
docker exec -it trino trino
```

```sql
-- API users mới nhất
SELECT username, email, gender, ingested_at
FROM iceberg.bronze.api_users_raw
ORDER BY ingested_at DESC
LIMIT 20;

-- CDC events theo thứ tự
SELECT id, name, op, department, ingested_at
FROM iceberg.bronze.cdc_users_raw
ORDER BY ingested_at DESC
LIMIT 20;

-- Đếm records
SELECT COUNT(*) FROM iceberg.bronze.api_users_raw;
SELECT COUNT(*) FROM iceberg.bronze.cdc_users_raw;

-- Phân phối theo op (CDC)
SELECT op, COUNT(*) as cnt
FROM iceberg.bronze.cdc_users_raw
GROUP BY op;
-- r: initial snapshot, c: insert, u: update, d: delete

-- Xem metadata Iceberg
SELECT snapshot_id, committed_at, operation
FROM iceberg.bronze."api_users_raw$snapshots"
ORDER BY committed_at DESC;
```

## Latency

Thời gian từ lúc POST API đến lúc Bronze có thể query được:

```
POST request → Kafka (< 100ms) → Flink nhận (< 1s) → checkpoint (0-30s) → commit → visible
```

Tổng: **10–60 giây** (trung bình ~30s tùy thời điểm checkpoint).

Đây là đặc tính của Iceberg file-based format, không phải hạn chế của Flink hay Kafka.
