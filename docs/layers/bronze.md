# Bronze Layer

## Mục đích

Bronze là lớp **raw data** — lưu nguyên xi dữ liệu từ Kafka vào Iceberg, không transform. Nguyên tắc chính:

- **Append-only**: không bao giờ sửa hay xóa record
- **Immutable**: giữ nguyên mọi trường từ nguồn
- **Auditable**: có `ingested_at` để biết data đến lúc nào
- **Replayable**: nếu Silver/Gold bị hỏng, Bronze là nguồn để rebuild

---

## Tables

### `iceberg.bronze.api_users_raw`

Chứa raw data từ API (FastAPI POST /users hoặc Airflow DAG pull randomuser.me).

| Column       | Type         | Mô tả |
|--------------|--------------|-------|
| `first_name` | STRING       | Tên (raw, chưa trim) |
| `last_name`  | STRING       | Họ (raw) |
| `gender`     | STRING       | Giới tính (raw: "male", "female") |
| `postcode`   | STRING       | Mã bưu điện |
| `email`      | STRING       | Email (raw, chưa lowercase) |
| `username`   | STRING       | Tên đăng nhập |
| `dob`        | STRING       | Ngày sinh dạng ISO 8601 string |
| `phone`      | STRING       | Số điện thoại |
| `picture`    | STRING       | URL ảnh đại diện |
| `ingested_at`| TIMESTAMP(3) | Thời điểm Flink ghi vào Bronze |

### `iceberg.bronze.cdc_users_raw`

Chứa tất cả CDC events từ PostgreSQL qua Debezium — giữ cả `op` để trace lịch sử.

| Column        | Type         | Mô tả |
|---------------|--------------|-------|
| `id`          | INT          | Primary key từ PostgreSQL |
| `name`        | STRING       | Tên user (raw) |
| `email`       | STRING       | Email (raw) |
| `department`  | STRING       | Phòng ban |
| `op`          | STRING       | Operation: `c`=create, `u`=update, `d`=delete, `r`=read/snapshot |
| `source_ts_ms`| BIGINT       | Timestamp transaction trong PostgreSQL (milliseconds) |
| `ingested_at` | TIMESTAMP(3) | Thời điểm Flink ghi vào Bronze |

---

## Cách Bronze được ghi

Flink job (`processing/flink/user_processor.py`) chạy 2 pipeline:

```
Kafka [users_created]          → iceberg.bronze.api_users_raw
Kafka [postgres.public.users]  → iceberg.bronze.cdc_users_raw
```

Data được flush xuống MinIO mỗi **30 giây** (Flink checkpoint interval).

---

## Query operational data từ Bronze

```sql
-- Tất cả records mới nhất (real-time nhất có thể)
SELECT * FROM iceberg.bronze.api_users_raw
ORDER BY ingested_at DESC
LIMIT 20;

-- Theo dõi ingestion rate
SELECT
    DATE_TRUNC('minute', ingested_at) AS minute,
    COUNT(*) AS records
FROM iceberg.bronze.api_users_raw
WHERE ingested_at > (CURRENT_TIMESTAMP - INTERVAL '1' HOUR)
GROUP BY 1
ORDER BY 1;

-- Xem lịch sử thay đổi của 1 user cụ thể (CDC)
SELECT op, name, department, source_ts_ms, ingested_at
FROM iceberg.bronze.cdc_users_raw
WHERE id = 1
ORDER BY source_ts_ms;

-- Time travel: xem data tại snapshot thứ 2
SELECT * FROM iceberg.bronze.api_users_raw
FOR VERSION AS OF 2;

-- Xem tất cả snapshots hiện có
SELECT * FROM iceberg.bronze."api_users_raw$snapshots";
```

---

## Lưu ý

- Bronze **không filter op** — cả INSERT lẫn DELETE đều được lưu vào `cdc_users_raw`
- Raw data có thể có duplicate (Kafka retry, at-least-once delivery) → Silver mới deduplicate
- File format: **Parquet** (nén tốt, columnar, Trino đọc nhanh)
- Iceberg format-version 2 → hỗ trợ row-level delete (dùng cho Silver upsert)
