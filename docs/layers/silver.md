# Silver Layer

## Mục đích

Silver là lớp **clean data** — lấy raw từ Bronze, làm sạch, deduplicate, và upsert vào Iceberg table. Chạy batch mỗi 15 phút qua Spark.

Nguyên tắc:
- **Idempotent**: chạy đi chạy lại nhiều lần vẫn cho kết quả giống nhau
- **Incremental**: chỉ xử lý records mới hơn watermark (MAX ingested_at) trong Silver
- **No DELETE**: CDC delete events bị lọc, Silver chỉ giữ trạng thái cuối cùng của các records tồn tại
- **Upsert by unique key**: `username` cho api_users, `id` cho cdc_users

## Tables

### `iceberg.silver.api_users`

| Column | Type | Transform từ Bronze |
|--------|------|---------------------|
| first_name | STRING | `TRIM(first_name)` |
| last_name | STRING | `TRIM(last_name)` |
| full_name | STRING | `CONCAT_WS(' ', first_name, last_name)` |
| gender | STRING | `LOWER(TRIM(gender))` |
| email | STRING | `LOWER(TRIM(email))` |
| username | STRING | `LOWER(TRIM(username))` — **unique key** |
| birth_year | INT | `SUBSTRING(dob, 1, 4).cast(Integer)` |
| phone | STRING | `TRIM(phone)` |
| postcode | STRING | giữ nguyên |
| ingested_at | TIMESTAMP | giữ nguyên từ Bronze |

### `iceberg.silver.cdc_users`

| Column | Type | Transform từ Bronze |
|--------|------|---------------------|
| id | INT | giữ nguyên — **unique key** |
| name | STRING | `TRIM(name)` |
| email | STRING | `LOWER(TRIM(email))` |
| department | STRING | `TRIM(department)` |
| op | STRING | giữ nguyên (`c`, `u`, `r` — bỏ `d`) |
| source_ts | TIMESTAMP | `source_ts_ms / 1000` cast sang Timestamp |
| ingested_at | TIMESTAMP | giữ nguyên từ Bronze |

## Logic xử lý

### Incremental load

```python
# Lấy watermark từ Silver
watermark = spark.table("iceberg.silver.api_users") \
    .agg(MAX("ingested_at")).collect()[0][0]

# Chỉ lấy Bronze records mới hơn watermark
bronze = bronze.filter(col("ingested_at") > watermark)
```

Lần đầu chạy (Silver trống): `watermark = None` → lấy toàn bộ Bronze.

### Dedup trong batch

```python
# Nếu cùng username xuất hiện nhiều lần trong batch, chỉ giữ record mới nhất
.withColumn("_rank",
    row_number().over(
        Window.partitionBy("username").orderBy(col("ingested_at").desc())
    )
)
.filter(col("_rank") == 1)
```

### MERGE INTO (upsert)

```sql
MERGE INTO iceberg.silver.api_users t
USING silver_api_updates s ON t.username = s.username
WHEN MATCHED     THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

## Chạy Spark Silver

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/spark_session.py \
  /opt/spark/jobs/silver_transform.py
```

## Query Silver (Trino)

```bash
docker exec -it trino trino
```

```sql
-- API users đã normalize
SELECT username, full_name, email, gender, birth_year
FROM iceberg.silver.api_users
ORDER BY username;

-- CDC users (không có DELETE)
SELECT id, name, email, department, op
FROM iceberg.silver.cdc_users
ORDER BY id;

-- Verify không có duplicate
SELECT username, COUNT(*) as cnt
FROM iceberg.silver.api_users
GROUP BY username
HAVING COUNT(*) > 1;
-- Phải trả về 0 rows

-- So sánh Bronze vs Silver
SELECT
  (SELECT COUNT(*) FROM iceberg.bronze.api_users_raw) as bronze_count,
  (SELECT COUNT(*) FROM iceberg.silver.api_users)     as silver_count;
-- Bronze >= Silver (Bronze append-only, Silver deduplicated)
```

## Tần suất chạy

Airflow DAG `spark_medallion_pipeline` chạy Silver job mỗi **15 phút**. Gold chạy ngay sau khi Silver hoàn thành.

Có thể chạy thủ công bất cứ lúc nào — Spark Silver là idempotent.
