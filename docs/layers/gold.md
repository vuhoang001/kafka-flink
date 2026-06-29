# Gold Layer

## Mục đích

Gold là lớp **business-ready data** — join và aggregate từ Silver, sẵn sàng cho BI tool, dashboard, báo cáo. Rebuild hoàn toàn mỗi lần chạy.

Nguyên tắc:
- **Full rebuild**: DROP + recreate mỗi lần → kết quả luôn nhất quán
- **Denormalized**: join sẵn để query nhanh, không cần join khi đọc
- **Aggregated**: tính sẵn các metrics business

## Tables

### `iceberg.gold.users_enriched`

Kết hợp thông tin user từ 2 nguồn: FastAPI (api_users) và PostgreSQL CDC (cdc_users).

JOIN condition: `LOWER(api.email) = LOWER(cdc.email)`

| Column | Type | Nguồn | Mô tả |
|--------|------|-------|-------|
| username | STRING | Silver api | username đã lowercase |
| full_name | STRING | Silver api | first + last name |
| gender | STRING | Silver api | lowercase |
| email | STRING | Silver api | lowercase |
| birth_year | INT | Silver api | năm sinh |
| phone | STRING | Silver api | |
| postcode | STRING | Silver api | |
| db_id | INT | Silver cdc | id trong PostgreSQL, NULL nếu không match |
| department | STRING | Silver cdc | NULL nếu không match |
| last_db_update | TIMESTAMP | Silver cdc | source_ts của event CDC cuối |
| api_ingested_at | TIMESTAMP | Silver api | thời điểm nhận qua API |

> Dùng LEFT JOIN → user từ API không có trong DB vẫn xuất hiện, `db_id` và `department` sẽ là NULL.

### `iceberg.gold.user_stats`

Aggregation theo nhóm demography.

| Column | Type | Mô tả |
|--------|------|-------|
| gender | STRING | "male", "female", hoặc "unknown" nếu NULL |
| department | STRING | từ cdc_users, "unknown" nếu NULL |
| birth_year | INT | |
| total_users | LONG | số users trong nhóm |
| unique_emails | LONG | số email unique |
| matched_db_users | LONG | số users có `db_id != NULL` (khớp với DB) |
| first_seen | TIMESTAMP | api_ingested_at nhỏ nhất trong nhóm |
| last_seen | TIMESTAMP | api_ingested_at lớn nhất trong nhóm |

## Logic xử lý

### Build users_enriched

```python
api = spark.table("iceberg.silver.api_users")
cdc = spark.table("iceberg.silver.cdc_users")

enriched = api.alias("a").join(
    cdc.alias("c"),
    on=lower(col("a.email")) == lower(col("c.email")),
    how="left"
).select(...)

spark.sql("DROP TABLE IF EXISTS iceberg.gold.users_enriched")
enriched.writeTo("iceberg.gold.users_enriched").using("iceberg").create()
```

### Build user_stats

```python
stats = enriched.groupBy(
    coalesce("gender", lit("unknown")).alias("gender"),
    coalesce("department", lit("unknown")).alias("department"),
    "birth_year"
).agg(
    count("*").alias("total_users"),
    countDistinct("email").alias("unique_emails"),
    count("db_id").alias("matched_db_users"),
    min("api_ingested_at").alias("first_seen"),
    max("api_ingested_at").alias("last_seen"),
)
```

## Chạy Spark Gold

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/spark_session.py \
  /opt/spark/jobs/gold_transform.py
```

> **Lưu ý**: Phải chạy Silver trước Gold. Gold đọc từ Silver tables.

## Query Gold (Trino)

```bash
docker exec -it trino trino
```

```sql
-- Users enriched: thông tin đầy đủ
SELECT
    username,
    full_name,
    email,
    gender,
    birth_year,
    department,
    db_id IS NOT NULL AS has_db_record
FROM iceberg.gold.users_enriched
ORDER BY username;

-- Users không match với DB (chỉ có trong API)
SELECT username, email
FROM iceberg.gold.users_enriched
WHERE db_id IS NULL;

-- Users match với DB
SELECT username, email, department, last_db_update
FROM iceberg.gold.users_enriched
WHERE db_id IS NOT NULL
ORDER BY last_db_update DESC;

-- Stats tổng hợp
SELECT gender, department, total_users, matched_db_users
FROM iceberg.gold.user_stats
ORDER BY total_users DESC;

-- Tỷ lệ users có trong DB theo department
SELECT
    department,
    SUM(total_users) as total,
    SUM(matched_db_users) as in_db,
    ROUND(100.0 * SUM(matched_db_users) / SUM(total_users), 1) AS match_pct
FROM iceberg.gold.user_stats
GROUP BY department
ORDER BY total DESC;
```
