# Gold Layer

## Mục đích

Gold là lớp **business-ready data** — join, aggregate, sẵn sàng cho BI tool, dashboard, báo cáo. Nguyên tắc:

- **Denormalized**: join sẵn để query không cần JOIN phức tạp
- **Aggregated**: tính sẵn metrics thường dùng
- **Full rebuild**: mỗi lần dbt chạy là rebuild hoàn toàn từ Silver
- **Named by business**: tên bảng/column theo ngôn ngữ business, không kỹ thuật

---

## Tables

### `iceberg.gold.users_enriched`

Profile đầy đủ của user — kết hợp data từ API (Silver) và PostgreSQL CDC (Silver).

| Column          | Type      | Nguồn |
|-----------------|-----------|-------|
| `username`      | STRING    | Silver api_users |
| `full_name`     | STRING    | Silver api_users |
| `gender`        | STRING    | Silver api_users |
| `email`         | STRING    | Silver api_users |
| `birth_year`    | INT       | Silver api_users |
| `phone`         | STRING    | Silver api_users |
| `postcode`      | STRING    | Silver api_users |
| `db_id`         | INT       | Silver cdc_users (NULL nếu không match) |
| `department`    | STRING    | Silver cdc_users (NULL nếu không match) |
| `last_db_update`| TIMESTAMP | Silver cdc_users.source_ts |
| `api_ingested_at`| TIMESTAMP| Silver api_users.ingested_at |

**Điều kiện join**: `LOWER(api_users.email) = LOWER(cdc_users.email)`
**Loại join**: LEFT JOIN — giữ tất cả API users, kể cả không tìm thấy match trong DB

### `iceberg.gold.user_stats`

Thống kê tổng hợp theo gender + department + birth_year.

| Column           | Type      | Mô tả |
|------------------|-----------|-------|
| `gender`         | STRING    | "male" / "female" / "unknown" |
| `department`     | STRING    | Tên phòng ban / "unknown" |
| `birth_year`     | INT       | Năm sinh |
| `total_users`    | BIGINT    | Tổng số users |
| `unique_emails`  | BIGINT    | Số email duy nhất |
| `matched_db_users`| BIGINT  | Users có match trong PostgreSQL |
| `first_seen`     | TIMESTAMP | Ingestion timestamp sớm nhất |
| `last_seen`      | TIMESTAMP | Ingestion timestamp mới nhất |

---

## Cơ chế rebuild

Gold dùng `materialized='table'` — **không incremental**. Mỗi lần dbt chạy là DROP và CREATE lại từ đầu.

Lý do không dùng incremental cho Gold:
- Gold là aggregation từ Silver — cần tính lại toàn bộ khi Silver thay đổi
- Nếu dùng incremental trên aggregation, kết quả sẽ sai khi Silver có update

```
dbt run --select silver.*  →  Silver updated
dbt run --select gold.*    →  Gold completely rebuilt from Silver
```

---

## Lịch chạy

Airflow DAG `dbt_medallion_pipeline` chạy Gold **sau khi Silver xong**:

```python
silver_task >> gold_task   # Gold chỉ bắt đầu khi Silver hoàn thành
```

---

## Query Gold

```sql
-- Profile đầy đủ (dùng cho user detail page)
SELECT *
FROM iceberg.gold.users_enriched
WHERE email = 'a@example.com';

-- Users chưa có trong PostgreSQL (chỉ có từ API)
SELECT username, full_name, email
FROM iceberg.gold.users_enriched
WHERE db_id IS NULL;

-- Users có trong cả API lẫn DB (đã match)
SELECT username, department, birth_year
FROM iceberg.gold.users_enriched
WHERE db_id IS NOT NULL
ORDER BY birth_year;

-- Thống kê theo phòng ban
SELECT department, SUM(total_users) AS total
FROM iceberg.gold.user_stats
GROUP BY department
ORDER BY total DESC;

-- Tỉ lệ male/female
SELECT gender, total_users,
       ROUND(100.0 * total_users / SUM(total_users) OVER(), 1) AS pct
FROM iceberg.gold.user_stats
GROUP BY gender, total_users;

-- Users theo năm sinh
SELECT birth_year, SUM(total_users) AS cnt
FROM iceberg.gold.user_stats
WHERE birth_year IS NOT NULL
GROUP BY birth_year
ORDER BY birth_year;
```

---

## Dbt lineage

```
sources.yml (Bronze Iceberg tables)
    │
    ├─► silver.api_users  ──────────────────────┐
    │                                           ▼
    └─► silver.cdc_users  ──────────► gold.users_enriched ──► gold.user_stats
```

Chạy theo thứ tự này:
```bash
dbt run --select silver.api_users silver.cdc_users
dbt run --select gold.users_enriched
dbt run --select gold.user_stats
```

Hoặc dbt tự resolve thứ tự dựa trên `ref()`:
```bash
dbt run   # chạy toàn bộ theo đúng thứ tự
```
