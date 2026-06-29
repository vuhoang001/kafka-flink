# Silver Layer

## Mục đích

Silver là lớp **clean data** — lấy raw Bronze, làm sạch và deduplicate. Nguyên tắc:

- **Normalized**: email lowercase, text trimmed
- **Deduplicated**: mỗi `username` / `id` chỉ có 1 record (latest)
- **Validated**: loại bỏ NULL ở các field quan trọng
- **Incremental**: chỉ xử lý data mới từ Bronze — không scan lại toàn bộ

---

## Tables

### `iceberg.silver.api_users`

Data API đã clean, một record per `username`.

| Column       | Type    | Transform từ Bronze |
|--------------|---------|---------------------|
| `first_name` | STRING  | `TRIM(first_name)` |
| `last_name`  | STRING  | `TRIM(last_name)` |
| `full_name`  | STRING  | `CONCAT(first_name, ' ', last_name)` |
| `gender`     | STRING  | `LOWER(TRIM(gender))` → "male"/"female" |
| `email`      | STRING  | `LOWER(TRIM(email))` |
| `username`   | STRING  | `LOWER(TRIM(username))` — **unique key** |
| `birth_year` | INT     | `TRY_CAST(SUBSTRING(dob,1,4) AS INT)` |
| `phone`      | STRING  | `TRIM(phone)` |
| `postcode`   | STRING  | Giữ nguyên |
| `ingested_at`| TIMESTAMP | Từ Bronze — dùng để incremental |

### `iceberg.silver.cdc_users`

Trạng thái mới nhất của mỗi user trong PostgreSQL.

| Column      | Type      | Transform từ Bronze |
|-------------|-----------|---------------------|
| `id`        | INT       | **unique key** |
| `name`      | STRING    | `TRIM(name)` |
| `email`     | STRING    | `LOWER(TRIM(email))` |
| `department`| STRING    | `TRIM(department)` |
| `op`        | STRING    | Giữ nguyên (c/u/r) |
| `source_ts` | TIMESTAMP | Từ `source_ts_ms` |
| `ingested_at`| TIMESTAMP | Từ Bronze |

---

## Cơ chế incremental

Silver dùng dbt incremental strategy `merge`:

```sql
-- Mỗi lần dbt chạy, chỉ load rows MỚI HƠN max(ingested_at) trong Silver
WHERE ingested_at > (
  SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1970-01-01 00:00:00')
  FROM this   -- this = bảng Silver hiện tại
)
```

Khi merge theo `unique_key = 'username'`:
- Nếu username chưa có → INSERT mới
- Nếu username đã có → UPDATE (giữ record mới nhất)

Kết quả: Silver luôn chứa trạng thái mới nhất của mỗi user, không có duplicate.

---

## Lịch chạy

Airflow DAG `dbt_medallion_pipeline` chạy Silver mỗi **15 phút**:

```
t=0:00  dbt run --select silver.*
t=0:15  dbt run --select silver.*
t=0:30  dbt run --select silver.*
...
```

---

## Query Silver

```sql
-- Users đã được clean
SELECT * FROM iceberg.silver.api_users LIMIT 10;

-- Kiểm tra không có duplicate
SELECT username, COUNT(*) AS cnt
FROM iceberg.silver.api_users
GROUP BY username
HAVING cnt > 1;

-- Trạng thái mới nhất của users trong DB
SELECT * FROM iceberg.silver.cdc_users
ORDER BY ingested_at DESC LIMIT 10;

-- Users có trong API nhưng không có trong DB
SELECT a.username, a.email
FROM iceberg.silver.api_users a
LEFT JOIN iceberg.silver.cdc_users c ON LOWER(a.email) = LOWER(c.email)
WHERE c.id IS NULL;
```

---

## Sự khác biệt Bronze vs Silver

| Vấn đề           | Bronze                         | Silver                          |
|------------------|--------------------------------|---------------------------------|
| "Nguyen Van A "  | Giữ nguyên (có khoảng trắng)  | Đã `TRIM` → "Nguyen Van A"     |
| "MALE" vs "male" | Cả hai tồn tại                | Chuẩn hóa → "male"             |
| Duplicate username| Nhiều rows cùng username      | Chỉ 1 row (latest)             |
| `op='d'` (delete)| Có trong Bronze                | Bị loại → Silver không có      |
| `dob` dạng string| "1995-01-01T00:00:00Z"         | `birth_year = 1995` (INT)      |
