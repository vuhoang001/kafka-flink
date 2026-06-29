# dbt (data build tool)

## Vai trò

dbt là **transformation layer** — chạy SQL trên Trino để transform Bronze → Silver → Gold. Không lưu data, không phải database, không thay thế Iceberg hay Trino.

```
Iceberg Bronze (raw)
       │
       │  dbt chạy SQL qua Trino
       ▼
Iceberg Silver (clean)
       │
       │  dbt chạy SQL qua Trino
       ▼
Iceberg Gold (business-ready)
```

---

## Cài đặt

```bash
pip install dbt-trino
```

---

## Cấu trúc thư mục

```
query/dbt/
├── dbt_project.yml        # Config project (tên, profile, model settings)
├── profiles.yml           # Kết nối tới Trino
└── models/
    ├── sources.yml        # Khai báo Bronze tables là "source" (không do dbt tạo)
    ├── bronze/            # View thẳng vào Bronze — không copy data
    │   ├── api_users_raw.sql
    │   └── cdc_users_raw.sql
    ├── silver/            # Incremental merge — clean + dedupe
    │   ├── api_users.sql
    │   └── cdc_users.sql
    └── gold/              # Full table rebuild — join + aggregate
        ├── users_enriched.sql
        └── user_stats.sql
```

---

## Chạy dbt

```bash
cd query/dbt

# Kiểm tra kết nối Trino
dbt debug --profiles-dir .

# Chạy toàn bộ (Bronze view + Silver + Gold)
dbt run --profiles-dir .

# Chỉ chạy Silver (khi chỉ muốn update Silver)
dbt run --profiles-dir . --select silver.*

# Chạy Gold sau Silver
dbt run --profiles-dir . --select gold.*

# Chạy 1 model cụ thể
dbt run --profiles-dir . --select users_enriched

# Full refresh Silver (bỏ qua incremental, rebuild từ đầu)
dbt run --profiles-dir . --select silver.* --full-refresh

# Xem lineage (dependency graph)
dbt ls --profiles-dir . --select +gold.users_enriched
```

---

## Materialization theo layer

### Bronze — `view`

```sql
-- Bronze models chỉ là view, không copy data
{{ config(materialized='view') }}
SELECT * FROM {{ source('bronze', 'api_users_raw') }}
```

Ưu điểm: luôn thấy data mới nhất mà không tốn storage.

### Silver — `incremental` (merge)

```sql
{{ config(
    materialized        = 'incremental',
    incremental_strategy = 'merge',
    unique_key          = 'username',
    file_format         = 'iceberg'
) }}

SELECT ... FROM {{ source('bronze', 'api_users_raw') }}

{% if is_incremental() %}
  -- Chỉ load rows mới hơn max ingested_at trong Silver
  AND ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
{% endif %}
```

Lần đầu chạy (`is_incremental() = false`): load toàn bộ Bronze.  
Lần sau (`is_incremental() = true`): chỉ load rows có `ingested_at` mới hơn.

Khi merge theo `unique_key = 'username'`:
- Chưa có → INSERT
- Đã có → UPDATE (overwrite với data mới nhất)

### Gold — `table`

```sql
{{ config(materialized='table', file_format='iceberg') }}

SELECT ... FROM {{ ref('api_users') }}
LEFT JOIN {{ ref('cdc_users') }} ON ...
```

Mỗi lần dbt chạy: DROP + CREATE lại từ Silver. Đảm bảo chính xác khi Silver thay đổi.

---

## ref() và source()

| Function | Dùng khi | Ví dụ |
|----------|---------|-------|
| `{{ source('bronze', 'api_users_raw') }}` | Trỏ đến bảng không do dbt tạo (Bronze Iceberg) | Khai báo trong `sources.yml` |
| `{{ ref('api_users') }}` | Trỏ đến model dbt khác (Silver, Gold) | dbt tự resolve thứ tự chạy |

---

## Lịch chạy tự động

Airflow DAG `dbt_medallion_pipeline` (`ingestion/dags/dbt_pipeline.py`):

```
[Mỗi 15 phút]
      │
      ▼
dbt_run_silver   (dbt run --select silver.*)
      │
      ▼ (sau khi silver xong)
dbt_run_gold     (dbt run --select gold.*)
```

---

## Troubleshooting

### Xem SQL Trino sẽ chạy

```bash
dbt compile --profiles-dir . --select users_enriched
# SQL compiled nằm ở target/compiled/...
cat target/compiled/realtime_streaming/models/gold/users_enriched.sql
```

### Reset Silver (khi cần rebuild từ đầu)

```bash
# Full refresh: bỏ qua incremental logic, load lại toàn bộ từ Bronze
dbt run --profiles-dir . --select silver.* --full-refresh
```

### Xem dbt docs

```bash
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
# Mở http://localhost:8080 → xem lineage graph
```
