{{
  config(
    materialized        = 'incremental',
    incremental_strategy = 'merge',
    unique_key          = 'id',
    file_format         = 'iceberg',
    on_schema_change    = 'sync_all_columns'
  )
}}

-- Silver: trạng thái mới nhất của mỗi user trong PostgreSQL
-- Chỉ giữ op = 'c' (insert) và 'u' (update) — bỏ qua 'd' (delete) và 'r' (snapshot)
-- Incremental: chỉ load CDC events mới hơn ingested_at hiện tại
SELECT
    id,
    TRIM(name)                 AS name,
    LOWER(TRIM(email))         AS email,
    TRIM(department)           AS department,
    op,
    TIMESTAMP_MILLIS(source_ts_ms) AS source_ts,
    ingested_at
FROM {{ source('bronze', 'cdc_users_raw') }}
WHERE id   IS NOT NULL
  AND name IS NOT NULL
  AND op IN ('c', 'u', 'r')

{% if is_incremental() %}
  AND ingested_at > (
    SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1970-01-01 00:00:00')
    FROM {{ this }}
  )
{% endif %}
