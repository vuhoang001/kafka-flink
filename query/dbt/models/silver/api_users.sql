{{
  config(
    materialized        = 'incremental',
    incremental_strategy = 'merge',
    unique_key          = 'username',
    file_format         = 'iceberg',
    on_schema_change    = 'sync_all_columns'
  )
}}

-- Silver: làm sạch và chuẩn hóa data API
-- Chỉ load rows mới hơn max(ingested_at) hiện tại → incremental
SELECT
    TRIM(first_name)                        AS first_name,
    TRIM(last_name)                         AS last_name,
    CONCAT(TRIM(first_name), ' ', TRIM(last_name)) AS full_name,
    LOWER(TRIM(gender))                     AS gender,
    LOWER(TRIM(email))                      AS email,
    LOWER(TRIM(username))                   AS username,
    TRY_CAST(SUBSTRING(dob, 1, 4) AS INT)   AS birth_year,
    TRIM(phone)                             AS phone,
    postcode,
    ingested_at
FROM {{ source('bronze', 'api_users_raw') }}
WHERE username IS NOT NULL
  AND email    IS NOT NULL

{% if is_incremental() %}
  AND ingested_at > (
    SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1970-01-01 00:00:00')
    FROM {{ this }}
  )
{% endif %}
