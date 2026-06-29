{{
  config(
    materialized = 'table',
    file_format  = 'iceberg'
  )
}}

-- Gold: profile đầy đủ của user — join API + CDC theo email
-- Đây là bảng phục vụ BI tool / dashboard
SELECT
    a.username,
    a.full_name,
    a.gender,
    a.email,
    a.birth_year,
    a.phone,
    a.postcode,
    c.id            AS db_id,
    c.department,
    c.source_ts     AS last_db_update,
    a.ingested_at   AS api_ingested_at
FROM {{ ref('api_users') }} a
LEFT JOIN {{ ref('cdc_users') }} c
    ON LOWER(a.email) = LOWER(c.email)
