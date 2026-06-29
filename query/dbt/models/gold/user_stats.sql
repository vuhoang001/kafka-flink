{{
  config(
    materialized = 'table',
    file_format  = 'iceberg'
  )
}}

-- Gold: thống kê tổng hợp theo gender + department + birth_year
-- Dùng cho báo cáo, dashboard metrics
SELECT
    COALESCE(gender, 'unknown')         AS gender,
    COALESCE(department, 'unknown')     AS department,
    birth_year,
    COUNT(*)                            AS total_users,
    COUNT(DISTINCT email)               AS unique_emails,
    COUNT(DISTINCT db_id)               AS matched_db_users,
    MIN(api_ingested_at)                AS first_seen,
    MAX(api_ingested_at)                AS last_seen
FROM {{ ref('users_enriched') }}
GROUP BY gender, department, birth_year
ORDER BY total_users DESC
