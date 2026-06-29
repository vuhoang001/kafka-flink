{{ config(materialized='view') }}

-- View trực tiếp vào Bronze CDC table
-- Giữ nguyên op field để có thể trace INSERT/UPDATE/DELETE
SELECT * FROM {{ source('bronze', 'cdc_users_raw') }}
