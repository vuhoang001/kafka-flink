{{ config(materialized='view') }}

-- View trực tiếp vào Bronze table để tiện query operational data
-- Không transform gì cả — đây là điểm truy vấn "real-time nhất" (30s latency)
SELECT * FROM {{ source('bronze', 'api_users_raw') }}
