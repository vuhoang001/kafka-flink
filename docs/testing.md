# Test End-to-End

Hướng dẫn này chạy toàn bộ luồng từ đầu đến cuối và verify từng bước.

## Điều kiện tiên quyết

Tất cả services đã khởi động theo [docs/setup.md](setup.md). Kiểm tra nhanh:

```bash
docker compose ps
# Tất cả phải healthy hoặc Up (trừ connector-init và minio-init: exited 0)
```

---

## Bước 1 — Submit Flink job

Flink job đọc liên tục từ Kafka và ghi vào Bronze.

```bash
docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/user_processor.py
```

Kiểm tra job đang chạy:

```bash
docker exec flink-jobmanager flink list
```

Output mong đợi:
```
Waiting for response...
------------------ Running/Restarting Jobs -------------------
<timestamp> : <job-id> : user_processor (RUNNING)
```

Bạn cũng có thể vào http://localhost:18081 → thấy 1 job RUNNING với 2 tasks.

---

## Bước 2 — Gửi data qua API (luồng 1: API → Kafka → Flink → Bronze)

### Gửi 1 user

```bash
curl -X POST http://localhost:8000/users \
  -H "Content-Type: application/json" \
  -d '{
    "first_name": "Hoang",
    "last_name":  "Nguyen",
    "gender":     "male",
    "postcode":   "100000",
    "email":      "hoang@example.com",
    "username":   "hoangnv",
    "dob":        "1995-01-15",
    "phone":      "0901234567",
    "picture":    ""
  }'
```

Response:
```json
{"status": "ok", "message": "User hoangnv sent to Kafka"}
```

### Gửi nhiều users để có đủ data test

```bash
for i in 1 2 3 4 5; do
  curl -s -X POST http://localhost:8000/users \
    -H "Content-Type: application/json" \
    -d "{
      \"first_name\": \"User\",
      \"last_name\":  \"Test$i\",
      \"gender\":     \"female\",
      \"postcode\":   \"70000$i\",
      \"email\":      \"user$i@example.com\",
      \"username\":   \"user$i\",
      \"dob\":        \"199$i-06-15\",
      \"phone\":      \"090000000$i\"
    }"
  echo ""
done
```

### Verify message đã vào Kafka

```bash
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic users_created \
  --from-beginning \
  --max-messages 3
```

Output mong đợi:
```json
{"first_name":"Hoang","last_name":"Nguyen","gender":"male",...}
```

---

## Bước 3 — Trigger CDC từ PostgreSQL (luồng 2: DB → Debezium → Kafka → Flink → Bronze)

### Insert data vào PostgreSQL

```bash
docker exec postgres psql -U postgres -d mydb -c "
  INSERT INTO users (name, email, department) VALUES
    ('Hoang Nguyen', 'hoang@example.com', 'Engineering'),
    ('Alice Tran',   'alice@example.com', 'Marketing'),
    ('Bob Le',       'bob@example.com',   'Engineering'),
    ('Carol Pham',   'carol@example.com', 'Data'),
    ('David Vo',     'david@example.com', 'Data');
"
```

### Verify CDC event đã vào Kafka

```bash
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic postgres.public.users \
  --from-beginning \
  --max-messages 3
```

Output mong đợi (Debezium format):
```json
{"after":{"id":1,"name":"Alice Nguyen","email":"alice@example.com","department":"Engineering"},"op":"r","ts_ms":...}
```

(`"op":"r"` = read/snapshot từ initial snapshot khi connector đăng ký)

### Test UPDATE và DELETE

```bash
# Update
docker exec postgres psql -U postgres -d mydb -c "
  UPDATE users SET department = 'Platform' WHERE email = 'hoang@example.com';
"

# Kiểm tra CDC event UPDATE (op=u)
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic postgres.public.users \
  --max-messages 1
```

---

## Bước 4 — Đợi Flink commit và query Bronze

Flink checkpoint sau mỗi **30 giây**. Sau khi checkpoint, data mới visible trong Iceberg.

```bash
# Đợi 30-60 giây, sau đó query
docker exec -it trino trino
```

Trong Trino CLI:

```sql
-- Kiểm tra Bronze API users
SELECT username, email, gender, ingested_at
FROM iceberg.bronze.api_users_raw
ORDER BY ingested_at DESC
LIMIT 10;

-- Kiểm tra Bronze CDC users
SELECT id, name, email, department, op, ingested_at
FROM iceberg.bronze.cdc_users_raw
ORDER BY ingested_at DESC
LIMIT 10;

-- Số lượng records
SELECT COUNT(*) as total FROM iceberg.bronze.api_users_raw;
SELECT COUNT(*) as total FROM iceberg.bronze.cdc_users_raw;
```

Thoát Trino: `quit`

Nếu Bronze trống (query trả về 0 rows):
```bash
# Kiểm tra Flink có đang ghi không
docker logs flink-taskmanager --tail 30

# Kiểm tra checkpoint đã complete chưa
# Vào http://localhost:18081 → Jobs → <job> → Checkpoints
```

---

## Bước 5 — Chạy Spark Silver transform (Bronze → Silver)

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/spark_session.py \
  /opt/spark/jobs/silver_transform.py
```

Output mong đợi:
```
[api_users] Đã upsert 6 records vào Silver.
[cdc_users] Đã upsert 5 records vào Silver.
Silver transform hoàn thành.
```

Nếu Bronze chưa có data sẽ thấy:
```
[api_users] Không có data mới trong Bronze.
```

Query Silver để verify:

```bash
docker exec -it trino trino
```

```sql
-- Silver API users (đã normalize)
SELECT username, full_name, email, gender, birth_year
FROM iceberg.silver.api_users
ORDER BY ingested_at DESC;

-- Silver CDC users (đã dedup, không có DELETE)
SELECT id, name, email, department, op
FROM iceberg.silver.cdc_users
ORDER BY id;

-- Kiểm tra dedup: mỗi username chỉ xuất hiện 1 lần
SELECT username, COUNT(*) as cnt
FROM iceberg.silver.api_users
GROUP BY username
HAVING COUNT(*) > 1;
-- Phải trả về 0 rows nếu dedup đúng
```

---

## Bước 6 — Chạy Spark Gold transform (Silver → Gold)

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/spark_session.py \
  /opt/spark/jobs/gold_transform.py
```

Output mong đợi:
```
[users_enriched] Đã ghi 6 records vào Gold.
[user_stats] Đã ghi 3 records vào Gold.
Gold transform hoàn thành.
```

Query Gold để verify:

```bash
docker exec -it trino trino
```

```sql
-- Gold: users enriched (JOIN api + cdc)
SELECT
    username,
    full_name,
    email,
    gender,
    birth_year,
    department,    -- từ CDC, NULL nếu không match
    db_id          -- id trong PostgreSQL, NULL nếu không match
FROM iceberg.gold.users_enriched
ORDER BY username;

-- Gold: user stats (aggregation)
SELECT
    gender,
    department,
    birth_year,
    total_users,
    unique_emails,
    matched_db_users
FROM iceberg.gold.user_stats
ORDER BY total_users DESC;
```

---

## Bước 7 — Test incremental (gửi thêm data, chạy lại Spark)

### Gửi thêm user mới và update user cũ

```bash
# User mới
curl -X POST http://localhost:8000/users \
  -H "Content-Type: application/json" \
  -d '{
    "first_name": "New",
    "last_name":  "User",
    "gender":     "male",
    "postcode":   "200000",
    "email":      "newuser@example.com",
    "username":   "newuser",
    "dob":        "2000-03-20",
    "phone":      "0912345678"
  }'

# Update CDC
docker exec postgres psql -U postgres -d mydb -c "
  INSERT INTO users (name, email, department) VALUES ('New Person', 'newuser@example.com', 'Product');
"
```

### Đợi Flink commit (30s) rồi chạy lại Silver + Gold

```bash
sleep 35

docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/spark_session.py \
  /opt/spark/jobs/silver_transform.py

docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/spark_session.py \
  /opt/spark/jobs/gold_transform.py
```

### Verify incremental hoạt động đúng

```bash
docker exec -it trino trino
```

```sql
-- Silver chỉ tăng thêm 1 record mới (không duplicate)
SELECT COUNT(*) FROM iceberg.silver.api_users;  -- phải tăng thêm 1

-- Bronze có nhiều records hơn (append-only)
SELECT COUNT(*) FROM iceberg.bronze.api_users_raw;  -- nhiều hơn Silver

-- Gold users_enriched có user mới với department từ CDC
SELECT username, full_name, email, department
FROM iceberg.gold.users_enriched
WHERE username = 'newuser';
```

---

## Bước 8 — Kiểm tra MinIO (dữ liệu thực sự lưu trên S3)

Mở MinIO Console: http://localhost:9001 (minio / minio123)

Vào bucket `warehouse` → phải thấy cấu trúc:
```
warehouse/
├── bronze/
│   ├── api_users_raw/
│   │   ├── data/       ← các file .parquet
│   │   └── metadata/   ← Iceberg metadata JSON
│   └── cdc_users_raw/
├── silver/
│   ├── api_users/
│   └── cdc_users/
└── gold/
    ├── users_enriched/
    └── user_stats/
```

Hoặc dùng lệnh:

```bash
docker exec minio-init mc ls -r local/warehouse/ | head -30
```

---

## Bước 9 — Time-travel query (Iceberg feature)

```bash
docker exec -it trino trino
```

```sql
-- Xem lịch sử snapshots của Bronze table
SELECT snapshot_id, committed_at, operation, summary
FROM iceberg.bronze."api_users_raw$snapshots"
ORDER BY committed_at DESC;

-- Query dữ liệu tại thời điểm cụ thể (thay timestamp)
SELECT COUNT(*)
FROM iceberg.bronze.api_users_raw
FOR TIMESTAMP AS OF TIMESTAMP '2026-06-29 10:00:00 UTC';

-- Query theo snapshot ID
SELECT COUNT(*)
FROM iceberg.bronze.api_users_raw
FOR VERSION AS OF <snapshot_id>;
```

---

## Chạy toàn bộ luồng 1 lần (script)

```bash
#!/bin/bash
set -e

echo "=== 1. Submit Flink job ==="
docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/user_processor.py

echo "=== 2. Gửi data qua API ==="
for i in 1 2 3; do
  curl -s -X POST http://localhost:8000/users \
    -H "Content-Type: application/json" \
    -d "{\"first_name\":\"Test\",\"last_name\":\"User$i\",\"gender\":\"male\",
         \"postcode\":\"10000$i\",\"email\":\"test$i@example.com\",
         \"username\":\"testuser$i\",\"dob\":\"199$i-01-01\",\"phone\":\"090000000$i\"}"
  echo " → user$i sent"
done

echo "=== 3. Insert CDC data ==="
docker exec postgres psql -U postgres -d mydb -c "
  INSERT INTO users (name, email, department)
  VALUES ('Test User1','test1@example.com','Engineering'),
         ('Test User2','test2@example.com','Marketing'),
         ('Test User3','test3@example.com','Data')
  ON CONFLICT DO NOTHING;
"

echo "=== 4. Đợi Flink checkpoint (35s) ==="
sleep 35

echo "=== 5. Spark Silver ==="
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/spark_session.py \
  /opt/spark/jobs/silver_transform.py

echo "=== 6. Spark Gold ==="
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/spark_session.py \
  /opt/spark/jobs/gold_transform.py

echo "=== 7. Query kết quả ==="
docker exec -it trino trino --execute "
  SELECT 'bronze_api' as layer, COUNT(*) as cnt FROM iceberg.bronze.api_users_raw
  UNION ALL
  SELECT 'bronze_cdc',          COUNT(*)         FROM iceberg.bronze.cdc_users_raw
  UNION ALL
  SELECT 'silver_api',          COUNT(*)         FROM iceberg.silver.api_users
  UNION ALL
  SELECT 'silver_cdc',          COUNT(*)         FROM iceberg.silver.cdc_users
  UNION ALL
  SELECT 'gold_enriched',       COUNT(*)         FROM iceberg.gold.users_enriched
  UNION ALL
  SELECT 'gold_stats',          COUNT(*)         FROM iceberg.gold.user_stats;
"

echo "=== DONE ==="
```

---

## Troubleshooting

| Vấn đề | Kiểm tra | Fix |
|--------|----------|-----|
| Bronze trống sau 30s | `docker logs flink-taskmanager --tail 50` | Xem lỗi Iceberg/S3; kiểm tra Nessie healthy |
| Flink job không start | `docker logs flink-jobmanager --tail 50` | Check Nessie và MinIO healthy trước |
| Spark lỗi connection refused | `docker logs spark-master --tail 30` | `docker compose restart spark-worker` |
| Nessie unhealthy | `docker logs nessie --tail 30` | Tạo `nessiedb`, thêm `QUARKUS_OIDC_ENABLED=false` |
| Silver trống dù Bronze có data | Kiểm tra watermark: query max ingested_at trong Silver | Xóa Silver table để reset watermark nếu cần |
| Trino lỗi "table not found" | `SHOW TABLES FROM iceberg.bronze` | Flink chưa tạo table hoặc chưa có checkpoint |
