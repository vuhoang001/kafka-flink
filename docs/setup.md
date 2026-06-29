# Hướng dẫn cài đặt và khởi động

## Yêu cầu

| Công cụ | Phiên bản tối thiểu |
|---------|---------------------|
| Docker  | 24+                 |
| Docker Compose | 2.20+        |
| Python  | 3.9+ (cho dbt local) |

Phần cứng khuyến nghị: **8 GB RAM**, 4 CPU cores (Flink + Trino ngốn RAM).

---

## Bước 1 — Khởi động toàn bộ stack

```bash
docker compose up -d
```

Thứ tự khởi động tự động (theo `depends_on`):

```
postgres → broker → kafka-connect → connector-init
        └→ nessie
minio   → minio-init
        → flink-jobmanager → flink-taskmanager
        → nessie → trino
broker  → api
```

Kiểm tra tất cả service đang chạy:

```bash
docker compose ps
```

Đợi tất cả ở trạng thái `healthy` (khoảng 60–120 giây).

---

## Bước 2 — Verify từng service

```bash
# Kafka đang nhận kết nối
docker exec broker kafka-topics \
  --bootstrap-server broker:29092 --list

# Debezium connector đã đăng ký
curl -s http://localhost:8083/connectors | python3 -m json.tool

# Nessie REST catalog phản hồi
curl http://localhost:19120/iceberg/v1/config

# MinIO bucket warehouse tồn tại
docker run --rm --network realtime-data-streaming_confluent \
  minio/mc:latest sh -c \
  "mc alias set local http://minio:9000 minio minio123 && mc ls local/"

# Trino đang chạy
curl -s http://localhost:8080/v1/info | python3 -m json.tool
```

---

## Bước 3 — Submit Flink job

```bash
docker exec flink-jobmanager \
  flink run -d -py /opt/flink/jobs/user_processor.py
```

Kiểm tra job đã chạy:

```bash
docker exec flink-jobmanager flink list
```

Xem log nếu có lỗi:

```bash
docker logs flink-taskmanager 2>&1 | tail -50
```

---

## Bước 4 — Đẩy data vào

### Qua FastAPI

```bash
curl -X POST http://localhost:8000/users \
  -H "Content-Type: application/json" \
  -d '{
    "first_name": "Nguyen",
    "last_name":  "Van A",
    "gender":     "male",
    "postcode":   "100000",
    "email":      "a@example.com",
    "username":   "nguyenvana",
    "dob":        "1995-01-01T00:00:00Z",
    "phone":      "0901234567",
    "picture":    ""
  }'
```

Swagger UI đầy đủ: http://localhost:8000/docs

### Qua Airflow DAG (pull tự động)

```bash
# Chạy manual một lần
python ingestion/dags/kafka_stream.py
```

DAG `user_automation` chạy `@daily` nếu Airflow đang chạy.

---

## Bước 5 — Chờ Bronze layer có data

Sau khoảng 30 giây (một checkpoint cycle), kiểm tra:

```bash
docker exec -it trino trino \
  --execute "SELECT COUNT(*) FROM iceberg.bronze.api_users_raw"
```

---

## Bước 6 — Cài dbt và chạy transformation

```bash
pip install dbt-trino

cd query/dbt

# Kiểm tra kết nối Trino
dbt debug --profiles-dir .

# Chạy toàn bộ pipeline Bronze → Silver → Gold
dbt run --profiles-dir .
```

Sau khi `dbt run` xong:

```bash
docker exec -it trino trino \
  --execute "SELECT * FROM iceberg.gold.users_enriched LIMIT 5"
```

---

## Rebuild sau khi thay đổi code

### Chỉ rebuild Flink (thay đổi user_processor.py hoặc Dockerfile)

```bash
docker compose up -d --build flink-jobmanager flink-taskmanager

# Submit lại job mới
docker exec flink-jobmanager flink list  # ghi lại job ID
docker exec flink-jobmanager flink cancel <job-id>
docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/user_processor.py
```

### Rebuild toàn bộ (xóa data cũ)

```bash
docker compose down -v   # xóa cả volumes (mất data!)
docker compose up -d --build
```

---

## Xem UI

| Service       | URL                        | Login              |
|---------------|----------------------------|--------------------|
| API Swagger   | http://localhost:8000/docs | —                  |
| Flink Web UI  | http://localhost:18081     | —                  |
| MinIO Console | http://localhost:9001      | minio / minio123   |
| Nessie        | http://localhost:19120     | —                  |
| Trino Web UI  | http://localhost:8080      | user: `trino`      |
| Kafka Connect | http://localhost:8083      | —                  |
