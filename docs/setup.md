# Hướng dẫn Setup & Khởi động

## Yêu cầu

| Công cụ | Phiên bản tối thiểu | Ghi chú |
|---------|---------------------|---------|
| Docker | 24+ | |
| Docker Compose | v2.20+ | dùng `docker compose` (không phải `docker-compose`) |
| RAM | 8 GB | Khuyến nghị 12 GB cho Flink + Spark + Trino cùng lúc |
| Disk | 20 GB | MinIO data + Docker images |

## Bước 1 — Clone dự án

```bash
git clone https://gitlab.foxai.com.vn/hoangtv/ingest-data.git
cd ingest-data
```

## Bước 2 — Build images

Build Flink và Spark images (tải JARs về, mất 2-5 phút lần đầu):

```bash
docker compose build
```

Nếu muốn build riêng từng service:

```bash
docker compose build flink-jobmanager   # cũng build flink-taskmanager
docker compose build spark-master       # cũng build spark-worker
docker compose build api
```

## Bước 3 — Khởi động infrastructure

```bash
docker compose up -d
```

Chờ tất cả services healthy (~60 giây):

```bash
docker compose ps
```

Kết quả mong đợi — tất cả STATUS phải là `healthy` hoặc `Up`:

```
NAME              STATUS
broker            healthy
postgres          healthy
kafka-connect     healthy
connector-init    exited (0)    ← OK, chạy xong rồi thoát
minio             healthy
minio-init        exited (0)    ← OK
iceberg-rest      healthy
spark-master      Up
spark-worker      Up
trino             healthy
api               healthy
flink-jobmanager  Up
flink-taskmanager Up
```

## Bước 4 — Verify iceberg-rest

Kiểm tra Iceberg REST catalog đã hoạt động:

```bash
curl -s http://localhost:8181/v1/config | python3 -m json.tool
```

Kết quả mong đợi:
```json
{
    "defaults": {},
    "overrides": {}
}
```

## Bước 5 — Submit Flink job

```bash
docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/user_processor.py
```

Kiểm tra job đang chạy:

```bash
# Qua CLI
docker exec flink-jobmanager flink list

# Qua Web UI
open http://localhost:18081
# Phải thấy 1 job RUNNING với 2 tasks
```

## Bước 6 — Verify các services

### Kafka
```bash
# List topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list
# Phải thấy: users_created, postgres.public.users

# Debezium connector
curl -s http://localhost:8083/connectors | python3 -m json.tool
# Phải thấy: ["postgres-connector"]

curl -s http://localhost:8083/connectors/postgres-connector/status | python3 -m json.tool
# "state": "RUNNING"
```

### MinIO
```bash
# Mở http://localhost:9001 (minio / minio123)
# Phải thấy bucket "warehouse" đã được tạo
```

### FastAPI
```bash
curl http://localhost:8888/health
# {"status": "ok"}
```

### Trino
```bash
curl -s http://localhost:8080/v1/info | python3 -m json.tool
# "starting": false
```

## Xử lý lỗi thường gặp

### iceberg-rest unhealthy
```bash
docker logs iceberg-rest --tail 30
# Nếu thấy "Started @...ms" → server đã start, chỉ cần đợi healthcheck pass
# Nếu lỗi S3 → kiểm tra MinIO healthy trước
docker compose ps minio
```

### Flink job không submit được
```bash
# Xem logs để debug
docker logs flink-jobmanager --tail 50
docker logs flink-taskmanager --tail 50

# Thường do iceberg-rest hoặc MinIO chưa healthy
docker compose ps iceberg-rest minio
```

### Spark không chạy được job
```bash
docker logs spark-master --tail 30
# Nếu thấy "no resources available" → spark-worker chưa kết nối
docker compose restart spark-worker
```

### PostgreSQL volume cũ — init.sql không chạy lại
```bash
# Xóa volume và khởi động lại (MẤT DATA hiện có)
docker compose down -v
docker compose up -d
```

## Reset hoàn toàn

```bash
# Dừng và xóa toàn bộ (kể cả volumes)
docker compose down -v

# Khởi động lại từ đầu
docker compose up -d
```
