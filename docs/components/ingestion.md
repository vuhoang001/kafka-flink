# Ingestion Layer

## Tổng quan

Có 3 cách đưa data vào pipeline:

| Cách | File | Trigger | Topic Kafka |
|------|------|---------|-------------|
| HTTP API | `ingestion/api/` | Người dùng gọi POST | `users_created` |
| Airflow DAG | `ingestion/dags/kafka_stream.py` | Scheduled (hàng ngày) | `users_created` |
| PostgreSQL CDC | `cdc/postgres-connector.json` | Mọi INSERT/UPDATE/DELETE | `postgres.public.users` |

---

## FastAPI (`ingestion/api/`)

### Mô tả

FastAPI service nhận HTTP request, validate bằng Pydantic, produce vào Kafka `users_created`.

### Endpoints

| Method | Path | Mô tả |
|--------|------|-------|
| `POST` | `/users` | Nhận user data, ghi vào Kafka |
| `GET`  | `/health` | Health check |
| `GET`  | `/docs` | Swagger UI tự động |

### Schema request (`POST /users`)

```json
{
  "first_name": "Nguyen",
  "last_name":  "Van A",
  "gender":     "male",
  "postcode":   "100000",
  "email":      "a@example.com",
  "username":   "nguyenvana",
  "dob":        "1995-01-01T00:00:00Z",
  "phone":      "0901234567",
  "picture":    ""
}
```

Tất cả fields là bắt buộc trừ `picture`.

### Cách gọi

```bash
# POST một user
curl -X POST http://localhost:8000/users \
  -H "Content-Type: application/json" \
  -d '{
    "first_name": "Nguyen", "last_name": "Van A",
    "gender": "male", "postcode": "100000",
    "email": "a@example.com", "username": "nguyenvana",
    "dob": "1995-01-01T00:00:00Z", "phone": "0901234567"
  }'

# Health check
curl http://localhost:8000/health

# Swagger UI
open http://localhost:8000/docs
```

### Cấu trúc file

```
ingestion/api/
├── main.py          # FastAPI app + Kafka producer
├── requirements.txt # fastapi, uvicorn, kafka-python, pydantic[email]
└── Dockerfile       # python:3.11-slim, port 8000
```

---

## Airflow DAG — kafka_stream.py

### Mô tả

Pull data từ `randomuser.me` API và produce vào Kafka. Chạy 60 giây mỗi lần kích hoạt (1 message/giây = 60 users/lần).

### Lịch chạy

```python
schedule="@daily"   # Chạy 1 lần/ngày lúc 00:00
```

### Chạy thủ công

```bash
# Chạy trực tiếp (không cần Airflow)
python ingestion/dags/kafka_stream.py
```

### Cấu trúc flow

```
get_data()          # GET https://randomuser.me/api/
    │
format_data()       # Extract fields cần thiết
    │
stream_to_kafka()   # Produce vào topic users_created (60 giây)
```

---

## Airflow DAG — dbt_pipeline.py

### Mô tả

Chạy dbt transformation pipeline: Bronze → Silver → Gold.

### Lịch chạy

```python
schedule="*/15 * * * *"   # Mỗi 15 phút
```

### Dependency

```
dbt_run_silver >> dbt_run_gold
```

Silver phải hoàn thành trước khi Gold bắt đầu, vì Gold phụ thuộc Silver.

### Config

Biến môi trường `DBT_PROJECT_DIR` xác định đường dẫn tới dbt project. Mặc định: đường dẫn tương đối từ dags folder.

---

## PostgreSQL CDC (`cdc/`)

### Mô tả

Debezium connector theo dõi bảng `public.users` trong PostgreSQL và publish mọi thay đổi vào Kafka.

### Điều kiện PostgreSQL cần có

```sql
-- PostgreSQL phải có wal_level=logical (đã cấu hình trong docker-compose)
SHOW wal_level;  -- phải là 'logical'
```

### Connector config (`cdc/postgres-connector.json`)

```json
{
  "name": "postgres-connector",
  "config": {
    "connector.class":   "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port":     "5432",
    "database.user":     "postgres",
    "database.password": "postgres",
    "database.dbname":   "mydb",
    "table.include.list": "public.users",
    "plugin.name":       "pgoutput",
    "topic.prefix":      "postgres",
    "slot.name":         "debezium"
  }
}
```

Connector được tự động đăng ký bởi `connector-init` service khi khởi động.

### Quản lý connector

```bash
# Xem status
curl http://localhost:8083/connectors/postgres-connector/status

# Restart connector nếu lỗi
curl -X POST http://localhost:8083/connectors/postgres-connector/restart

# Xóa connector (dừng CDC)
curl -X DELETE http://localhost:8083/connectors/postgres-connector

# Đăng ký lại
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @cdc/postgres-connector.json
```
