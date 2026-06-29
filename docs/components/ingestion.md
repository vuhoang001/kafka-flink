# Ingestion Layer

## Tổng quan

Có 2 cách đưa data vào pipeline:

1. **FastAPI** — HTTP API cho external applications đẩy user data
2. **PostgreSQL CDC** — Debezium tự động capture thay đổi từ DB

## FastAPI

### Endpoints

| Method | Path | Mô tả |
|--------|------|-------|
| POST | `/users` | Gửi 1 user vào Kafka topic `users_created` |
| GET | `/health` | Health check |
| GET | `/docs` | Swagger UI tự động |

### Request schema (POST /users)

```json
{
  "first_name": "Hoang",
  "last_name":  "Nguyen",
  "gender":     "male",
  "postcode":   "100000",
  "email":      "hoang@example.com",
  "username":   "hoangnv",
  "dob":        "1995-01-15",
  "phone":      "0901234567",
  "picture":    ""
}
```

| Field | Type | Required | Validation |
|-------|------|----------|------------|
| first_name | string | ✓ | |
| last_name | string | ✓ | |
| gender | string | ✓ | |
| postcode | string | ✓ | |
| email | string | ✓ | EmailStr (Pydantic validate format) |
| username | string | ✓ | |
| dob | string | ✓ | Chuỗi tự do, ví dụ "1995-01-15" |
| phone | string | ✓ | |
| picture | string | — | Mặc định rỗng `""` |

### Response

```json
{"status": "ok", "message": "User hoangnv sent to Kafka"}
```

Lỗi Kafka:
```json
{"detail": "Kafka error: ..."}
```
HTTP 502.

### Ví dụ gọi API

```bash
# 1 user
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
    "phone":      "0901234567"
  }'

# Batch (loop trong bash)
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8000/users \
    -H "Content-Type: application/json" \
    -d "{
      \"first_name\": \"Test\",
      \"last_name\":  \"User$i\",
      \"gender\":     \"male\",
      \"postcode\":   \"10000$i\",
      \"email\":      \"test$i@example.com\",
      \"username\":   \"testuser$i\",
      \"dob\":        \"199$i-06-15\",
      \"phone\":      \"090000000$i\"
    }" && echo ""
done

# Swagger UI
open http://localhost:8000/docs
```

## PostgreSQL CDC

Dữ liệu thay đổi trong bảng `public.users` của PostgreSQL sẽ tự động được Debezium capture và gửi vào Kafka.

### Thay đổi trong PostgreSQL

```bash
# INSERT (op=c trong Kafka)
docker exec postgres psql -U postgres -d mydb -c "
  INSERT INTO users (name, email, department)
  VALUES ('New User', 'newuser@example.com', 'Engineering');
"

# UPDATE (op=u trong Kafka)
docker exec postgres psql -U postgres -d mydb -c "
  UPDATE users SET department = 'Platform' WHERE email = 'newuser@example.com';
"

# DELETE (op=d trong Kafka — Silver sẽ bỏ qua)
docker exec postgres psql -U postgres -d mydb -c "
  DELETE FROM users WHERE email = 'newuser@example.com';
"

# Xem dữ liệu hiện tại
docker exec postgres psql -U postgres -d mydb -c "SELECT * FROM users;"
```

### Schema bảng `users`

```sql
CREATE TABLE users (
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(100) NOT NULL,
    email      VARCHAR(100) NOT NULL,
    department VARCHAR(50)
);
```

## Airflow DAG

File: `ingestion/dags/spark_pipeline.py`

DAG `spark_medallion_pipeline` chạy Spark Silver → Gold mỗi 15 phút:

```
silver_transform.py  →  gold_transform.py
```

Nếu chưa cài Airflow, chạy thủ công:

```bash
# Silver
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/spark_session.py \
  /opt/spark/jobs/silver_transform.py

# Gold (sau khi Silver xong)
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/spark/jobs/spark_session.py \
  /opt/spark/jobs/gold_transform.py
```
