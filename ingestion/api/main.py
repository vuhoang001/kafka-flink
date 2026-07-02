import json
import os
from typing import Any

import psycopg2
from fastapi import Body, FastAPI, HTTPException
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import BaseModel

app = FastAPI(title="User Ingest API")

_kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:29092").split(",")

producer = KafkaProducer(
    bootstrap_servers=_kafka_servers,
    max_block_ms=5000,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

_pg_config = dict(
    host=os.environ.get("POSTGRES_HOST", "postgres"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    dbname=os.environ.get("POSTGRES_DB", "mydb"),
    user=os.environ.get("POSTGRES_USER", "postgres"),
    password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
)


# ── Đường 1: bắn thẳng Kafka (schema-on-read, nhận JSON bất kỳ) ──────────────

@app.post("/users", status_code=201)
def create_user(payload: dict[str, Any] = Body(...)):
    """Nhận JSON bất kỳ và đẩy nguyên xi vào Kafka topic users_created.

    Không validate schema — Bronze lưu raw, Silver mới chuẩn hoá.
    """
    try:
        future = producer.send("users_created", payload)
        future.get(timeout=5)
    except KafkaError as e:
        raise HTTPException(status_code=502, detail=f"Kafka error: {e}")
    return {"status": "ok", "message": "Payload sent to Kafka"}


# ── Đường 2: ghi PostgreSQL → Debezium bắt CDC → Kafka ───────────────────────

class DbUserIn(BaseModel):
    """Khác /users, endpoint này CÓ schema — vì đích là bảng SQL có cột cố định.

    Luồng: INSERT vào Postgres → WAL → Debezium → topic postgres.public.users
    → Flink → bronze.cdc_users_raw. API không đụng vào Kafka.
    """
    name: str
    email: str
    department: str | None = None


@app.post("/db/users", status_code=201)
def create_db_user(user: DbUserIn):
    try:
        # Kết nối mỗi request — đủ cho demo; production dùng connection pool.
        with psycopg2.connect(**_pg_config) as conn:
            with conn.cursor() as cur:
                # email có UNIQUE constraint — POST trùng email = update thay vì
                # tạo dòng mới, Debezium sẽ phát event op='u' thay vì op='c'
                cur.execute(
                    """
                    INSERT INTO users (name, email, department) VALUES (%s, %s, %s)
                    ON CONFLICT (email) DO UPDATE
                        SET name = EXCLUDED.name, department = EXCLUDED.department
                    RETURNING id
                    """,
                    (user.name, user.email, user.department),
                )
                new_id = cur.fetchone()[0]
    except psycopg2.Error as e:
        raise HTTPException(status_code=502, detail=f"Postgres error: {e}")
    return {"status": "ok", "id": new_id, "message": f"User {user.name} inserted into Postgres (CDC will pick it up)"}


@app.get("/health")
def health():
    return {"status": "ok"}
