import json
import os
from typing import Any

from fastapi import Body, FastAPI, HTTPException
from kafka import KafkaProducer
from kafka.errors import KafkaError

app = FastAPI(title="User Ingest API")

_kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:29092").split(",")

producer = KafkaProducer(
    bootstrap_servers=_kafka_servers,
    max_block_ms=5000,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


@app.post("/users", status_code=201)
def create_user(payload: dict[str, Any] = Body(...)):
    """Nhận JSON bất kỳ và đẩy nguyên xi vào Kafka.

    Không validate schema ở đây — Bronze lưu raw (schema-on-read),
    việc chuẩn hoá cấu trúc là trách nhiệm của bước Bronze → Silver.
    """
    try:
        future = producer.send("users_created", payload)
        future.get(timeout=5)
    except KafkaError as e:
        raise HTTPException(status_code=502, detail=f"Kafka error: {e}")
    return {"status": "ok", "message": "Payload sent to Kafka"}


@app.get("/health")
def health():
    return {"status": "ok"}
