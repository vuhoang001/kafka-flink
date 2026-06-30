import json
import os

from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import BaseModel, EmailStr

app = FastAPI(title="User Ingest API")

_kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:29092").split(",")

producer = KafkaProducer(
    bootstrap_servers=_kafka_servers,
    max_block_ms=5000,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


class UserIn(BaseModel):
    first_name: str
    last_name: str
    gender: str
    postcode: str
    email: EmailStr
    username: str
    dob: str
    phone: str
    picture: str = ""


@app.post("/users", status_code=201)
def create_user(user: UserIn):
    try:
        future = producer.send("users_created", user.model_dump())
        future.get(timeout=5)
    except KafkaError as e:
        raise HTTPException(status_code=502, detail=f"Kafka error: {e}")
    return {"status": "ok", "message": f"User {user.username} sent to Kafka"}


@app.get("/health")
def health():
    return {"status": "ok"}
