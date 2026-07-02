import logging
import time
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner": "hoanggggf",
    "depends_on_past": False,
}


def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    return res["results"][0]


def stream_to_kafka():
    """Fetch user data từ API và gửi NGUYÊN XI vào Kafka topic 'users_created' trong 60 giây.

    Không format/chọn lọc field ở đây — gửi toàn bộ object randomuser trả về
    (name, location, login, dob, registered, picture, nat, ...).
    Bronze lưu raw, Silver mới chuẩn hoá schema.
    """
    import json

    from kafka import KafkaProducer

    # Airflow chạy trên host, nên kết nối Kafka qua localhost:9092
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        max_block_ms=5000,
    )

    end_time = time.time() + 60  # stream trong 60 giây

    while time.time() < end_time:
        try:
            raw = get_data()
            producer.send("users_created", json.dumps(raw).encode("utf-8"))
            logging.info("Sent raw user: %s", raw.get("email", "?"))
            time.sleep(1)
        except Exception as e:
            logging.error("Error: %s", e)
            continue

    producer.flush()
    logging.info("Done streaming.")


with DAG(
    "user_automation",
    default_args=default_args,
    start_date=datetime(2026, 9, 3, 10, 00),
    schedule="@daily",
    catchup=False,
) as dag:

    streaming_task = PythonOperator(
        task_id="stream_users_to_kafka",
        python_callable=stream_to_kafka,
    )


if __name__ == "__main__":
    stream_to_kafka()
