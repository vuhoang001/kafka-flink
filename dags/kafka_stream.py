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


def format_data(res):
    return {
        "first_name": res["name"]["first"],
        "last_name": res["name"]["last"],
        "gender": res["gender"],
        "postcode": str(res["location"]["postcode"]),
        "email": res["email"],
        "username": res["login"]["username"],
        "dob": res["dob"]["date"],
        "registered_date": res["registered"]["date"],
        "phone": res["phone"],
        "picture": res["picture"]["medium"],
    }


def stream_to_kafka():
    """Fetch user data từ API và gửi liên tục vào Kafka topic 'users_created' trong 60 giây."""
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
            user = format_data(raw)
            producer.send("users_created", json.dumps(user).encode("utf-8"))
            logging.info("Sent: %s %s", user["first_name"], user["last_name"])
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
