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
    res = res["results"][0]
    return res

def format_data(res):
    data = {}
    data["first_name"] = res["name"]["first"]
    data["last_name"] = res["name"]["last"]
    data["gender"] = res["gender"]
    data["postcode"] = res["location"]["postcode"]
    data["email"] = res["email"]
    data["username"] = res["login"]["username"]
    data["dob"] = res["dob"]["date"]
    data["registered_date"] = res["registered"]["date"]
    data["phone"] = res["phone"]
    data['picture'] = res['picture']['medium']
    return data

def streaming_data():
    import json
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent=3, ensure_ascii=False))


with DAG(
    "user_automation",
    default_args=default_args,
    start_date=datetime(2026, 9, 3, 10, 00),
    schedule="@daily",
    catchup=False,
) as dag:

    streaming_task = PythonOperator(
        task_id="streaming_data_from_api", python_callable=streaming_data
    )


if __name__ == "__main__":
    streaming_data()
