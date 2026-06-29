"""
Airflow DAG — chạy dbt pipeline theo lịch định kỳ.

Thứ tự chạy:
  dbt run --select silver.*   (clean Bronze → Silver, incremental)
  dbt run --select gold.*     (Silver → Gold, full refresh)

Lịch: 15 phút/lần → Silver + Gold cập nhật gần real-time
      Bronze được Flink cập nhật liên tục (30s/commit)

Cài đặt trên Airflow worker:
  pip install dbt-trino
"""

import os
import subprocess
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

DBT_PROJECT_DIR = os.environ.get(
    "DBT_PROJECT_DIR",
    "/opt/airflow/dags/../../../query/dbt",  # điều chỉnh theo môi trường
)


def run_dbt(select: str):
    result = subprocess.run(
        ["dbt", "run", "--select", select, "--profiles-dir", DBT_PROJECT_DIR],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt failed:\n{result.stderr}")


with DAG(
    "dbt_medallion_pipeline",
    default_args={"owner": "hoanggggf"},
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",   # mỗi 15 phút
    catchup=False,
    tags=["dbt", "medallion"],
) as dag:

    silver_task = PythonOperator(
        task_id="dbt_run_silver",
        python_callable=run_dbt,
        op_kwargs={"select": "silver.*"},
    )

    gold_task = PythonOperator(
        task_id="dbt_run_gold",
        python_callable=run_dbt,
        op_kwargs={"select": "gold.*"},
    )

    # Silver phải xong trước Gold
    silver_task >> gold_task
