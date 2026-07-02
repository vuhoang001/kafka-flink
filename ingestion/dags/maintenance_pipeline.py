"""
Airflow DAG — dọn dẹp Iceberg hằng ngày (gộp file nhỏ + xoá snapshot cũ).

Chạy lúc 2h sáng, giờ thấp điểm — rewrite_data_files đọc/ghi lại nhiều data,
tránh giành tài nguyên với pipeline Silver/Gold 15 phút.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

SPARK_MASTER = "spark://spark-master:7077"
JOBS_DIR     = "/opt/spark/jobs"

with DAG(
    "iceberg_maintenance",
    default_args={"owner": "hoanggggf"},
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["spark", "iceberg", "maintenance"],
) as dag:

    BashOperator(
        task_id="iceberg_maintenance",
        bash_command=(
            "docker exec spark-master "
            "/opt/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            f"--py-files {JOBS_DIR}/spark_session.py "
            f"{JOBS_DIR}/maintenance.py"
        ),
    )
