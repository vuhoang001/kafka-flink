"""
Airflow DAG — chạy Spark jobs theo lịch định kỳ.

Thứ tự:
  silver_transform.py  (Bronze → Silver, incremental MERGE)
  gold_transform.py    (Silver → Gold, full rebuild)

Lịch: 15 phút/lần
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

SPARK_MASTER = "spark://spark-master:7077"
JOBS_DIR     = "/opt/spark/jobs"
SPARK_SILVER = "silver_transform.py"
SPARK_GOLD   = "gold_transform.py"

SPARK_SUBMIT = (
    "docker exec spark-master "
    "/opt/spark/bin/spark-submit "
    f"--master {SPARK_MASTER} "
    "--py-files {jobs_dir}/spark_session.py "
    "{jobs_dir}/{job}"
)

with DAG(
    "spark_medallion_pipeline",
    default_args={"owner": "hoanggggf"},
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["spark", "medallion"],
) as dag:

    silver = BashOperator(
        task_id="spark_silver",
        bash_command=SPARK_SUBMIT.format(
            jobs_dir=JOBS_DIR,
            job=SPARK_SILVER,
        ),
    )

    gold = BashOperator(
        task_id="spark_gold",
        bash_command=SPARK_SUBMIT.format(
            jobs_dir=JOBS_DIR,
            job=SPARK_GOLD,
        ),
    )

    # Silver phải xong trước Gold
    silver >> gold
