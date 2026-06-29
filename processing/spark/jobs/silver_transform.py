"""
Spark Job: Bronze → Silver

Chạy incremental — chỉ xử lý records mới hơn max(ingested_at) trong Silver.
Dùng MERGE INTO để upsert theo unique key.

Cách chạy:
  docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/silver_transform.py
"""

from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_session import get_spark


# ── API users: Bronze → Silver ───────────────────────────────────────────────

def transform_api_users(spark):
    bronze = spark.table("iceberg.bronze.api_users_raw")

    # Lấy watermark để incremental load
    try:
        watermark = (
            spark.table("iceberg.silver.api_users")
            .agg(F.max("ingested_at").alias("max_ts"))
            .collect()[0]["max_ts"]
        )
    except Exception:
        watermark = None

    if watermark:
        bronze = bronze.filter(F.col("ingested_at") > watermark)

    if bronze.isEmpty():
        print("[api_users] Không có data mới trong Bronze.")
        return

    silver = (
        bronze
        .filter(F.col("username").isNotNull() & F.col("email").isNotNull())
        .withColumn("first_name",  F.trim("first_name"))
        .withColumn("last_name",   F.trim("last_name"))
        .withColumn("full_name",   F.concat_ws(" ", F.trim("first_name"), F.trim("last_name")))
        .withColumn("gender",      F.lower(F.trim("gender")))
        .withColumn("email",       F.lower(F.trim("email")))
        .withColumn("username",    F.lower(F.trim("username")))
        .withColumn("birth_year",  F.substring("dob", 1, 4).cast(T.IntegerType()))
        .withColumn("phone",       F.trim("phone"))
        .select(
            "first_name", "last_name", "full_name",
            "gender", "email", "username",
            "birth_year", "phone", "postcode", "ingested_at",
        )
        # Giữ record mới nhất nếu cùng username trong batch này
        .withColumn(
            "_rank",
            F.row_number().over(
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window.partitionBy("username")
                .orderBy(F.col("ingested_at").desc())
            ),
        )
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.api_users (
            first_name  STRING,
            last_name   STRING,
            full_name   STRING,
            gender      STRING,
            email       STRING,
            username    STRING,
            birth_year  INT,
            phone       STRING,
            postcode    STRING,
            ingested_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)

    silver.createOrReplaceTempView("silver_api_updates")
    spark.sql("""
        MERGE INTO iceberg.silver.api_users t
        USING silver_api_updates s ON t.username = s.username
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    count = silver.count()
    print(f"[api_users] Đã upsert {count} records vào Silver.")


# ── CDC users: Bronze → Silver ───────────────────────────────────────────────

def transform_cdc_users(spark):
    bronze = spark.table("iceberg.bronze.cdc_users_raw")

    try:
        watermark = (
            spark.table("iceberg.silver.cdc_users")
            .agg(F.max("ingested_at").alias("max_ts"))
            .collect()[0]["max_ts"]
        )
    except Exception:
        watermark = None

    if watermark:
        bronze = bronze.filter(F.col("ingested_at") > watermark)

    # Bỏ qua DELETE (op='d') — Silver chỉ giữ trạng thái cuối cùng của user còn tồn tại
    bronze = bronze.filter(F.col("op").isin("c", "u", "r") & F.col("id").isNotNull())

    if bronze.isEmpty():
        print("[cdc_users] Không có data mới trong Bronze.")
        return

    from pyspark.sql.window import Window

    silver = (
        bronze
        .filter(F.col("name").isNotNull())
        .withColumn("name",       F.trim("name"))
        .withColumn("email",      F.lower(F.trim("email")))
        .withColumn("department", F.trim("department"))
        .withColumn(
            "source_ts",
            (F.col("source_ts_ms") / 1000).cast(T.TimestampType()),
        )
        .select("id", "name", "email", "department", "op", "source_ts", "ingested_at")
        # Giữ event mới nhất của mỗi id trong batch
        .withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy("id").orderBy(F.col("source_ts").desc())
            ),
        )
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.cdc_users (
            id          INT,
            name        STRING,
            email       STRING,
            department  STRING,
            op          STRING,
            source_ts   TIMESTAMP,
            ingested_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)

    silver.createOrReplaceTempView("silver_cdc_updates")
    spark.sql("""
        MERGE INTO iceberg.silver.cdc_users t
        USING silver_cdc_updates s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    count = silver.count()
    print(f"[cdc_users] Đã upsert {count} records vào Silver.")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    spark = get_spark("silver_transform")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

    transform_api_users(spark)
    transform_cdc_users(spark)

    spark.stop()
    print("Silver transform hoàn thành.")


if __name__ == "__main__":
    main()
