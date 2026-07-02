"""
Spark Job: Bronze → Silver

Bronze giờ là schema-on-read: mỗi record chỉ có cột `payload` (JSON string nguyên xi)
+ metadata Kafka + ingested_at. Silver là nơi DUY NHẤT định nghĩa schema:
  parse payload → chọn field → làm sạch → dedup → MERGE INTO.

Chạy incremental — chỉ xử lý records mới hơn max(ingested_at) trong Silver.

Cách chạy:
  docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --py-files /opt/spark/jobs/spark_session.py \
    /opt/spark/jobs/silver_transform.py
"""

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from spark_session import get_spark


def get_watermark(spark, silver_table):
    """max(ingested_at) của Silver — mốc incremental. None nếu bảng chưa tồn tại."""
    try:
        return (
            spark.table(silver_table)
            .agg(F.max("ingested_at").alias("max_ts"))
            .collect()[0]["max_ts"]
        )
    except Exception:
        return None


# ── API users: Bronze → Silver ───────────────────────────────────────────────

def transform_api_users(spark):
    bronze = spark.table("iceberg.bronze.api_users_raw")

    watermark = get_watermark(spark, "iceberg.silver.api_users")
    if watermark:
        bronze = bronze.filter(F.col("ingested_at") > watermark)

    if bronze.isEmpty():
        print("[api_users] Không có data mới trong Bronze.")
        return

    # ── Parse payload (schema-on-read) ──
    # Topic users_created nhận 2 dạng JSON:
    #   1. raw randomuser.me (lồng nhau): {"name": {"first": ...}, "login": {...}, "dob": {"date": ...}}
    #   2. dạng phẳng ai đó POST tay:     {"first_name": ..., "username": ..., "dob": "1995-..."}
    # get_json_object trả NULL nếu path không tồn tại → coalesce thử dạng lồng trước, phẳng sau.
    def j(path):
        return F.get_json_object(F.col("payload"), path)

    parsed = bronze.select(
        F.coalesce(j("$.name.first"),        j("$.first_name")).alias("first_name"),
        F.coalesce(j("$.name.last"),         j("$.last_name")).alias("last_name"),
        j("$.gender").alias("gender"),
        F.coalesce(j("$.location.postcode"), j("$.postcode")).alias("postcode"),
        j("$.email").alias("email"),
        F.coalesce(j("$.login.username"),    j("$.username")).alias("username"),
        F.coalesce(j("$.dob.date"),          j("$.dob")).alias("dob"),
        j("$.phone").alias("phone"),
        F.col("ingested_at"),
    )

    silver = (
        parsed
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
                Window.partitionBy("username").orderBy(F.col("ingested_at").desc())
            ),
        )
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

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

# Debezium envelope — chỉ khai những field Silver cần, field khác vẫn nằm
# nguyên trong payload ở Bronze, cần thì bổ sung schema rồi chạy lại.
CDC_SCHEMA = T.StructType([
    T.StructField("after", T.StructType([
        T.StructField("id",         T.IntegerType()),
        T.StructField("name",       T.StringType()),
        T.StructField("email",      T.StringType()),
        T.StructField("department", T.StringType()),
    ])),
    T.StructField("op",    T.StringType()),
    T.StructField("ts_ms", T.LongType()),
])


def transform_cdc_users(spark):
    bronze = spark.table("iceberg.bronze.cdc_users_raw")

    watermark = get_watermark(spark, "iceberg.silver.cdc_users")
    if watermark:
        bronze = bronze.filter(F.col("ingested_at") > watermark)

    # ── Parse payload (schema-on-read) ──
    # CDC message có cấu trúc ổn định (Debezium envelope) → dùng from_json + schema
    # tường minh. Message parse lỗi → cột thành NULL, bị filter loại ở dưới.
    parsed = (
        bronze
        .withColumn("d", F.from_json("payload", CDC_SCHEMA))
        .select(
            F.col("d.after.id").alias("id"),
            F.col("d.after.name").alias("name"),
            F.col("d.after.email").alias("email"),
            F.col("d.after.department").alias("department"),
            F.col("d.op").alias("op"),
            F.col("d.ts_ms").alias("source_ts_ms"),
            "ingested_at",
        )
    )

    # Bỏ qua DELETE (op='d', after=null) — Silver chỉ giữ trạng thái cuối cùng
    # của user còn tồn tại
    parsed = parsed.filter(F.col("op").isin("c", "u", "r") & F.col("id").isNotNull())

    if parsed.isEmpty():
        print("[cdc_users] Không có data mới trong Bronze.")
        return

    silver = (
        parsed
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
