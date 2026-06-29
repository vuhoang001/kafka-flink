"""
Spark Job: Silver → Gold

Rebuild hoàn toàn từ Silver mỗi lần chạy.
Tạo 2 Gold tables:
  - users_enriched : join API + CDC theo email
  - user_stats     : aggregation theo gender, department, birth_year

Cách chạy:
  docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/gold_transform.py
"""

from pyspark.sql import functions as F

from spark_session import get_spark


def build_users_enriched(spark):
    api   = spark.table("iceberg.silver.api_users")
    cdc   = spark.table("iceberg.silver.cdc_users")

    enriched = (
        api.alias("a")
        .join(
            cdc.alias("c"),
            on=F.lower(F.col("a.email")) == F.lower(F.col("c.email")),
            how="left",
        )
        .select(
            F.col("a.username"),
            F.col("a.full_name"),
            F.col("a.gender"),
            F.col("a.email"),
            F.col("a.birth_year"),
            F.col("a.phone"),
            F.col("a.postcode"),
            F.col("c.id").alias("db_id"),
            F.col("c.department"),
            F.col("c.source_ts").alias("last_db_update"),
            F.col("a.ingested_at").alias("api_ingested_at"),
        )
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")
    spark.sql("DROP TABLE IF EXISTS iceberg.gold.users_enriched")
    enriched.writeTo("iceberg.gold.users_enriched").using("iceberg").create()

    print(f"[users_enriched] Đã ghi {enriched.count()} records vào Gold.")


def build_user_stats(spark):
    enriched = spark.table("iceberg.gold.users_enriched")

    stats = (
        enriched
        .groupBy(
            F.coalesce("gender", F.lit("unknown")).alias("gender"),
            F.coalesce("department", F.lit("unknown")).alias("department"),
            "birth_year",
        )
        .agg(
            F.count("*").alias("total_users"),
            F.countDistinct("email").alias("unique_emails"),
            F.count("db_id").alias("matched_db_users"),
            F.min("api_ingested_at").alias("first_seen"),
            F.max("api_ingested_at").alias("last_seen"),
        )
        .orderBy(F.col("total_users").desc())
    )

    spark.sql("DROP TABLE IF EXISTS iceberg.gold.user_stats")
    stats.writeTo("iceberg.gold.user_stats").using("iceberg").create()

    print(f"[user_stats] Đã ghi {stats.count()} records vào Gold.")


def main():
    spark = get_spark("gold_transform")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

    build_users_enriched(spark)
    build_user_stats(spark)

    spark.stop()
    print("Gold transform hoàn thành.")


if __name__ == "__main__":
    main()
