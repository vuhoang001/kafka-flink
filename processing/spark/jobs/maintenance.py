"""
Spark Job: Iceberg maintenance — dọn dẹp định kỳ (khuyến nghị 1 lần/ngày).

Tại sao cần: Flink commit mỗi 30s (theo checkpoint), mỗi commit tạo 1 snapshot
+ vài file Parquet nhỏ. Sau vài ngày: hàng nghìn file bé + hàng nghìn snapshot
→ query chậm dần, metadata phình to.

Với mỗi bảng, job chạy 2 procedure của Iceberg:
  1. rewrite_data_files — gộp các file nhỏ thành file lớn (~128MB/file)
  2. expire_snapshots   — xoá snapshot cũ hơn RETENTION_DAYS (giữ tối thiểu
                          RETAIN_LAST bản để còn time-travel gần đây)

Gold không cần dọn — gold_transform drop + tạo lại bảng mỗi lần chạy.

Lưu ý: bảng đang được Flink ghi song song vẫn dọn được (Iceberg dùng optimistic
concurrency) — nếu trùng thời điểm commit thì procedure fail, lần chạy sau dọn bù,
vì vậy mỗi bảng được try/except riêng, không để chết cả job.

Cách chạy:
  docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --py-files /opt/spark/jobs/spark_session.py \
    /opt/spark/jobs/maintenance.py
"""

from datetime import datetime, timedelta

from spark_session import get_spark

TABLES = [
    "bronze.api_users_raw",
    "bronze.cdc_users_raw",
    "silver.api_users",
    "silver.cdc_users",
]

RETENTION_DAYS = 7      # snapshot cũ hơn số ngày này sẽ bị xoá
RETAIN_LAST    = 10     # nhưng luôn giữ lại ít nhất chừng này snapshot


def maintain_table(spark, table):
    full_name = f"iceberg.{table}"
    try:
        spark.table(full_name)
    except Exception:
        print(f"[maintenance] {full_name} chưa tồn tại — bỏ qua.")
        return

    cutoff = (datetime.now() - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")

    try:
        result = spark.sql(f"""
            CALL iceberg.system.rewrite_data_files(
                table   => '{table}',
                options => map('min-input-files', '5', 'target-file-size-bytes', '134217728')
            )
        """).collect()[0]
        print(f"[maintenance] {full_name}: gộp {result['rewritten_data_files_count']} file nhỏ "
              f"thành {result['added_data_files_count']} file.")
    except Exception as e:
        print(f"[maintenance] {full_name}: rewrite_data_files lỗi (sẽ dọn bù lần sau): {e}")

    try:
        result = spark.sql(f"""
            CALL iceberg.system.expire_snapshots(
                table       => '{table}',
                older_than  => TIMESTAMP '{cutoff}',
                retain_last => {RETAIN_LAST}
            )
        """).collect()[0]
        print(f"[maintenance] {full_name}: xoá {result['deleted_data_files_count']} data file "
              f"+ {result['deleted_manifest_files_count']} manifest của snapshot hết hạn.")
    except Exception as e:
        print(f"[maintenance] {full_name}: expire_snapshots lỗi (sẽ dọn bù lần sau): {e}")


def main():
    spark = get_spark("iceberg_maintenance")
    for table in TABLES:
        maintain_table(spark, table)
    spark.stop()
    print("Maintenance hoàn thành.")


if __name__ == "__main__":
    main()
