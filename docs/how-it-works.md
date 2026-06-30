# Cách hoạt động của hệ thống — Giải thích toàn bộ luồng

## Mục lục
1. [Tổng quan](#1-tổng-quan)
2. [Nguồn dữ liệu](#2-nguồn-dữ-liệu--2-luồng-vào)
3. [Flink — Stream processing](#3-flink--xử-lý-stream-realtime)
4. [Iceberg + MinIO — Lưu trữ](#4-iceberg--minio--lưu-trữ)
5. [Spark — Batch transform](#5-spark--batch-transform)
6. [Trino — Query](#6-trino--query-engine)
7. [Viết hệ thống tương tự](#7-cách-viết-hệ-thống-tương-tự)

---

## 1. Tổng quan

Hệ thống theo kiến trúc **Medallion** — data đi qua 3 tầng chất lượng tăng dần:

```
Người dùng / PostgreSQL
       │
       ▼
   [Kafka]          ← message bus trung gian, lưu tạm, tách producer khỏi consumer
       │
       ▼
   [Flink]          ← đọc Kafka LIÊN TỤC, ghi file Parquet mỗi 30s
       │
       ▼
  BRONZE (MinIO)    ← raw data nguyên xi, append-only, không sửa
       │
       ▼
  [Spark batch]     ← chạy tay hoặc theo schedule, normalize + dedup
       │
       ▼
  SILVER (MinIO)    ← data sạch, đã dedup, upsert theo unique key
       │
       ▼
  [Spark batch]     ← JOIN 2 nguồn, aggregate
       │
       ▼
  GOLD (MinIO)      ← data business-ready, dùng trực tiếp cho BI
       │
       ▼
  [Trino]           ← SQL query engine, đọc tất cả layers
```

**Tại sao Kafka ở giữa?**
Nếu FastAPI ghi thẳng vào Flink hoặc database → khi Flink restart, data mất. Kafka giữ message trong `retention.ms` (mặc định 7 ngày). Flink crash → restart → đọc lại từ offset đã lưu → không mất event nào.

---

## 2. Nguồn dữ liệu — 2 luồng vào

### Luồng 1: FastAPI HTTP (`ingestion/api/main.py`)

```python
producer = KafkaProducer(
    bootstrap_servers=["broker:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

@app.post("/users")
def create_user(user: UserIn):
    producer.send("users_created", user.model_dump())
    return {"status": "ok"}
```

**Luồng:** `POST /users` → Pydantic validate schema → JSON → Kafka topic `users_created`

Đây là luồng **push chủ động** — ứng dụng tự gọi API khi có dữ liệu mới.

---

### Luồng 2: PostgreSQL CDC qua Debezium

CDC (Change Data Capture) = bắt thay đổi database mà **không cần sửa ứng dụng**.

PostgreSQL có tính năng **WAL** (Write-Ahead Log) — ghi lại MỌI thay đổi vào log trước khi áp vào table (để recovery). Debezium đọc WAL này và đẩy ra Kafka.

```sql
-- PostgreSQL bật WAL level logical (trong docker-compose)
postgres -c wal_level=logical
```

Message Debezium gửi vào Kafka có dạng:

```json
{
  "after":  {"id": 1, "name": "Hoang", "email": "h@example.com", "department": "Eng"},
  "before": null,
  "op":     "c",        ← c=create, u=update, d=delete, r=read(initial snapshot)
  "ts_ms":  1719734400000
}
```

- `op = "r"` — lần đầu Debezium connect, nó snapshot toàn bộ table hiện có
- `op = "c"` — INSERT mới
- `op = "u"` — UPDATE
- `op = "d"` — DELETE, lúc này `after = null`

---

## 3. Flink — Xử lý stream realtime

File: `processing/flink/user_processor.py`

### 3.1 Khởi tạo môi trường

```python
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)           # dev: 1 task slot, prod: tăng lên
env.enable_checkpointing(30000)  # checkpoint mỗi 30 giây
```

**Checkpoint là gì?**

```
t=0s    Flink bắt đầu đọc Kafka từ offset X
t=30s   Checkpoint trigger:
          1. Lưu offset Kafka hiện tại vào state backend
          2. Flush + commit file Parquet vào MinIO (Iceberg commit)
t=30s+  Data mới xuất hiện trong Bronze, Trino có thể query được
```

Nếu Flink crash lúc t=45s → restart → đọc lại từ offset đã lưu ở t=30s → replay 15s data → không mất gì.

### 3.2 Kết nối Iceberg catalog

```python
t_env.execute_sql("""
    CREATE CATALOG iceberg WITH (
        'type'                 = 'iceberg',
        'catalog-type'         = 'rest',           -- dùng REST protocol
        'uri'                  = 'http://iceberg-rest:8181',  -- catalog server
        'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',  -- đọc/ghi S3
        's3.endpoint'          = 'http://minio:9000',  -- MinIO thay AWS S3
        's3.access-key-id'     = 'minio',
        's3.secret-access-key' = 'minio123',
        's3.path-style-access' = 'true',   -- MinIO yêu cầu path-style
        'client.region'        = 'us-east-1'  -- AWS SDK v2 bắt buộc có region
    )
""")
```

**Catalog là gì?** Catalog = danh bạ của tables. Khi Flink hỏi "table `iceberg.bronze.api_users_raw` ở đâu?" → catalog trả lời "metadata của nó ở `s3://warehouse/bronze/api_users_raw/metadata/v5.metadata.json`" → Flink đọc metadata → biết data file ở đâu.

### 3.3 Khai báo Kafka source như Table

```python
t_env.execute_sql("""
    CREATE TABLE api_source (
        first_name STRING,
        last_name  STRING,
        ...
    ) WITH (
        'connector'          = 'kafka',
        'topic'              = 'users_created',
        'scan.startup.mode'  = 'earliest-offset',  -- đọc từ đầu topic
        'format'             = 'json'
    )
""")
```

Đây là **virtual table** — không lưu gì cả, chỉ khai báo schema và cách đọc từ Kafka. Mỗi row = 1 Kafka message được parse từ JSON.

### 3.4 Khai báo Iceberg sink

```python
t_env.execute_sql("""
    CREATE TABLE IF NOT EXISTS iceberg.bronze.api_users_raw (
        first_name  STRING,
        ...
        ingested_at TIMESTAMP(3)   -- thêm field này để biết khi nào vào hệ thống
    ) WITH (
        'format-version'       = '2',      -- Iceberg v2 hỗ trợ MERGE INTO
        'write.format.default' = 'parquet' -- columnar format, nén tốt
    )
""")
```

Table này **thật** — đăng ký trong catalog, data lưu xuống MinIO dạng Parquet.

### 3.5 Chạy 2 pipelines song song

```python
stmt_set = t_env.create_statement_set()

# Pipeline 1: API events → Bronze
stmt_set.add_insert_sql("""
    INSERT INTO iceberg.bronze.api_users_raw
    SELECT first_name, last_name, ..., CURRENT_TIMESTAMP
    FROM default_catalog.default_database.api_source
""")

# Pipeline 2: CDC events → Bronze
stmt_set.add_insert_sql("""
    INSERT INTO iceberg.bronze.cdc_users_raw
    SELECT
        after.id, after.name, after.email, after.department,
        op, ts_ms,
        CURRENT_TIMESTAMP
    FROM default_catalog.default_database.cdc_source
    WHERE after IS NOT NULL   -- bỏ qua DELETE (after=null)
""")

stmt_set.execute()  -- submit cả 2, chạy song song, checkpoint chung
```

`StatementSet` cho phép Flink chạy nhiều INSERT trong 1 job, chia sẻ checkpoint và resource.

---

## 4. Iceberg + MinIO — Lưu trữ

### Cấu trúc file trong MinIO

```
s3://warehouse/
└── bronze/
    └── api_users_raw/
        ├── data/
        │   ├── 00001-0-abc123.parquet   ← data thực tế (checkpoint 1)
        │   ├── 00002-0-def456.parquet   ← data thực tế (checkpoint 2)
        │   └── ...
        └── metadata/
            ├── v1.metadata.json         ← schema table, partition spec
            ├── v2.metadata.json         ← cập nhật sau snapshot 1
            ├── snap-001.avro            ← snapshot 1: danh sách files
            └── snap-002-manifest.avro   ← manifest: thống kê từng file
```

### Iceberg REST Catalog (`tabulario/iceberg-rest:8181`)

Catalog chỉ lưu **1 thứ duy nhất** cho mỗi table: đường dẫn đến metadata file mới nhất.

```
iceberg-rest memory:
  bronze.api_users_raw → s3://warehouse/bronze/api_users_raw/metadata/v5.metadata.json
```

Khi Flink commit snapshot mới:
1. Ghi data file mới vào MinIO
2. Ghi metadata file mới vào MinIO
3. Gọi REST API cập nhật pointer trong catalog

**Hậu quả quan trọng:** iceberg-rest dùng SQLite **in-memory** → restart là mất hết pointer → state không nhất quán với MinIO. Đó là lý do phải `docker compose down -v` khi có vấn đề, để MinIO cũng được reset theo.

---

## 5. Spark — Batch transform

### 5.1 SparkSession (`spark_session.py`)

```python
def get_spark(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        # Extension bắt buộc để dùng MERGE INTO, time-travel
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Đăng ký catalog tên "iceberg"
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/")
        # S3FileIO cho MinIO
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.iceberg.s3.access-key-id", "minio")
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minio123")
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
        .getOrCreate()
    )
```

### 5.2 Silver — Incremental MERGE (`silver_transform.py`)

**Tại sao incremental?**
- Bronze là append-only — Flink chỉ thêm row, không bao giờ sửa
- Nếu đọc toàn bộ Bronze mỗi lần → ngày càng chậm
- Dùng watermark = `max(ingested_at)` trong Silver để chỉ xử lý data mới

```python
# Bước 1: Lấy watermark
watermark = spark.table("iceberg.silver.api_users") \
    .agg(F.max("ingested_at")).collect()[0][0]

# Bước 2: Chỉ lấy data mới hơn watermark
bronze = spark.table("iceberg.bronze.api_users_raw")
if watermark:
    bronze = bronze.filter(F.col("ingested_at") > watermark)

# Bước 3: Normalize
silver = (
    bronze
    .filter(F.col("username").isNotNull())   # bỏ row thiếu key
    .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
    .withColumn("birth_year", F.substring("dob", 1, 4).cast(IntegerType()))
    .withColumn("email", F.lower(F.trim("email")))
    # Dedup: nếu cùng username xuất hiện nhiều lần trong batch → giữ mới nhất
    .withColumn("_rank",
        row_number().over(
            Window.partitionBy("username").orderBy(col("ingested_at").desc())
        )
    )
    .filter("_rank = 1").drop("_rank")
)

# Bước 4: MERGE INTO (upsert)
silver.createOrReplaceTempView("updates")
spark.sql("""
    MERGE INTO iceberg.silver.api_users t
    USING updates s ON t.username = s.username
    WHEN MATCHED     THEN UPDATE SET *   -- user đã có → cập nhật
    WHEN NOT MATCHED THEN INSERT *       -- user mới → thêm vào
""")
```

**CDC Silver** tương tự, thêm bước lọc DELETE:
```python
# op='d' = DELETE → bỏ qua, Silver chỉ giữ trạng thái cuối của user còn tồn tại
bronze = bronze.filter(F.col("op").isin("c", "u", "r"))
```

### 5.3 Gold — Full rebuild (`gold_transform.py`)

**Tại sao full rebuild (không incremental)?**
- Gold cần JOIN toàn bộ Silver API + Silver CDC → không thể chỉ xử lý phần mới
- Silver đã dedup nên kích thước nhỏ, rebuild nhanh

```python
# JOIN 2 nguồn theo email
api = spark.table("iceberg.silver.api_users")
cdc = spark.table("iceberg.silver.cdc_users")

enriched = api.alias("a").join(
    cdc.alias("c"),
    on=F.lower(F.col("a.email")) == F.lower(F.col("c.email")),
    how="left"   # giữ tất cả API users, CDC match là bonus
).select(
    "a.username", "a.full_name", "a.gender", "a.email",
    "a.birth_year", "a.phone",
    "c.id",           # NULL nếu không có trong DB
    "c.department",   # NULL nếu không match
)

# DROP + tạo lại (full rebuild)
spark.sql("DROP TABLE IF EXISTS iceberg.gold.users_enriched")
enriched.writeTo("iceberg.gold.users_enriched").using("iceberg").create()
```

---

## 6. Trino — Query engine

Trino **không lưu data** — chỉ đọc Iceberg metadata để biết file nào ở đâu, rồi đọc Parquet trực tiếp từ MinIO.

Config (`query/trino/etc/catalog/iceberg.properties`):
```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
iceberg.rest-catalog.warehouse=s3://warehouse/
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.aws-access-key=minio
s3.aws-secret-key=minio123
s3.path-style-access=true
```

Trino hỗ trợ **time-travel** — feature của Iceberg cho phép query data tại thời điểm quá khứ:

```sql
-- Xem lịch sử snapshots
SELECT snapshot_id, committed_at, operation
FROM iceberg.bronze."api_users_raw$snapshots"
ORDER BY committed_at DESC;

-- Query data tại 1 thời điểm cụ thể
SELECT COUNT(*) FROM iceberg.bronze.api_users_raw
FOR TIMESTAMP AS OF TIMESTAMP '2026-06-30 06:00:00 UTC';
```

---

## 7. Cách viết hệ thống tương tự

### 7.1 Pattern tổng quát

```
[Nguồn] → [Message Bus] → [Stream Processor] → [Raw Storage]
                                                      ↓
                                            [Batch Transform] → [Clean Storage]
                                                                      ↓
                                                              [Query Engine]
```

| Vị trí | Công nghệ trong project | Có thể thay bằng |
|--------|------------------------|-----------------|
| Message Bus | Kafka | Pulsar, Kinesis, PubSub |
| Stream Processor | Flink | Spark Streaming, Kafka Streams |
| Object Storage | MinIO | AWS S3, GCS, Azure Blob |
| Table Format | Iceberg | Delta Lake, Hudi |
| Catalog | iceberg-rest | Glue, Hive Metastore |
| Batch Transform | Spark | dbt (nếu engine hỗ trợ MERGE INTO) |
| Query Engine | Trino | Athena, BigQuery, DuckDB |

### 7.2 Khi nào dùng loại nguồn nào

| Tình huống | Cách làm |
|------------|---------|
| App tự gửi event | HTTP API → Kafka Producer |
| Database thay đổi mà không muốn sửa code | CDC (Debezium) |
| File CSV/JSON upload định kỳ | Spark batch đọc file trực tiếp |
| Dữ liệu từ API bên ngoài | Airflow DAG gọi API → ghi Kafka hoặc thẳng Iceberg |

### 7.3 Khi nào dùng Stream vs Batch

| Stream (Flink) | Batch (Spark) |
|----------------|---------------|
| Cần data trong vài giây → vài phút | Chấp nhận delay 15-60 phút |
| Append-only: event log, sensor data | Cần MERGE INTO, JOIN nhiều bảng |
| Volume lớn, liên tục | Chạy theo schedule |
| Bronze layer | Silver + Gold layer |

### 7.4 Template Flink job mới

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30000)  # điều chỉnh theo latency yêu cầu
    t_env = StreamTableEnvironment.create(env)

    # 1. Kết nối catalog
    t_env.execute_sql("""
        CREATE CATALOG iceberg WITH (
            'type'                 = 'iceberg',
            'catalog-type'         = 'rest',
            'uri'                  = 'http://iceberg-rest:8181',
            'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint'          = 'http://minio:9000',
            's3.access-key-id'     = 'minio',
            's3.secret-access-key' = 'minio123',
            's3.path-style-access' = 'true',
            'client.region'        = 'us-east-1'
        )
    """)

    # 2. Khai báo Kafka source
    t_env.execute_sql("""
        CREATE TABLE kafka_source (
            field1 STRING,
            field2 INT
            -- thêm fields theo schema Kafka message
        ) WITH (
            'connector'          = 'kafka',
            'topic'              = 'your-topic',
            'properties.bootstrap.servers' = 'broker:29092',
            'scan.startup.mode'  = 'earliest-offset',
            'format'             = 'json'
        )
    """)

    # 3. Khai báo Iceberg sink
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg.bronze")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS iceberg.bronze.your_table (
            field1      STRING,
            field2      INT,
            ingested_at TIMESTAMP(3)   -- luôn thêm cái này
        ) WITH (
            'format-version'       = '2',
            'write.format.default' = 'parquet'
        )
    """)

    # 4. Pipeline
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql("""
        INSERT INTO iceberg.bronze.your_table
        SELECT field1, field2, CURRENT_TIMESTAMP
        FROM default_catalog.default_database.kafka_source
    """)
    stmt_set.execute()

if __name__ == "__main__":
    main()
```

### 7.5 Template Spark Silver job mới

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session import get_spark

def transform(spark):
    bronze = spark.table("iceberg.bronze.your_table")

    # Watermark — chỉ xử lý data mới
    try:
        watermark = spark.table("iceberg.silver.your_table") \
            .agg(F.max("ingested_at")).collect()[0][0]
    except Exception:
        watermark = None

    if watermark:
        bronze = bronze.filter(F.col("ingested_at") > watermark)

    if bronze.isEmpty():
        print("Không có data mới.")
        return

    # Transform
    silver = (
        bronze
        .filter(F.col("unique_key").isNotNull())
        # ... normalize ...
        # Dedup trong batch
        .withColumn("_rank",
            F.row_number().over(
                Window.partitionBy("unique_key")
                      .orderBy(F.col("ingested_at").desc())
            )
        )
        .filter("_rank = 1").drop("_rank")
    )

    # Tạo table nếu chưa có
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.your_table (
            unique_key STRING,
            -- ... fields ...
            ingested_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)

    # MERGE INTO — upsert
    silver.createOrReplaceTempView("updates")
    spark.sql("""
        MERGE INTO iceberg.silver.your_table t
        USING updates s ON t.unique_key = s.unique_key
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

if __name__ == "__main__":
    spark = get_spark("your_silver_job")
    transform(spark)
    spark.stop()
```

### 7.6 Những điểm hay bị nhầm

| Vấn đề | Nguyên nhân | Fix |
|--------|-------------|-----|
| Flink không ghi Bronze dù đang chạy | Chưa có checkpoint | Đợi 30s hoặc kiểm tra `flink list` |
| `NoClassDefFoundError: UserGroupInformation` | Flink 1.17 khởi động Hadoop security module | `security.delegation.tokens.enabled: false` |
| `Unable to load region` (S3FileIO) | AWS SDK v2 không tìm thấy region | Thêm `'client.region' = 'us-east-1'` vào catalog config HOẶC env var `AWS_REGION` |
| `FileNotFoundException` khi query Trino | iceberg-rest restart → metadata mất sync với MinIO | `docker compose down -v && docker compose up -d` |
| Bronze có data nhưng Silver trống | Watermark quá cao từ run trước | DELETE Silver table để reset watermark |
| Trino query chậm | Quá nhiều small files (checkpoint interval ngắn) | Chạy Iceberg compaction: `ALTER TABLE ... EXECUTE optimize` |
