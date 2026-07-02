# Deep Dive — Hiểu tường tận từng dòng code để tự viết lại bằng tay

> Tài liệu này khác `how-it-works.md`: thay vì mô tả tổng quan, nó đi **từng dòng code, từng config**, giải thích **tại sao viết như vậy**, và kết thúc bằng **lộ trình tự xây lại từ con số 0**.

## Mục lục

1. [Tư duy trước khi đọc code: bài toán là gì?](#1-tư-duy-trước-khi-đọc-code)
2. [Kafka — trái tim của hệ thống](#2-kafka)
3. [Luồng ingest 1: FastAPI → Kafka](#3-luồng-ingest-1-fastapi--kafka)
4. [Luồng ingest 2: PostgreSQL → Debezium CDC → Kafka](#4-luồng-ingest-2-cdc)
5. [Iceberg + MinIO + REST Catalog — nơi data nằm xuống](#5-iceberg--minio--rest-catalog)
6. [Flink — đọc Kafka liên tục, ghi Bronze](#6-flink)
7. [Spark — Bronze → Silver → Gold](#7-spark)
8. [Trino — cửa sổ SQL nhìn vào tất cả](#8-trino)
9. [Airflow — người bấm nút theo lịch](#9-airflow)
10. [Lộ trình tự viết lại bằng tay](#10-lộ-trình-tự-viết-lại-bằng-tay)
11. [Câu hỏi tự kiểm tra](#11-câu-hỏi-tự-kiểm-tra)

---

## 1. Tư duy trước khi đọc code

Đừng đọc code trước. Hãy hiểu **bài toán** trước, vì mỗi công nghệ trong repo này tồn tại để giải một vấn đề cụ thể. Nếu bạn nắm được chuỗi vấn đề → giải pháp dưới đây, bạn sẽ tự suy ra được kiến trúc mà không cần nhớ:

| # | Vấn đề | Nếu không giải | Giải pháp trong repo |
|---|--------|----------------|----------------------|
| 1 | Data đến liên tục từ nhiều nguồn (API, database), tốc độ khác nhau | Producer ghi thẳng vào consumer → consumer chết là mất data, producer bị block | **Kafka** làm bộ đệm trung gian |
| 2 | Muốn bắt mọi thay đổi trong PostgreSQL mà không sửa code app | Polling `SELECT * WHERE updated_at > ?` → miss DELETE, tốn tài nguyên, trễ | **Debezium** đọc WAL (Change Data Capture) |
| 3 | Cần đọc Kafka 24/7 và ghi xuống storage, không mất/không trùng event | Tự viết consumer loop → tự quản lý offset, crash là hỏng | **Flink** với checkpoint + exactly-once |
| 4 | File Parquet trên S3 không có khái niệm "bảng": không ACID, không UPDATE, không schema | Ghi đè file → reader đang đọc bị lỗi; không MERGE được | **Iceberg** table format |
| 5 | Nhiều engine (Flink, Spark, Trino) cùng đọc/ghi một bảng — ai giữ "sự thật" bảng gồm những file nào? | Mỗi engine tự track → lệch nhau | **Iceberg REST Catalog** — một server metadata chung |
| 6 | Data thô cần làm sạch, dedup, join, aggregate | Query trực tiếp raw data → chậm, sai, mỗi người tự xử lý một kiểu | **Spark** batch + kiến trúc **Medallion** (Bronze/Silver/Gold) |
| 7 | Analyst muốn dùng SQL, không muốn viết Spark | — | **Trino** query engine |
| 8 | Job Spark phải chạy đều đặn 15 phút/lần, Silver xong mới đến Gold | Chạy tay, quên là data cũ | **Airflow** scheduler |

**Medallion Architecture** là quy ước đặt tên 3 tầng chất lượng:

- **Bronze** = raw, nguyên xi như lúc vào, append-only, không bao giờ sửa. Mục đích: nếu logic transform sai, bạn luôn có bản gốc để chạy lại.
- **Silver** = đã làm sạch (trim, lowercase, cast kiểu), đã dedup, mỗi entity 1 dòng (upsert theo key).
- **Gold** = phục vụ business trực tiếp: đã join các nguồn, đã aggregate, query là ra dashboard.

Luồng tổng: `(API | Postgres) → Kafka → Flink → Bronze → Spark → Silver → Spark → Gold → Trino`.

Điểm chia quan trọng: **trước Bronze là streaming (liên tục, độ trễ giây), sau Bronze là batch (15 phút/lần)**. Đây là pattern rất phổ biến trong thực tế: ingest realtime, transform theo lô.

---

## 2. Kafka

### 2.1. Khái niệm tối thiểu cần nắm

- **Topic**: một "kênh" chứa message, giống một file log chỉ ghi thêm (append-only). Repo có 2 topic chính: `users_created` (từ API) và `postgres.public.users` (từ Debezium).
- **Partition**: mỗi topic chia thành N partition để scale. Repo dùng 1 partition cho đơn giản.
- **Offset**: số thứ tự của message trong partition. Consumer nhớ "tôi đã đọc đến offset nào" → crash xong đọc tiếp từ đó, không mất.
- **Consumer group**: nhóm consumer chia nhau đọc. Hai group khác nhau đọc **độc lập** — cùng một message có thể được cả Flink lẫn một consumer debug đọc, không ảnh hưởng nhau. Đây là lý do Flink dùng `group.id = flink-api-group` và `flink-cdc-group` riêng.
- **Retention**: Kafka giữ message mặc định 7 ngày rồi mới xoá, **bất kể đã có ai đọc chưa**. Kafka không phải queue kiểu "đọc xong là mất".

### 2.2. Giải thích config trong `docker-compose.yaml`

```yaml
broker:
  image: confluentinc/cp-kafka:7.4.0
  environment:
    KAFKA_NODE_ID: 1
    KAFKA_PROCESS_ROLES: 'broker,controller'
    KAFKA_CONTROLLER_QUORUM_VOTERS: '1@broker:29093'
```

- **KRaft mode**: Kafka cũ cần ZooKeeper để bầu leader và giữ metadata. Từ Kafka 3.x, chính Kafka tự làm việc đó (KRaft = Kafka Raft). `PROCESS_ROLES: 'broker,controller'` nghĩa là node này vừa chứa data (broker) vừa quản lý cluster (controller). Cluster 1 node thì quorum voters chỉ có chính nó.

```yaml
    KAFKA_LISTENERS: 'PLAINTEXT://broker:29092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://broker:29093'
    KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092'
```

**Đây là phần khó hiểu nhất và hay gây lỗi nhất của Kafka trong Docker.** Cơ chế kết nối Kafka có 2 bước:

1. Client kết nối đến địa chỉ bootstrap ban đầu.
2. Broker trả về danh sách **advertised listeners** — "muốn nói chuyện với tôi thì gọi vào địa chỉ này". Client **bỏ địa chỉ ban đầu** và dùng địa chỉ được trả về.

Vấn đề: container trong Docker network gọi broker bằng hostname `broker`, còn process trên máy host gọi bằng `localhost`. Nếu chỉ advertise một địa chỉ, một trong hai phía sẽ chết. Giải pháp: **2 listener trên 2 port**:

| Listener | Port | Ai dùng | Advertise thành |
|----------|------|---------|------------------|
| `PLAINTEXT` | 29092 | Container nội bộ (Flink, Debezium, FastAPI) | `broker:29092` |
| `PLAINTEXT_HOST` | 9092 | Process trên host (Airflow DAG chạy local) | `localhost:9092` |

Đây là lý do trong `.env` có `KAFKA_BOOTSTRAP_SERVERS=broker:29092` (cho container) còn `kafka_stream.py` (chạy trên host qua Airflow) lại dùng `localhost:9092`.

```yaml
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```

Các topic nội bộ (lưu offset, transaction state) mặc định replication factor 3 — cluster 1 node phải hạ xuống 1 không thì Kafka không tạo được topic nội bộ và treo.

### 2.3. Topic được tạo ở đâu?

Service `connector-init` trong compose:

```bash
kafka-topics --bootstrap-server broker:29092 --create --if-not-exists \
  --topic users_created --partitions 1 --replication-factor 1
```

Topic `postgres.public.users` thì **Debezium tự tạo** khi có event đầu tiên (Kafka bật auto-create topic mặc định). Tên topic CDC theo công thức: `<topic.prefix>.<schema>.<table>`.

---

## 3. Luồng ingest 1: FastAPI → Kafka

File: `ingestion/api/main.py` — chỉ 45 dòng, là producer đơn giản nhất trong repo.

```python
_kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "broker:29092").split(",")

producer = KafkaProducer(
    bootstrap_servers=_kafka_servers,
    max_block_ms=5000,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
```

Giải thích từng tham số:

- `bootstrap_servers`: địa chỉ khởi đầu để "làm quen" với cluster (xem cơ chế 2 bước ở mục 2.2). `.split(",")` vì env var có thể chứa nhiều broker phân cách bởi dấu phẩy.
- `max_block_ms=5000`: nếu Kafka chết, lệnh `send()` chỉ chờ tối đa 5 giây rồi ném lỗi — không để HTTP request treo vô hạn.
- `value_serializer`: Kafka chỉ nhận **bytes**. Hàm này tự động biến dict Python → JSON string → bytes mỗi lần send. Nhờ nó, chỗ gọi chỉ cần `producer.send("users_created", user.model_dump())`.
- Producer tạo **1 lần ở module level**, không tạo mỗi request — tạo producer tốn kém (mở TCP connection, fetch metadata).

```python
class UserIn(BaseModel):
    first_name: str
    ...
    email: EmailStr
    picture: str = ""
```

Pydantic model = **validate tại cửa**. Request thiếu field hoặc email sai định dạng → FastAPI tự trả 422, message rác không bao giờ lọt vào Kafka. Nguyên tắc: chặn data bẩn càng sớm càng rẻ.

```python
@app.post("/users", status_code=201)
def create_user(user: UserIn):
    try:
        future = producer.send("users_created", user.model_dump())
        future.get(timeout=5)
    except KafkaError as e:
        raise HTTPException(status_code=502, detail=f"Kafka error: {e}")
```

Chi tiết quan trọng nhất: `producer.send()` là **bất đồng bộ** — nó chỉ bỏ message vào buffer trong RAM rồi return ngay một `future`. Nếu dừng ở đây và trả 201, message có thể **chưa hề đến Kafka** (Kafka chết thì mất luôn mà client vẫn tưởng thành công). `future.get(timeout=5)` **block chờ Kafka xác nhận (ack)** — chỉ khi broker báo đã ghi mới trả 201. Đánh đổi: chậm hơn vài ms nhưng không nói dối client.

`/health` endpoint tồn tại vì docker-compose healthcheck gọi `curl -f http://localhost:8000/health` để biết container sống.

### File `ingestion/dags/kafka_stream.py` — producer thứ hai (fake data)

DAG Airflow này bơm dữ liệu giả để test: gọi `https://randomuser.me/api/` mỗi giây trong 60 giây, format lại rồi bắn vào **cùng topic** `users_created`. Cấu trúc 3 hàm rất đáng học:

```
get_data()      → gọi API ngoài, trả raw JSON        (I/O)
format_data(res) → map raw JSON → dict phẳng đúng schema (pure function, dễ test)
stream_to_kafka() → vòng lặp: get → format → send      (điều phối)
```

Vòng lặp có `try/except ... continue` — API ngoài chết 1 lần không làm chết cả phiên stream. Cuối cùng `producer.flush()` ép đẩy hết message còn trong buffer trước khi thoát (không flush thì message cuối có thể mất khi process kết thúc).

Chú ý dòng `if __name__ == "__main__": stream_to_kafka()` — cho phép chạy thẳng `python kafka_stream.py` để test không cần Airflow.

---

## 4. Luồng ingest 2: CDC

### 4.1. CDC là gì và tại sao đọc WAL

**Change Data Capture** = bắt mọi INSERT/UPDATE/DELETE trong database và phát ra thành event stream.

PostgreSQL ghi mọi thay đổi vào **WAL** (Write-Ahead Log) *trước khi* áp vào bảng — đây là cơ chế crash-recovery có sẵn của Postgres. CDC tận dụng luôn: thay vì hỏi bảng "có gì mới không?" (polling), Debezium **đăng ký làm một replication client** và Postgres chủ động đẩy từng thay đổi sang. Kết quả:

- Không miss DELETE (polling theo `updated_at` không thấy row đã xoá).
- Độ trễ mili-giây.
- Không tốn query lên bảng chính.

### 4.2. Config từng dòng

**`docker-compose.yaml`** — Postgres phải bật chế độ WAL chi tiết:

```yaml
command: postgres -c wal_level=logical
```

Mặc định `wal_level=replica` chỉ đủ cho physical replication (copy byte). `logical` ghi thêm thông tin để **decode ra từng row thay đổi** — không có dòng này Debezium không chạy được.

**`storage/postgres/init.sql`** — mount vào `/docker-entrypoint-initdb.d/` nên tự chạy **một lần duy nhất khi volume Postgres còn trống**. Nó tạo bảng `users (id, name, email, department)` và insert 3 dòng mẫu.

**`cdc/postgres-connector.json`** — được `connector-init` POST lên Kafka Connect REST API (`http://kafka-connect:8083/connectors`):

```json
"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
"database.hostname": "postgres",        // hostname trong docker network
"topic.prefix": "postgres",             // tên topic = postgres.<schema>.<table>
"table.include.list": "public.users",   // chỉ theo dõi bảng này
"plugin.name": "pgoutput",              // plugin decode WAL có sẵn trong PG10+, không cần cài thêm
"slot.name": "debezium_slot",           // replication slot — PG giữ WAL lại cho slot này
                                        // đến khi Debezium xác nhận đã đọc (Debezium chết vẫn không mất)
"value.converter.schemas.enable": "false"  // QUAN TRỌNG: tắt phần "schema" dài dòng trong mỗi message,
                                           // chỉ gửi payload JSON gọn — Flink parse dễ hơn nhiều
```

### 4.3. Định dạng message Debezium — phải thuộc lòng

Mỗi thay đổi thành một JSON có 4 phần chính:

```json
{
  "before": null,                                    // trạng thái TRƯỚC (null nếu INSERT)
  "after":  {"id": 4, "name": "David", "email": "d@x.com", "department": "Sales"},
  "op": "c",                                         // c=create, u=update, d=delete, r=read(snapshot ban đầu)
  "ts_ms": 1719900000000,                            // thời điểm thay đổi (epoch millis)
  "source": { ... }                                  // metadata (lsn, table, ...)
}
```

- Khi connector khởi động lần đầu, nó **snapshot** toàn bộ bảng — 3 row mẫu trong init.sql sẽ thành 3 event `op = "r"`.
- DELETE có `after = null` — vì thế Flink filter `WHERE after IS NOT NULL` (xem mục 6).
- Spark Silver filter `op IN ('c','u','r')` — đọc lại mục 7 để thấy hai filter này khớp nhau.

---

## 5. Iceberg + MinIO + REST Catalog

### 5.1. Vấn đề Iceberg giải quyết

MinIO (S3) chỉ biết **object** — những file bytes có tên. Đặt file Parquet lên S3, bạn có "đống file", không phải "bảng":

- Không UPDATE/DELETE từng dòng (S3 object là immutable).
- Không transaction: writer đang ghi đè, reader đọc nửa chừng → data rác.
- Không schema: mỗi file có thể mỗi kiểu.
- "Bảng gồm những file nào?" phải LIST cả thư mục — chậm và không nhất quán.

**Iceberg là một lớp metadata đặt lên trên đống file đó**, biến chúng thành bảng thật sự:

```
metadata.json  (root — trạng thái bảng tại 1 thời điểm: schema, snapshot hiện tại)
   └── manifest list  (snapshot này gồm những manifest nào)
        └── manifest file  (liệt kê data file + thống kê min/max từng cột)
             └── data files  (.parquet — data thật)
```

Cơ chế then chốt: **mọi file đều immutable, chỉ ghi thêm file mới**. Một lần "commit" = ghi data file mới + manifest mới + metadata.json mới, rồi **đổi con trỏ** "metadata mới nhất" sang file mới — thao tác đổi con trỏ là atomic. Hệ quả:

- **ACID**: reader luôn thấy một snapshot toàn vẹn — hoặc trước commit, hoặc sau, không bao giờ nửa chừng.
- **Time-travel**: metadata cũ còn nguyên → query được trạng thái bảng ở quá khứ.
- **MERGE INTO / UPDATE**: engine viết lại các file bị ảnh hưởng thành file mới + commit — với người dùng trông như update từng dòng.

### 5.2. Catalog — tại sao cần thêm một server nữa?

Câu hỏi còn lại: **"con trỏ metadata mới nhất của bảng X đang là file nào?"** — ai giữ? Đó là việc của **catalog**. Repo dùng `tabulario/iceberg-rest` — một HTTP server nhỏ, mọi engine hỏi/cập nhật con trỏ qua REST API:

```
Flink  ─┐
Spark  ─┼──HTTP──▶  iceberg-rest:8181  ──"bảng bronze.api_users_raw
Trino  ─┘                                 → metadata file s3://warehouse/bronze/api_users_raw/metadata/00042.json"
```

Nhờ một nguồn sự thật duy nhất, Flink ghi xong là Spark/Trino **thấy ngay**, và hai writer ghi đồng thời sẽ được catalog phát hiện xung đột (optimistic concurrency).

Config của nó trong compose:

```yaml
CATALOG_WAREHOUSE: s3://warehouse/            # thư mục gốc chứa mọi bảng
CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO   # đọc/ghi file bằng AWS SDK
CATALOG_S3_ENDPOINT: http://minio:9000        # trỏ về MinIO thay vì AWS thật
CATALOG_S3_PATH__STYLE__ACCESS: "true"        # xem giải thích dưới
```

**`path-style-access: true`** xuất hiện ở *mọi* service (Flink, Spark, Trino, iceberg-rest) — vì AWS thật dùng URL kiểu `https://bucket.s3.amazonaws.com/key` (bucket nằm trong **subdomain**), còn MinIO local không có DNS wildcard nên phải dùng kiểu `http://minio:9000/bucket/key` (bucket nằm trong **path**). Quên dòng này là lỗi `UnknownHostException: warehouse.minio`.

**`s3.region` / `client.region`**: AWS SDK v2 **bắt buộc** có region dù MinIO không quan tâm giá trị — thiếu là chết ngay lúc khởi tạo client. Đây là lý do 2 commit fix gần đây trong git history của repo này tồn tại.

### 5.3. Mapping tên bảng → đường đi thực tế

Khi bạn viết `iceberg.bronze.api_users_raw` ở bất kỳ engine nào:

| Phần | Nghĩa |
|------|-------|
| `iceberg` | tên **catalog** (do bạn tự đặt khi config engine) |
| `bronze` | namespace/database trong catalog |
| `api_users_raw` | bảng — catalog trả về đường dẫn metadata trên `s3://warehouse/bronze/api_users_raw/` |

---

## 6. Flink

File: `processing/flink/user_processor.py`. Đây là job **chạy mãi mãi** (streaming) — submit một lần, nó đọc Kafka liên tục cho đến khi bạn cancel.

### 6.1. Mô hình tư duy: Flink SQL = luồng chảy qua các "bảng"

Flink Table API cho phép mô tả stream bằng SQL. "Bảng" Kafka trong Flink không phải bảng tĩnh — nó là **stream đội lốt bảng**: mỗi message mới đến = một row mới "xuất hiện" trong bảng. Câu `INSERT INTO sink SELECT ... FROM source` vì thế không chạy một lần rồi xong, mà là một **pipeline liên tục**: row nào chảy vào source thì được transform và đổ vào sink.

### 6.2. Đi từng khối code

```python
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.enable_checkpointing(30000)
```

- `set_parallelism(1)`: mỗi operator chạy 1 bản — đủ cho demo, production tăng số này để scale.
- `enable_checkpointing(30000)`: **dòng quan trọng nhất file**. Mỗi 30 giây, Flink chụp lại toàn bộ trạng thái (đặc biệt là **Kafka offset đang đọc đến đâu**). Với Iceberg sink, checkpoint còn quyết định nhịp ghi: **data chỉ được COMMIT vào Iceberg tại mỗi checkpoint**. Hai hệ quả bạn phải nhớ:
  1. Gửi message vào Kafka xong phải **đợi tối đa ~30s** mới thấy trong Bronze — không phải lỗi, là thiết kế.
  2. **Exactly-once**: offset Kafka và commit Iceberg được chốt cùng nhau trong một checkpoint. Flink crash giữa chừng → restore về checkpoint gần nhất → đọc lại từ offset đó → data chưa commit thì ghi lại, đã commit thì không lặp. Không mất, không trùng.
  3. Không bật checkpoint = Iceberg sink **không bao giờ commit** = bảng trống vĩnh viễn dù job chạy. Đây là bug kinh điển của người mới.

```python
t_env.execute_sql(f"""
    CREATE CATALOG iceberg WITH (
        'type' = 'iceberg',
        'catalog-type' = 'rest',
        'uri' = '{ICEBERG_REST_URI}',
        'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
        's3.endpoint' = '{MINIO_ENDPOINT}', ...
    )
""")
```

Khai báo với Flink: "có một catalog tên `iceberg`, metadata hỏi REST server, file đọc/ghi qua S3FileIO trỏ vào MinIO". Sau dòng này, mọi tên `iceberg.xxx.yyy` đều được resolve qua catalog đó.

```python
CREATE TABLE IF NOT EXISTS api_source (
    first_name STRING, ... , picture STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'users_created',
    'properties.group.id' = 'flink-api-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
```

- Bảng này tạo trong `default_catalog` (in-memory của Flink, mất khi job tắt) — nó chỉ là **định nghĩa cách đọc topic**, không phải bảng lưu trữ.
- Schema các cột phải **khớp tên field trong JSON message** — Flink map theo tên, field thừa bị bỏ qua, field thiếu thành NULL.
- `scan.startup.mode = earliest-offset`: lần đầu chạy (group chưa có offset) thì đọc từ **đầu topic** — để không bỏ sót message gửi trước khi job start. Các lần restart sau, offset từ checkpoint được ưu tiên.
- `'format' = 'json'`: Flink tự parse bytes → JSON → cột.

Bảng CDC source thú vị hơn:

```python
CREATE TABLE IF NOT EXISTS cdc_source (
    after ROW<id INT, name STRING, email STRING, department STRING>,
    op     STRING,
    ts_ms  BIGINT
) ...
    'json.ignore-parse-errors' = 'true'
```

- `after ROW<...>`: JSON của Debezium có object lồng nhau (`{"after": {"id": ...}}`) → khai báo kiểu `ROW` để truy cập `after.id`. Không cần khai `before`/`source` vì không dùng — Flink chỉ parse cột được khai.
- `json.ignore-parse-errors = true`: message rác không làm **chết cả job streaming** (job chết là dừng ingest toàn hệ thống) — đánh đổi: message hỏng bị bỏ lặng lẽ.

```python
CREATE TABLE IF NOT EXISTS iceberg.bronze.api_users_raw ( ..., ingested_at TIMESTAMP(3) )
WITH ('format-version' = '2', 'write.format.default' = 'parquet')
```

- Tên có tiền tố `iceberg.` → tạo trong catalog Iceberg → **bảng thật, bền vững** trên MinIO (khác 2 bảng source ở trên).
- `format-version = 2` cần cho các tính năng mới của Iceberg (row-level delete — Spark MERGE cần).
- Cột `ingested_at` **không có trong message nguồn** — được thêm lúc INSERT. Đây là pattern Bronze chuẩn: giữ data nguyên xi + đóng dấu thời gian vào. Cột này về sau thành **watermark** cho Spark incremental (mục 7).

```python
stmt_set = t_env.create_statement_set()
stmt_set.add_insert_sql("""INSERT INTO iceberg.bronze.api_users_raw SELECT ..., CURRENT_TIMESTAMP FROM ...api_source""")
stmt_set.add_insert_sql("""INSERT INTO iceberg.bronze.cdc_users_raw SELECT after.id, ..., op, ts_ms, CURRENT_TIMESTAMP FROM ...cdc_source WHERE after IS NOT NULL""")
stmt_set.execute()
```

- **Statement set** = gộp 2 câu INSERT vào **một job Flink duy nhất** (một job 2 nhánh chạy song song). Nếu `execute_sql` từng câu riêng thì thành 2 job, tốn tài nguyên và quản lý mệt hơn.
- `after.id` — cú pháp truy cập field trong ROW.
- `WHERE after IS NOT NULL` — loại event DELETE (DELETE có `after = null`, INSERT các cột null hết vào Bronze cũng vô nghĩa).
- Nhánh CDC giữ nguyên `op` và `ts_ms` — Bronze không phán xét, chỉ ghi lại; việc diễn giải "op nghĩa là gì" để dành cho Silver.
- `stmt_set.execute()` **không block** — nó submit job lên cluster rồi script Python kết thúc, job vẫn chạy tiếp trên Flink cluster. Xem job tại Flink UI (localhost:18081).

### 6.3. Dockerfile Flink — tại sao lắm JAR thế?

Flink core không biết Kafka hay Iceberg là gì. Mỗi connector là một JAR đặt vào `/opt/flink/lib/`:

| JAR | Để làm gì |
|-----|-----------|
| `flink-sql-connector-kafka` | `'connector' = 'kafka'` hoạt động |
| `iceberg-flink-runtime-1.17-1.5.2` | `CREATE CATALOG ... type=iceberg` hoạt động |
| `iceberg-aws-bundle` | S3FileIO — nói chuyện với MinIO qua AWS SDK |
| `hadoop-common` + 5 JAR hadoop | Iceberg FlinkCatalogFactory có dependency cứng vào vài class Hadoop dù ta không dùng HDFS — phải nhét vào cho đủ classpath |

Các dòng `security.delegation.tokens.enabled: false` trong conf: tắt cơ chế Kerberos/Hadoop security mà Flink tự động khởi động khi thấy JAR hadoop trên classpath — không tắt là crash lúc start vì không có cấu hình Kerberos. Đây là loại "trận chiến dependency" rất thực tế khi tự dựng stack.

---

## 7. Spark

Hai job batch, chạy xong là thoát (khác Flink chạy mãi): `silver_transform.py` (incremental MERGE) và `gold_transform.py` (full rebuild).

### 7.1. `spark_session.py` — cấu hình dùng chung

```python
SparkSession.builder
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
```

Extension này dạy Spark SQL các câu lệnh Iceberg-riêng: **`MERGE INTO`**, `UPDATE`, `DELETE`, các procedure. Thiếu nó, `MERGE INTO` báo syntax error.

```python
  .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.iceberg.type", "rest")
  .config("spark.sql.catalog.iceberg.uri", ICEBERG_REST_URI)
```

Cú pháp `spark.sql.catalog.<tên>` đăng ký catalog tên `iceberg` — **cùng REST server với Flink**, nên Spark thấy ngay các bảng Flink tạo. Các config `s3.*` giống hệt Flink (endpoint MinIO, path-style, region).

Tách file này riêng vì cả 2 job cùng cần — và khi submit phải kèm `--py-files /opt/spark/jobs/spark_session.py` để Spark ship file này đến các worker (executor chạy trên máy khác không tự thấy file của bạn).

### 7.2. `silver_transform.py` — đọc kỹ từng bước

Cả 2 hàm (`transform_api_users`, `transform_cdc_users`) cùng một khung 5 bước. Nắm khung này là bạn viết được cho bất kỳ nguồn nào:

**Bước 1 — Watermark: chỉ lấy phần mới**

```python
watermark = (
    spark.table("iceberg.silver.api_users")
    .agg(F.max("ingested_at").alias("max_ts"))
    .collect()[0]["max_ts"]
)
...
if watermark:
    bronze = bronze.filter(F.col("ingested_at") > watermark)
```

Logic: "Silver đã xử lý đến record có `ingested_at` = T → lần này chỉ đọc Bronze phần `> T`". Đây là **incremental processing** — Bronze lớn dần vô hạn, nhưng mỗi lần chạy chỉ đụng phần mới, thời gian chạy không tăng theo tuổi hệ thống.

- `try/except → watermark = None`: lần chạy **đầu tiên** bảng Silver chưa tồn tại, `spark.table()` ném exception → xử lý toàn bộ Bronze (full load lần đầu).
- `.collect()[0]["max_ts"]` — `agg` trả về DataFrame 1 dòng; `collect()` kéo về driver thành list Row, lấy giá trị scalar.
- `if bronze.isEmpty(): return` — không có gì mới thì thoát sớm, đừng chạy MERGE rỗng tốn công.

**Bước 2 — Làm sạch (chuẩn hoá)**

```python
.withColumn("email",      F.lower(F.trim("email")))
.withColumn("username",   F.lower(F.trim("username")))
.withColumn("full_name",  F.concat_ws(" ", F.trim("first_name"), F.trim("last_name")))
.withColumn("birth_year", F.substring("dob", 1, 4).cast(T.IntegerType()))
```

Mọi transform ở đây trả lời một câu hỏi: **"hai record cùng một người có so sánh được với nhau không?"** `"Hoang@X.com"` và `"hoang@x.com "` phải thành một. `dob` là string ISO `"1995-01-15T..."` → cắt 4 ký tự đầu, cast INT thành năm sinh. Lưu ý DataFrame là **immutable** — mỗi `.withColumn` trả về DataFrame mới, và tất cả là **lazy**: chưa có gì chạy cho đến khi gặp action (`count()`, `collect()`, MERGE).

**Bước 3 — Dedup trong batch**

```python
.withColumn("_rank", F.row_number().over(
    Window.partitionBy("username").orderBy(F.col("ingested_at").desc())
))
.filter(F.col("_rank") == 1)
.drop("_rank")
```

Đây là **idiom dedup kinh điển của Spark — thuộc lòng nó**: chia data theo key (`partitionBy("username")`), trong mỗi nhóm sắp theo thời gian giảm dần, đánh số 1,2,3..., giữ số 1 = **bản mới nhất của mỗi key**. Cần bước này vì trong một batch 15 phút, cùng một user có thể xuất hiện nhiều lần — mà MERGE sẽ lỗi (`multiple source rows matched`) nếu 2 dòng source cùng khớp 1 dòng target.

Với CDC thì `partitionBy("id").orderBy(F.col("source_ts").desc())` — dùng **thời gian của database** (nguồn sự thật) chứ không phải thời gian ingest.

Riêng CDC có thêm filter nghiệp vụ trước đó:

```python
bronze = bronze.filter(F.col("op").isin("c", "u", "r") & F.col("id").isNotNull())
```

Nhận create/update/snapshot, **bỏ delete** — quyết định thiết kế: Silver là "trạng thái hiện tại của những user còn tồn tại". (Muốn xử lý delete tử tế thì MERGE thêm nhánh `WHEN MATCHED AND s.op = 'd' THEN DELETE` — bài tập tự làm.)

**Bước 4 — Tạo bảng nếu chưa có**

```sql
CREATE TABLE IF NOT EXISTS iceberg.silver.api_users (...) USING iceberg
TBLPROPERTIES ('format-version' = '2')
```

Khai schema tường minh (không để MERGE tự suy) và `format-version=2` để MERGE row-level hoạt động hiệu quả.

**Bước 5 — MERGE INTO (upsert)**

```python
silver.createOrReplaceTempView("silver_api_updates")
spark.sql("""
    MERGE INTO iceberg.silver.api_users t
    USING silver_api_updates s ON t.username = s.username
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

- `createOrReplaceTempView` = đặt tên SQL cho DataFrame để trộn Python API với SQL.
- MERGE = "so từng dòng source với target theo key: **trùng thì UPDATE, chưa có thì INSERT**" — một câu, atomic (một commit Iceberg, reader không bao giờ thấy nửa chừng).
- `UPDATE SET *` / `INSERT *` = map mọi cột theo tên (source và target phải khớp schema).
- Chọn key: API users → `username`; CDC users → `id` (primary key gốc).

Kết quả sau nhiều lần chạy: bảng Silver luôn có **đúng 1 dòng / 1 user, là bản mới nhất** — dù Bronze chứa 50 bản ghi lịch sử của user đó.

### 7.3. `gold_transform.py` — tư duy khác hẳn Silver

Gold **không incremental, không MERGE** — mỗi lần chạy **đập đi xây lại**:

```python
spark.sql("DROP TABLE IF EXISTS iceberg.gold.users_enriched")
enriched.writeTo("iceberg.gold.users_enriched").using("iceberg").create()
```

Tại sao rebuild toàn bộ mà không incremental như Silver? Vì Gold là **kết quả của JOIN + aggregate** — một record Silver mới có thể thay đổi *nhiều* dòng aggregate cũ (một user Engineering mới → dòng stats của Engineering phải tính lại). Update incremental cho aggregate rất phức tạp và dễ sai; khi data còn nhỏ, **rebuild là lựa chọn đơn giản-mà-đúng**. Trade-off ghi nhớ: Silver = data lớn, có key rõ ràng → incremental MERGE; Gold = data đã cô đặc, logic phức tạp → full rebuild.

**Bảng 1 — `users_enriched`: JOIN 2 nguồn**

```python
api.alias("a").join(
    cdc.alias("c"),
    on=F.lower(F.col("a.email")) == F.lower(F.col("c.email")),
    how="left",
)
```

- `how="left"`: giữ **mọi** user từ API; user nào khớp email với bảng CDC thì được "làm giàu" thêm `department`, `db_id`; không khớp thì các cột đó NULL. Dùng `inner` sẽ **mất** user không có trong database — sai nghiệp vụ.
- `alias("a")`/`alias("c")` để phân biệt cột trùng tên (`email` có ở cả 2 bảng).
- Join theo `lower(email)` — phòng thủ 2 lớp dù Silver đã lowercase.

**Bảng 2 — `user_stats`: aggregate**

```python
.groupBy(
    F.coalesce("gender", F.lit("unknown")).alias("gender"),
    F.coalesce("department", F.lit("unknown")).alias("department"),
    "birth_year",
)
.agg(
    F.count("*").alias("total_users"),
    F.countDistinct("email").alias("unique_emails"),
    F.count("db_id").alias("matched_db_users"),   # count(col) bỏ qua NULL → đếm user khớp DB
    ...
)
```

- `coalesce(col, "unknown")`: NULL trong groupBy tạo thành nhóm khó đọc — thay bằng nhãn tường minh.
- Mẹo tinh tế: `F.count("db_id")` chỉ đếm dòng **không NULL** — tức là đếm số user match được với database sau left join. Đây là cách đo "tỉ lệ khớp giữa 2 nguồn" chỉ bằng một count.

### 7.4. Lệnh submit

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \        # gửi lên cluster (không chạy local trong container)
  --py-files /opt/spark/jobs/spark_session.py \  # ship module dùng chung đến worker
  /opt/spark/jobs/silver_transform.py
```

---

## 8. Trino

Trino là query engine **không lưu data** — chỉ đọc. Toàn bộ tích hợp nằm trong một file properties:

```properties
# query/trino/etc/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=${ENV:ICEBERG_REST_URI}
fs.native-s3.enabled=true
s3.endpoint=${ENV:MINIO_ENDPOINT}
s3.path-style-access=true
```

Quy ước của Trino: **mỗi file trong `etc/catalog/` = một catalog, tên file = tên catalog** → `iceberg.properties` sinh ra catalog `iceberg`, query dạng `iceberg.<schema>.<table>`. `${ENV:...}` đọc từ biến môi trường container. Vì cùng trỏ vào REST catalog, Trino thấy mọi bảng mà Flink/Spark tạo — ngay lập tức, không cần "refresh".

```sql
-- vào CLI
docker exec -it trino trino

SELECT * FROM iceberg.gold.user_stats;
-- time-travel: bảng này trông thế nào 1 giờ trước?
SELECT * FROM iceberg.silver.api_users FOR TIMESTAMP AS OF current_timestamp - INTERVAL '1' HOUR;
-- soi lịch sử commit
SELECT * FROM iceberg.bronze."api_users_raw$snapshots";
```

Vai trò trong kiến trúc: Flink/Spark là "nhà bếp" (ghi), Trino là "quầy phục vụ" (đọc) — analyst chỉ cần SQL, không đụng vào code xử lý.

---

## 9. Airflow

`ingestion/dags/spark_pipeline.py` — Airflow **không xử lý data**, chỉ là "người bấm nút đúng giờ, đúng thứ tự":

```python
with DAG(
    "spark_medallion_pipeline",
    schedule="*/15 * * * *",   # cron: phút 0,15,30,45 mỗi giờ
    start_date=datetime(2026, 1, 1),
    catchup=False,             # KHÔNG chạy bù các kỳ đã lỡ trong quá khứ
) as dag:
    silver = BashOperator(task_id="spark_silver", bash_command="docker exec spark-master spark-submit ... silver_transform.py")
    gold   = BashOperator(task_id="spark_gold",   bash_command="docker exec spark-master spark-submit ... gold_transform.py")

    silver >> gold   # dependency: silver THÀNH CÔNG mới chạy gold
```

- `catchup=False` quan trọng: không có nó, bật Airflow lên sau 1 tuần nghỉ là nó chạy bù 672 lần.
- `silver >> gold`: Gold đọc Silver, nên bắt buộc tuần tự. Silver fail → Gold **không chạy** → Gold không bị build từ data dở dang.
- Vì Airflow chạy trên **host** (không trong compose), nó phải `docker exec` vào container spark-master để submit — nếu Airflow cũng ở trong compose thì thay bằng SparkSubmitOperator hoặc gọi thẳng.
- `kafka_stream.py` (DAG `user_automation`) là DAG thứ hai: bơm fake data `@daily`, cũng chạy được bằng tay để test.

---

## 10. Lộ trình tự viết lại bằng tay

Nguyên tắc: **xây theo chiều data chảy, mỗi bước phải TEST XONG mới sang bước sau**. Đừng viết cả docker-compose một lần — debug 12 service cùng lúc là ác mộng.

### Giai đoạn 1 — Kafka + Producer (nửa ngày)

1. Viết compose chỉ có `broker` (chép phần KRaft config — phần này ai cũng chép, không ai viết tay từ trí nhớ; nhưng phải **giải thích được** dual listener).
2. Test: `docker exec broker kafka-topics --bootstrap-server broker:29092 --list`.
3. Viết FastAPI producer (45 dòng — viết tay được). Test: POST rồi đọc lại bằng
   `docker exec broker kafka-console-consumer --bootstrap-server broker:29092 --topic users_created --from-beginning`.
   **Console consumer là công cụ debug số 1 của bạn suốt dự án.**

### Giai đoạn 2 — CDC (nửa ngày)

4. Thêm `postgres` (nhớ `wal_level=logical`) + `init.sql`.
5. Thêm `kafka-connect` (image debezium), viết `postgres-connector.json`, POST bằng curl tay trước khi tự động hoá:
   `curl -X POST localhost:8083/connectors -H 'Content-Type: application/json' -d @cdc/postgres-connector.json`
6. Test: `INSERT` một dòng vào Postgres → console-consumer topic `postgres.public.users` phải hiện event trong ~1s. Nhìn kỹ JSON để thuộc format `before/after/op/ts_ms`.

### Giai đoạn 3 — MinIO + Iceberg REST (1-2 giờ)

7. Thêm `minio`, `minio-init` (tạo bucket), `iceberg-rest`.
8. Test: `curl localhost:8181/v1/config` trả JSON là sống; mở console MinIO :9001 thấy bucket `warehouse`.

### Giai đoạn 4 — Flink (1 ngày — khó nhất)

9. Viết Dockerfile Flink. Phần JAR: hiểu **vai trò từng nhóm** (kafka connector / iceberg runtime / aws bundle / hadoop deps) rồi chép version — đây là phần trial-and-error nhiều nhất, lỗi `ClassNotFoundException` nào cũng có nghĩa "thiếu JAR gì đó".
10. Viết `user_processor.py` — viết tay được nếu nhớ khung: env + checkpoint → CREATE CATALOG → CREATE source (kafka) → CREATE sink (iceberg) → statement_set INSERT. Bắt đầu với **một pipeline API** trước, chạy được rồi mới thêm nhánh CDC.
11. Submit: `docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/user_processor.py`. Mở UI :18081 xem job RUNNING, đợi 30s (checkpoint!), kiểm tra file xuất hiện trong MinIO.

### Giai đoạn 5 — Spark (1 ngày)

12. Dockerfile Spark (2 JAR thôi — dễ hơn Flink nhiều) + `spark-master`/`spark-worker` trong compose.
13. Viết `spark_session.py` (khung config catalog giống Flink, đổi cú pháp).
14. Viết `silver_transform.py` theo khung 5 bước: **watermark → clean → dedup (row_number) → create table → MERGE**. Test từng đoạn bằng `pyspark` shell tương tác trước khi ghép thành job.
15. Viết `gold_transform.py`: left join + groupBy/agg + drop-and-recreate.
16. Test: chạy Silver 2 lần liên tiếp — lần 2 phải in "Không có data mới" (watermark hoạt động). Gửi thêm user qua API, đợi 30s, chạy lại — user mới phải vào Silver, user cũ không nhân đôi (MERGE hoạt động).

### Giai đoạn 6 — Trino + Airflow (nửa ngày)

17. Bốn file config Trino (chép được, chỉ cần hiểu file-name = catalog-name).
18. DAG Airflow ~50 dòng: 2 BashOperator + `silver >> gold`.

### Kiểm tra cuối: bạn "tự viết được" khi nào?

Viết tay 100% không tra cứu là mục tiêu sai — dân chuyên nghiệp cũng chép config Kafka listener và version JAR. Cái bạn phải **tự viết được từ trí nhớ** là:

- ✅ Sơ đồ kiến trúc + lý do tồn tại của từng thành phần (bảng ở mục 1)
- ✅ FastAPI producer hoàn chỉnh
- ✅ Khung Flink job (catalog → source → sink → insert) 
- ✅ Khung Silver 5 bước, đặc biệt idiom `row_number().over(Window...)` và câu MERGE INTO
- ✅ Khung Gold (join + agg)
- ✅ Format message Debezium và ý nghĩa `op`
- ✅ Chuỗi lệnh test end-to-end (POST → console-consumer → Trino query)

Còn lại (listener config, JAR versions, hadoop workarounds) — hiểu **tại sao cần**, tra cứu **giá trị cụ thể**.

---

## 11. Câu hỏi tự kiểm tra

Trả lời trôi chảy hết là bạn đã "tường tận". Đáp án đều nằm trong tài liệu này.

**Kafka**
1. Tại sao broker cần 2 listener 29092 và 9092? Điều gì xảy ra nếu client trên host connect vào `broker:29092`?
2. Hai consumer group khác nhau đọc cùng topic thì thấy data thế nào?

**CDC**
3. Tại sao phải `wal_level=logical`? So với polling bảng theo `updated_at`, CDC hơn ở đâu?
4. `op` có những giá trị nào? Event DELETE khác gì event UPDATE về cấu trúc?
5. Replication slot để làm gì khi Debezium chết 1 giờ rồi sống lại?

**Flink**
6. Tại sao gửi message xong phải đợi ~30 giây mới thấy trong Bronze?
7. Nếu quên `enable_checkpointing` thì chuyện gì xảy ra với bảng Iceberg?
8. Flink crash lúc 12:00:15, checkpoint gần nhất lúc 12:00:00 — sau khi restart, data từ 12:00:00–12:00:15 bị mất, bị trùng, hay không sao? Tại sao?
9. Bảng `api_source` và bảng `iceberg.bronze.api_users_raw` khác nhau về bản chất thế nào?

**Iceberg**
10. S3 không cho sửa file, vậy MERGE INTO "update" bằng cách nào?
11. REST catalog giữ thông tin gì? Nếu nó chết, data trong MinIO có mất không?
12. Tại sao Spark thấy ngay bảng Flink vừa tạo mà không cần config gì thêm?

**Spark**
13. Watermark trong `silver_transform.py` là gì (không phải watermark của streaming!)? Lần chạy đầu tiên watermark bằng bao nhiêu và code xử lý ra sao?
14. Viết lại từ trí nhớ idiom dedup giữ bản ghi mới nhất theo key.
15. Tại sao phải dedup **trước khi** MERGE? Lỗi gì xảy ra nếu không?
16. Tại sao Silver dùng incremental MERGE còn Gold lại drop-and-rebuild?
17. `F.count("db_id")` sau left join đếm cái gì?

**Tổng hợp**
18. Kể tên đường đi đầy đủ của một user POST vào API cho đến khi xuất hiện trong `iceberg.gold.users_enriched` — qua những process nào, độ trễ mỗi chặng?
19. Nếu logic clean ở Silver bị phát hiện sai sau 1 tháng, làm sao sửa lại toàn bộ data mà không mất gì? (gợi ý: Bronze append-only)
