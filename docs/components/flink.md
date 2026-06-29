# Apache Flink

## Vai trò

Flink là stream processing engine — đọc liên tục từ Kafka và ghi vào Bronze layer (Iceberg).

---

## Cấu hình

| Thông số | Giá trị | Lý do |
|----------|---------|-------|
| Parallelism | 1 | Đủ cho dev, tăng lên khi cần throughput cao |
| Checkpoint interval | 30 giây | Iceberg commit mỗi 30s, trade-off latency vs overhead |
| Kafka group.id api | `flink-api-group` | Offset tracking độc lập |
| Kafka group.id cdc | `flink-cdc-group` | Offset tracking độc lập |
| S3 file IO | `S3FileIO` (iceberg-aws-bundle) | Tránh classloader conflict với S3A plugin |

---

## Jars cần thiết

| JAR | Mục đích |
|-----|----------|
| `flink-sql-connector-kafka-1.17.2.jar` | Đọc Kafka trong Flink SQL |
| `iceberg-flink-runtime-1.17-1.5.2.jar` | Iceberg table API + REST catalog client |
| `iceberg-aws-bundle-1.5.2.jar` | S3FileIO — ghi file Parquet lên MinIO |

> **Quan trọng**: Dùng Iceberg **1.5.2** — phiên bản cuối hỗ trợ Flink 1.17. Iceberg 1.6.x đã drop Flink 1.17.

---

## Iceberg catalog config

```python
t_env.execute_sql("""
    CREATE CATALOG iceberg WITH (
        'type'                 = 'iceberg',
        'catalog-type'         = 'rest',
        'uri'                  = 'http://nessie:19120/iceberg',
        'warehouse'            = 's3://warehouse/',
        'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
        's3.endpoint'          = 'http://minio:9000',
        's3.access-key-id'     = 'minio',
        's3.secret-access-key' = 'minio123',
        's3.path-style-access' = 'true',
        'header.X-Project-Name' = 'main'
    )
""")
```

**Tại sao `s3://` không phải `s3a://`?**
S3FileIO dùng AWS SDK v2 riêng — không đi qua Hadoop FileSystem. Nó chỉ hiểu scheme `s3://`. Nếu dùng `s3a://` sẽ bị fallback sang HadoopFileIO và gây classloader conflict âm thầm.

**`header.X-Project-Name = 'main'`** — chỉ định Nessie branch. Default là `main`, đặt tường minh để tránh nhầm lẫn.

---

## Cấu trúc Pipeline trong Flink

```
api_source (Kafka)
    │
    │ INSERT INTO iceberg.bronze.api_users_raw
    │ SELECT first_name, last_name, ..., CURRENT_TIMESTAMP
    ▼
iceberg.bronze.api_users_raw

cdc_source (Kafka)
    │
    │ INSERT INTO iceberg.bronze.cdc_users_raw
    │ SELECT after.id, after.name, ..., op, ts_ms, CURRENT_TIMESTAMP
    │ WHERE after IS NOT NULL
    ▼
iceberg.bronze.cdc_users_raw
```

Hai pipeline chạy trong cùng 1 Flink job (dùng `StatementSet`) để share tài nguyên.

---

## Cách submit và quản lý job

```bash
# Submit job
docker exec flink-jobmanager \
  flink run -d -py /opt/flink/jobs/user_processor.py

# Xem job đang chạy
docker exec flink-jobmanager flink list

# Cancel job (thay <job-id> bằng ID thực)
docker exec flink-jobmanager flink cancel <job-id>

# Xem log TaskManager
docker logs flink-taskmanager 2>&1 | tail -100

# Xem log JobManager
docker logs flink-jobmanager 2>&1 | tail -50
```

---

## Truy cập Flink Web UI

URL: http://localhost:18081

Từ UI có thể thấy:
- Danh sách jobs đang running
- Task graph (topology của pipeline)
- Checkpoint history và timing
- Số records đã xử lý
- Backpressure indicators

---

## Tuning

### Tăng throughput (nhiều data hơn)

```python
env.set_parallelism(4)   # tăng từ 1 lên 4
```

Cần thêm task slots trong `docker-compose.yaml`:
```yaml
FLINK_PROPERTIES: |
  taskmanager.numberOfTaskSlots: 8
```

### Giảm latency (data xuất hiện nhanh hơn)

```python
env.enable_checkpointing(10000)  # 10 giây thay vì 30
```

Lưu ý: checkpoint nhiều hơn = overhead nhiều hơn = throughput giảm.

### Xử lý data lỗi

Hiện tại `cdc_source` có `json.ignore-parse-errors = 'true'` — Debezium message lỗi format sẽ bị skip thay vì crash job.
