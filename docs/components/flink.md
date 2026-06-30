# Apache Flink

## Vai trò

Flink là **stream processing engine** — đọc liên tục từ Kafka và ghi vào Bronze layer (Iceberg). Chạy 24/7 như một long-running job.

## Cấu hình

| Tham số | Giá trị | Ý nghĩa |
|---------|---------|---------|
| Parallelism | 1 | 1 task slot xử lý tuần tự (đủ cho dev) |
| Checkpoint interval | 30,000 ms | Iceberg commit sau mỗi checkpoint |
| Flink version | 1.17.2 | Version cuối hỗ trợ Iceberg 1.5.x |
| Scala | 2.12 | |
| Java | 11 | |

## JARs bắt buộc

| JAR | Mục đích |
|-----|---------|
| `flink-sql-connector-kafka-1.17.2.jar` | Đọc từ Kafka |
| `iceberg-flink-runtime-1.17-1.5.2.jar` | Iceberg sink cho Flink |
| `iceberg-aws-bundle-1.5.2.jar` | S3FileIO — ghi vào MinIO |
| `hadoop-common-3.3.4.jar` | Iceberg FlinkCatalogFactory cần HdfsConfiguration |
| `hadoop-hdfs-client-3.3.4.jar` | Hadoop HDFS client |
| `hadoop-auth-3.3.4.jar` | Hadoop auth |
| `hadoop-shaded-guava-1.1.1.jar` | Transitive dep của Hadoop |
| `woodstox-core-6.2.4.jar` | XML parser cho Hadoop config |
| `stax2-api-4.2.1.jar` | Transitive dep của Woodstox |

> `iceberg-aws-bundle` bắt buộc để tránh classloader conflict với `flink-s3-fs-hadoop`. Không dùng `s3a://`, chỉ dùng `s3://` với S3FileIO.

## Iceberg Catalog config

```python
t_env.execute_sql("""
    CREATE CATALOG iceberg WITH (
        'type'                 = 'iceberg',
        'catalog-type'         = 'rest',
        'uri'                  = 'http://iceberg-rest:8181',
        'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
        's3.endpoint'          = 'http://minio:9000',
        's3.access-key-id'     = 'minio',
        's3.secret-access-key' = 'minio123',
        's3.path-style-access' = 'true'
    )
""")
```

## Flink security config (quan trọng)

Flink 1.17 khởi động `HadoopFSDelegationTokenProvider` ngay khi start — gọi `UserGroupInformation` tĩnh. Nếu thiếu Kerberos, UGI init fail → `NoClassDefFoundError` crash toàn bộ JobManager.

Bắt buộc có 2 config này trong `flink-conf.yaml` và `FLINK_PROPERTIES`:

```yaml
security.module.factory.classes: org.apache.flink.runtime.security.modules.JaasModuleFactory
security.delegation.tokens.enabled: false
```

- `security.delegation.tokens.enabled: false` — tắt `DefaultDelegationTokenManager` hoàn toàn
- `security.module.factory.classes` — chỉ load JAAS module, không load Hadoop module (tránh UGI call)

## Submit job

```bash
# Submit và chạy nền (detached mode)
docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/user_processor.py

# Xem danh sách jobs đang chạy
docker exec flink-jobmanager flink list

# Cancel job
docker exec flink-jobmanager flink cancel <job-id>

# Submit lại sau khi cancel
docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/user_processor.py
```

## Web UI

http://localhost:18081

- **Overview**: số tasks, checkpoint status
- **Jobs → Running Jobs → \<job\>**: xem graph, metrics, backpressure
- **Jobs → \<job\> → Checkpoints**: xem checkpoint history, duration

## Checkpoint và Iceberg commit

```
Timeline:
  t=0s   Flink nhận event từ Kafka
  t=30s  Checkpoint trigger
  t=32s  Checkpoint hoàn thành → Flink commit Iceberg files
  t=32s  Bronze table có thể query được qua Trino

Nếu Flink crash trước checkpoint:
  - Iceberg files chưa committed → không visible
  - Flink restart từ checkpoint cuối → replay Kafka → ghi lại (có thể duplicate trong Bronze)
  - Silver MERGE INTO dedup sẽ xử lý duplicate này
```

## Troubleshooting

### Job fail sau khi submit

```bash
docker logs flink-taskmanager --tail 100
```

Lỗi thường gặp:

- `NoSuchTableException` → iceberg-rest chưa healthy hoặc MinIO chưa có bucket `warehouse`
- `Connection refused: minio:9000` → MinIO chưa healthy
- `ClassNotFoundException S3FileIO` → thiếu `iceberg-aws-bundle` JAR
- `NoClassDefFoundError: UserGroupInformation` → thiếu `security.delegation.tokens.enabled: false`

### Job chạy nhưng Bronze trống

```bash
# Vào Flink UI → Jobs → <job> → Checkpoints
# Nếu "Completed" = 0 → checkpoint chưa xảy ra lần nào
# Chờ 30s hoặc xem log lỗi checkpoint

docker logs flink-taskmanager --tail 50 | grep -i "checkpoint\|error\|exception"
```

### Restart Flink

```bash
docker compose restart flink-jobmanager flink-taskmanager

# Sau khi restart, phải submit lại job
docker exec flink-jobmanager flink run -d -py /opt/flink/jobs/user_processor.py
```
