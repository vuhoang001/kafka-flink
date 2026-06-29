# Kafka & Debezium

## Kafka

### Cấu hình

- **Mode**: KRaft (không cần ZooKeeper)
- **Image**: `confluentinc/cp-kafka:7.4.0`
- **Cluster ID**: `MkU3OEVBNTcwNTJENDM2Qk`
- **Replication factor**: 1 (single broker, dev only)

### Topics

| Topic | Producer | Consumer | Nội dung |
|-------|----------|----------|---------|
| `users_created` | FastAPI | Flink | JSON user từ HTTP API |
| `postgres.public.users` | Debezium | Flink | CDC events từ PostgreSQL |

### Lệnh quản lý

```bash
# List topics
docker exec broker kafka-topics \
  --bootstrap-server localhost:9092 --list

# Xem messages trong topic
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic users_created \
  --from-beginning \
  --max-messages 5

# Xem CDC messages
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic postgres.public.users \
  --from-beginning \
  --max-messages 5

# Consumer group lag (Flink lag)
docker exec broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group flink-api-group

docker exec broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group flink-cdc-group
```

## Debezium (Kafka Connect)

### Vai trò

Debezium đọc PostgreSQL **Write-Ahead Log (WAL)** để capture INSERT, UPDATE, DELETE mà không cần thay đổi application code.

Yêu cầu PostgreSQL:
- `wal_level = logical` (đã set trong docker-compose: `command: postgres -c wal_level=logical`)
- `POSTGRES_HOST_AUTH_METHOD: md5` (Debezium cần md5, không phải scram)

### Connector config

File: `cdc/postgres-connector.json`

```json
{
  "name": "postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "mydb",
    "topic.prefix": "postgres",
    "table.include.list": "public.users",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}
```

### CDC Message format

```json
{
  "after": {
    "id": 1,
    "name": "Alice Nguyen",
    "email": "alice@example.com",
    "department": "Engineering"
  },
  "op": "c",
  "ts_ms": 1719619200000
}
```

| `op` | Ý nghĩa |
|------|---------|
| `r` | Read/snapshot — initial load khi connector đăng ký lần đầu |
| `c` | Create — INSERT |
| `u` | Update — UPDATE |
| `d` | Delete — DELETE (field `after` = null) |

### Quản lý connector

```bash
# Xem trạng thái connector
curl -s http://localhost:8083/connectors/postgres-connector/status \
  | python3 -m json.tool

# Xóa và đăng ký lại (nếu connector lỗi)
curl -X DELETE http://localhost:8083/connectors/postgres-connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @cdc/postgres-connector.json

# Xem danh sách connectors
curl -s http://localhost:8083/connectors | python3 -m json.tool

# Pause / Resume
curl -X PUT http://localhost:8083/connectors/postgres-connector/pause
curl -X PUT http://localhost:8083/connectors/postgres-connector/resume
```

### Lưu ý replication slot

Debezium tạo replication slot `debezium_slot` trong PostgreSQL. Nếu Debezium bị stop mà slot không được xóa, PostgreSQL sẽ giữ WAL và disk có thể đầy theo thời gian.

```bash
# Xem replication slots
docker exec postgres psql -U postgres -c "SELECT slot_name, active FROM pg_replication_slots;"

# Nếu cần xóa slot thủ công (khi Debezium đã được xóa)
docker exec postgres psql -U postgres -c "SELECT pg_drop_replication_slot('debezium_slot');"
```
