# Kafka & Debezium

## Kafka

### Cấu hình

Kafka chạy ở chế độ **KRaft** (không cần ZooKeeper). Cluster ID tĩnh trong `docker-compose.yaml`.

| Topic | Producer | Consumer | Mô tả |
|-------|----------|----------|-------|
| `users_created` | FastAPI, Airflow | Flink | Data user từ API |
| `postgres.public.users` | Debezium | Flink | CDC events từ PostgreSQL |
| `debezium_connect_configs` | Debezium | Debezium | Internal |
| `debezium_connect_offsets` | Debezium | Debezium | Internal |
| `debezium_connect_statuses` | Debezium | Debezium | Internal |

### Kiểm tra topics

```bash
# Liệt kê tất cả topics
docker exec broker kafka-topics \
  --bootstrap-server broker:29092 --list

# Xem messages trong topic (từ đầu, lấy 5 messages)
docker exec broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic users_created \
  --from-beginning \
  --max-messages 5

# Xem CDC messages
docker exec broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic postgres.public.users \
  --from-beginning \
  --max-messages 5

# Xem số messages trong topic
docker exec broker kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server broker:29092 \
  --topic users_created
```

### Listeners

| Listener | Địa chỉ | Dùng bởi |
|----------|---------|---------|
| `PLAINTEXT` | `broker:29092` | Các container trong Docker network |
| `PLAINTEXT_HOST` | `localhost:9092` | Kết nối từ host machine |

---

## Debezium (CDC)

### Cách hoạt động

```
PostgreSQL WAL (Write-Ahead Log)
        │
        │  Debezium đọc WAL liên tục
        ▼
kafka-connect (Debezium connector)
        │
        │  Publish event mỗi khi có thay đổi
        ▼
Kafka topic: postgres.public.users
```

**WAL** (Write-Ahead Log): PostgreSQL ghi mọi thay đổi vào WAL trước khi apply vào table. Debezium đọc WAL này như đọc một "stream of changes" mà không cần trigger hay stored procedure.

### Format message

Mỗi CDC event có cấu trúc:

```json
{
  "before": null,
  "after": {
    "id": 1,
    "name": "Alice Nguyen",
    "email": "alice@example.com",
    "department": "Engineering"
  },
  "op": "c",
  "ts_ms": 1735000000000,
  "source": { "table": "users", "lsn": 12345 }
}
```

| Field | Giá trị | Mô tả |
|-------|---------|-------|
| `op`  | `c` | CREATE (INSERT mới) |
| `op`  | `u` | UPDATE |
| `op`  | `d` | DELETE (`after` = null) |
| `op`  | `r` | READ (snapshot ban đầu khi connector khởi động) |

### Kiểm tra connector

```bash
# Xem connector đã đăng ký chưa
curl -s http://localhost:8083/connectors | python3 -m json.tool

# Xem status của connector
curl -s http://localhost:8083/connectors/postgres-connector/status \
  | python3 -m json.tool

# Xem config connector
curl -s http://localhost:8083/connectors/postgres-connector/config \
  | python3 -m json.tool
```

### Config connector (`cdc/postgres-connector.json`)

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "postgres",
  "database.port": "5432",
  "database.user": "postgres",
  "database.password": "postgres",
  "database.dbname": "mydb",
  "table.include.list": "public.users",
  "plugin.name": "pgoutput",
  "topic.prefix": "postgres"
}
```

**Tại sao `plugin.name = pgoutput`?**  
`pgoutput` là native PostgreSQL logical decoding plugin, không cần cài thêm gì.

**Tại sao PostgreSQL dùng `md5` auth?**  
Debezium 2.4 chưa hỗ trợ `scram-sha-256` — phải dùng `md5` để kết nối được.

### Trigger CDC thủ công

```bash
# Kết nối vào PostgreSQL
docker exec -it postgres psql -U postgres -d mydb

# INSERT mới → sẽ sinh ra op='c'
INSERT INTO users (name, email, department)
VALUES ('Test User', 'test@example.com', 'QA');

# UPDATE → op='u'
UPDATE users SET department = 'Engineering' WHERE email = 'test@example.com';

# DELETE → op='d'
DELETE FROM users WHERE email = 'test@example.com';
```
