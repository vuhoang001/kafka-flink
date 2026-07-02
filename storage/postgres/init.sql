-- Database riêng cho Iceberg REST catalog — lưu metadata bảng (con trỏ
-- "bảng X → metadata.json mới nhất"). Có nó, restart iceberg-rest không mất bảng.
CREATE DATABASE catalogdb;

-- Bảng users: đây là source table mà Debezium sẽ theo dõi CDC
CREATE TABLE users (
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(100) NOT NULL,
    email      VARCHAR(100) NOT NULL UNIQUE,
    department VARCHAR(50)
);

-- Dữ liệu mẫu (Debezium sẽ capture các row này qua snapshot ban đầu)
INSERT INTO users (name, email, department) VALUES
    ('Alice Nguyen',  'alice@example.com',   'Engineering'),
    ('Bob Tran',      'bob@example.com',     'Marketing'),
    ('Charlie Le',    'charlie@example.com', 'Engineering');
