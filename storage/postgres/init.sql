-- Database riêng cho Nessie (Iceberg catalog metadata)
-- Phải tạo trước khi Nessie khởi động
CREATE DATABASE nessiedb;

-- Bảng users: đây là source table mà Debezium sẽ theo dõi CDC
CREATE TABLE users (
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(100) NOT NULL,
    email      VARCHAR(100) NOT NULL,
    department VARCHAR(50)
);

-- Dữ liệu mẫu (Debezium sẽ capture các row này qua snapshot ban đầu)
INSERT INTO users (name, email, department) VALUES
    ('Alice Nguyen',  'alice@example.com',   'Engineering'),
    ('Bob Tran',      'bob@example.com',     'Marketing'),
    ('Charlie Le',    'charlie@example.com', 'Engineering');
