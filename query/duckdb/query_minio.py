"""
Query JSON files từ MinIO bằng DuckDB.

Cài đặt:
    pip install duckdb

Chạy:
    python query/duckdb/query_minio.py
"""

import duckdb

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

con = duckdb.connect()

con.execute(f"""
    CREATE SECRET minio_secret (
        TYPE S3,
        KEY_ID '{MINIO_ACCESS_KEY}',
        SECRET '{MINIO_SECRET_KEY}',
        ENDPOINT '{MINIO_ENDPOINT.replace("http://", "")}',
        USE_SSL false,
        URL_STYLE 'path'
    )
""")


def query_api_users():
    """Query data từ pipeline API (users_created → MinIO warehouse/api/)"""
    return con.execute("""
        SELECT *
        FROM read_json_auto('s3://warehouse/api/**/*.json')
        ORDER BY full_name
        LIMIT 20
    """).df()


def query_cdc_users():
    """Query data từ pipeline CDC (postgres.public.users → MinIO warehouse/cdc/)"""
    return con.execute("""
        SELECT *
        FROM read_json_auto('s3://warehouse/cdc/**/*.json')
        ORDER BY id
        LIMIT 20
    """).df()


def query_stats():
    """Thống kê tổng hợp"""
    return con.execute("""
        SELECT
            gender,
            COUNT(*) AS total,
            MIN(birth_year) AS oldest_birth_year,
            MAX(birth_year) AS newest_birth_year
        FROM read_json_auto('s3://warehouse/api/**/*.json')
        GROUP BY gender
        ORDER BY total DESC
    """).df()


if __name__ == "__main__":
    print("=== API Users (warehouse/api/) ===")
    print(query_api_users())

    print("\n=== CDC Users (warehouse/cdc/) ===")
    print(query_cdc_users())

    print("\n=== Stats by Gender ===")
    print(query_stats())
