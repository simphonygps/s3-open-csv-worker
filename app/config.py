# app/config.py
import os
from dataclasses import dataclass


@dataclass
class Settings:
    # S3 / MinIO
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str
    s3_use_ssl: bool

    # Postgres
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str

    # Misc
    processed_table: str = "s3_processed_files"  # idempotency table


def get_settings() -> Settings:
    return Settings(
        s3_endpoint=os.environ.get("S3_ENDPOINT", "minio:9000"),
        s3_access_key=os.environ["S3_ACCESS_KEY"],
        s3_secret_key=os.environ["S3_SECRET_KEY"],
        s3_bucket=os.environ.get("S3_BUCKET", "simphony-dev"),
        s3_use_ssl=os.environ.get("S3_USE_SSL", "false").lower() == "true",
        pg_host=os.environ.get("PG_HOST", "postgres-db"),
        pg_port=int(os.environ.get("PG_PORT", "5432")),
        pg_db=os.environ.get("PG_DB", "simphony"),
        pg_user=os.environ.get("PG_USER", "simphony"),
        pg_password=os.environ["PG_PASSWORD"],
    )
