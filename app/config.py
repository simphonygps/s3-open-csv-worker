# app/config.py
import os
from dataclasses import dataclass, field
from .sss_reader import SecretReadError, secret_text


@dataclass
class Settings:
    # S3 / MinIO
    s3_endpoint: str
    s3_access_key: str = field(repr=False)
    s3_secret_key: str = field(repr=False)
    s3_bucket: str
    s3_use_ssl: bool

    # Postgres
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str = field(repr=False)

    # Misc
    processed_table: str = "s3_processed_files"  # idempotency table


def get_settings() -> Settings:
    if any(key in os.environ for key in ('DB_DSN','DATABASE_URL','POSTGRES_DSN','PG_DSN','PGPASSWORD','PGPASSFILE')):
        raise SecretReadError('legacy_database_authority_rejected')
    return Settings(
        s3_endpoint=os.environ.get("S3_ENDPOINT", "minio:9000"),
        s3_access_key=secret_text("S3_ACCESS_KEY"),
        s3_secret_key=secret_text("S3_SECRET_KEY"),
        s3_bucket=os.environ["S3_BUCKET"],
        s3_use_ssl=os.environ.get("S3_USE_SSL", "false").lower() == "true",
        pg_host=os.environ.get("PG_HOST", "postgres-db"),
        pg_port=int(os.environ.get("PG_PORT", "5432")),
        pg_db=os.environ["PG_DB"],
        pg_user=os.environ["PG_USER"],
        pg_password=secret_text("PG_PASSWORD"),
    )
