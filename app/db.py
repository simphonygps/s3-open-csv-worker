# app/db.py

import logging
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import execute_batch

from .config import get_settings

settings = get_settings()
logger = logging.getLogger("s3-open-csv-worker")


@contextmanager
def get_connection():
    conn = psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=settings.pg_db,
        user=settings.pg_user,
        password=settings.pg_password,
    )
    try:
        yield conn
    finally:
        conn.close()


def ensure_processed_table():
    """Create idempotency table if not exists."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {settings.processed_table} (
        id SERIAL PRIMARY KEY,
        bucket TEXT NOT NULL,
        object_key TEXT NOT NULL,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uniq_bucket_key UNIQUE (bucket, object_key)
    );
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def mark_object_processed(bucket: str, key: str):
    sql = f"""
    INSERT INTO {settings.processed_table} (bucket, object_key)
    VALUES (%s, %s)
    ON CONFLICT (bucket, object_key) DO NOTHING;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (bucket, key))
        conn.commit()


def is_object_processed(bucket: str, key: str) -> bool:
    sql = f"SELECT 1 FROM {settings.processed_table} WHERE bucket = %s AND object_key = %s"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (bucket, key))
            return cur.fetchone() is not None


def insert_soft_data_rows(rows: list[dict]):
    """
    Insert a batch of rows into soft_data.

    The keys of the first row define the column list.
    We:
      - build a dynamic column list from row.keys()
      - quote ALL column names, to preserve camelCase
      - use execute_batch for efficiency

    Rows are expected to already have DB column names as keys
    (e.g. "timestamp", "deviceid", "gpsAccuracy", ..., "source").
    """
    if not rows:
        return

    # Column order is taken from the first row
    columns = list(rows[0].keys())

    # Always quote column names to preserve exact case (camelCase, etc.)
    def escape_col(name: str) -> str:
        return f'"{name}"'

    columns_sql = ", ".join(escape_col(c) for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))

    sql = f"""
    INSERT INTO soft_data (
        {columns_sql}
    ) VALUES ({placeholders})
    """

    values = [
        tuple(row.get(col) for col in columns)
        for row in rows
    ]

    logger.info(
        "[DB] Inserting %d rows into soft_data with columns: %s",
        len(rows),
        columns,
    )

    with get_connection() as conn:
        try:
            with conn.cursor() as cur:
                execute_batch(cur, sql, values, page_size=500)
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("[DB] Failed to insert batch into soft_data")
            raise
