# app/db.py

import logging
import json
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import Json, execute_batch

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
    """
    Ensure the s3_processed_files table exists and has all lifecycle columns.

    This function is idempotent:
      - CREATE TABLE IF NOT EXISTS ...
      - ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...
    """
    table = settings.processed_table

    ddl_create = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        id SERIAL PRIMARY KEY,
        bucket TEXT NOT NULL,
        object_key TEXT NOT NULL,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uniq_bucket_key UNIQUE (bucket, object_key)
    );
    """

    ddl_alter = f"""
    ALTER TABLE {table}
        ADD COLUMN IF NOT EXISTS status                TEXT DEFAULT 'received',
        ADD COLUMN IF NOT EXISTS rows_total            INT,
        ADD COLUMN IF NOT EXISTS rows_inserted         INT,
        ADD COLUMN IF NOT EXISTS rows_failed           INT,
        ADD COLUMN IF NOT EXISTS error_code            TEXT,
        ADD COLUMN IF NOT EXISTS error_message         TEXT,
        ADD COLUMN IF NOT EXISTS processed_started_at  TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS processed_finished_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS size_bytes            BIGINT,
        ADD COLUMN IF NOT EXISTS deleted_at            TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS deleted_reason        TEXT,
        ADD COLUMN IF NOT EXISTS deletion_mode         TEXT;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl_create)
            cur.execute(ddl_alter)
        conn.commit()
        logger.info(
            "[DB] ensured processed_table '%s' exists with lifecycle columns",
            table,
        )


def is_object_processed(bucket: str, key: str) -> bool:
    """
    Return True only if the object was processed successfully.

    We consider an object 'processed' only when status = 'success'.
    This allows us to retry objects that previously failed.
    """
    sql = f"""
    SELECT 1
    FROM {settings.processed_table}
    WHERE bucket = %s
      AND object_key = %s
      AND status = 'success'
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (bucket, key))
            return cur.fetchone() is not None


def mark_object_processing_started(bucket: str, key: str):
    """
    Mark that processing has started for the given object.

    If a row already exists, we reset status and lifecycle fields.
    """
    sql_insert = f"""
    INSERT INTO {settings.processed_table} (bucket, object_key, status,
                                            processed_started_at,
                                            processed_finished_at,
                                            rows_total, rows_inserted, rows_failed,
                                            error_code, error_message,
                                            size_bytes,
                                            deleted_at, deleted_reason, deletion_mode)
    VALUES (%s, %s, 'processing', NOW(), NULL,
            NULL, NULL, NULL,
            NULL, NULL,
            NULL,
            NULL, NULL, NULL)
    ON CONFLICT (bucket, object_key) DO UPDATE
    SET status = 'processing',
        processed_started_at = NOW(),
        processed_finished_at = NULL,
        rows_total = NULL,
        rows_inserted = NULL,
        rows_failed = NULL,
        error_code = NULL,
        error_message = NULL,
        size_bytes = NULL,
        deleted_at = NULL,
        deleted_reason = NULL,
        deletion_mode = NULL,
        processed_at = NOW();
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_insert, (bucket, key))
        conn.commit()
    logger.info("[DB] Marked object as processing: %s/%s", bucket, key)


def mark_object_processed_success(
    bucket: str,
    key: str,
    rows_total: int,
    rows_inserted: int,
    rows_failed: int,
    size_bytes: int | None = None,
):
    """
    Mark that processing completed successfully for the given object.

    Also stores size_bytes (may be None for legacy paths).
    """
    sql_update = f"""
    UPDATE {settings.processed_table}
    SET status = 'success',
        rows_total = %s,
        rows_inserted = %s,
        rows_failed = %s,
        error_code = NULL,
        error_message = NULL,
        processed_finished_at = NOW(),
        processed_at = NOW(),
        size_bytes = %s
    WHERE bucket = %s AND object_key = %s;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql_update,
                (rows_total, rows_inserted, rows_failed, size_bytes, bucket, key),
            )
            if cur.rowcount == 0:
                # In case mark_object_processing_started() was never called
                sql_insert = f"""
                INSERT INTO {settings.processed_table} (
                    bucket, object_key, status,
                    rows_total, rows_inserted, rows_failed,
                    processed_started_at, processed_finished_at,
                    size_bytes
                ) VALUES (%s, %s, 'success',
                          %s, %s, %s,
                          NOW(), NOW(),
                          %s)
                ON CONFLICT (bucket, object_key) DO UPDATE
                SET status = 'success',
                    rows_total = EXCLUDED.rows_total,
                    rows_inserted = EXCLUDED.rows_inserted,
                    rows_failed = EXCLUDED.rows_failed,
                    size_bytes = EXCLUDED.size_bytes,
                    processed_finished_at = NOW(),
                    processed_at = NOW();
                """
                cur.execute(
                    sql_insert,
                    (
                        bucket,
                        key,
                        rows_total,
                        rows_inserted,
                        rows_failed,
                        size_bytes,
                    ),
                )
        conn.commit()

    logger.info(
        "[DB] Marked object as success: %s/%s (total=%d, inserted=%d, failed=%d, size_bytes=%r)",
        bucket,
        key,
        rows_total,
        rows_inserted,
        rows_failed,
        size_bytes,
    )


def mark_object_processed_error(
    bucket: str,
    key: str,
    error_code: str,
    error_message: str,
    rows_total: int | None = None,
    rows_inserted: int | None = None,
    rows_failed: int | None = None,
):
    """
    Mark that processing failed for the given object.

    rows_* are optional and can be used to store partial counts if known.
    """
    # Truncate error_message to a safe length
    if error_message is not None and len(error_message) > 2000:
        error_message = error_message[:2000]

    sql_update = f"""
    UPDATE {settings.processed_table}
    SET status = 'error',
        rows_total = COALESCE(%s, rows_total),
        rows_inserted = COALESCE(%s, rows_inserted),
        rows_failed = COALESCE(%s, rows_failed),
        error_code = %s,
        error_message = %s,
        processed_finished_at = NOW(),
        processed_at = NOW()
    WHERE bucket = %s AND object_key = %s;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql_update,
                (
                    rows_total,
                    rows_inserted,
                    rows_failed,
                    error_code,
                    error_message,
                    bucket,
                    key,
                ),
            )
            if cur.rowcount == 0:
                # If no existing row, insert a new one with error status
                sql_insert = f"""
                INSERT INTO {settings.processed_table} (
                    bucket, object_key, status,
                    rows_total, rows_inserted, rows_failed,
                    error_code, error_message,
                    processed_started_at, processed_finished_at
                ) VALUES (%s, %s, 'error',
                          %s, %s, %s,
                          %s, %s,
                          NOW(), NOW())
                ON CONFLICT (bucket, object_key) DO UPDATE
                SET status = 'error',
                    rows_total = EXCLUDED.rows_total,
                    rows_inserted = EXCLUDED.rows_inserted,
                    rows_failed = EXCLUDED.rows_failed,
                    error_code = EXCLUDED.error_code,
                    error_message = EXCLUDED.error_message,
                    processed_finished_at = NOW(),
                    processed_at = NOW();
                """
                cur.execute(
                    sql_insert,
                    (
                        bucket,
                        key,
                        rows_total,
                        rows_inserted,
                        rows_failed,
                        error_code,
                        error_message,
                    ),
                )
        conn.commit()

    logger.warning(
        "[DB] Marked object as error: %s/%s (code=%s, msg=%s)",
        bucket,
        key,
        error_code,
        error_message,
    )


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

    # Column order is taken from the first row.
    # Internal ETL metadata keys (prefixed with "_") are not soft_data columns.
    columns = [k for k in rows[0].keys() if not k.startswith("_")]

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

    values = []
    for row in rows:
        row_values = []
        for col in columns:
            v = row.get(col)
            if col == "raw_payload" and v is not None:
                row_values.append(Json(v))
            else:
                row_values.append(v)
        values.append(tuple(row_values))

    logger.info(
        "[DB] Inserting %d rows into soft_data with columns: %s",
        len(rows),
        columns,
    )

    with get_connection() as conn:
        try:
            with conn.cursor() as cur:
                execute_batch(cur, sql, values, page_size=500)
                _insert_telemetry_etl_rows(cur, rows)
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception("[DB] Failed to insert batch into soft_data")
            raise


def _insert_telemetry_etl_rows(cur, rows: list[dict]) -> None:
    """
    Best-effort ETL routing observability for offline CSV records.
    """
    if not rows:
        return

    sql = """
    INSERT INTO public.telemetry_etl_records (
        dedup_key,
        flow_id, transport, source, probe_type, protocol_version, contract_version,
        message_type, payload_shape, device_identifier_type, device_id, etl_branch,
        routing_status, routing_reason, processing_status, traccar_sync_status,
        normalized_payload, received_at, routed_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'matched', %s, 'completed', 'pending', %s::jsonb, %s, NOW())
    ON CONFLICT DO NOTHING
    """

    payload_rows = []
    for row in rows:
        device_id = row.get("deviceid")
        ts = row.get("timestamp")
        probe_type = row.get("_etl_probe_type") or row.get("msg_probe") or "sw"
        protocol_version = row.get("_etl_protocol_version") or "legacy"
        flow_id = row.get("_etl_flow_id") or "offline_csv_v12"
        transport = row.get("_etl_transport") or "file"
        source = row.get("_etl_source") or "s3-open"
        contract_version = row.get("_etl_contract_version") or "1.2"
        message_type = row.get("_etl_message_type") or "offline_file_payload"
        payload_shape = row.get("_etl_payload_shape") or "csv_file"
        device_identifier_type = row.get("_etl_device_identifier_type") or "software_device_id"
        etl_branch = row.get("_etl_branch") or "offline_csv_file_v12"
        routing_reason = row.get("_etl_routing_reason") or "matched s3-open csv flow"

        dedup_key = None
        if device_id is not None and ts is not None:
            dedup_key = f"{source}:{probe_type}:{device_id}:{ts}"
        payload_rows.append(
            (
                dedup_key,
                flow_id,
                transport,
                source,
                probe_type,
                protocol_version,
                contract_version,
                message_type,
                payload_shape,
                device_identifier_type,
                device_id,
                etl_branch,
                routing_reason,
                json.dumps(row, default=str),
                ts,
            )
        )

    execute_batch(cur, sql, payload_rows, page_size=500)
