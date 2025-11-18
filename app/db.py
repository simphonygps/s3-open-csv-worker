# app/db.py
import psycopg2
from psycopg2.extras import execute_batch
from contextlib import contextmanager
from .config import get_settings

settings = get_settings()


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
    Expect keys already mapped to DB column names.
    """
    if not rows:
        return

    # === IMPORTANT ===
    # Adjust this to match your real soft_data columns.
    # Here we assume snake_case names corresponding to CSV header.
    columns = [
        "timestamp",
        "device_id",
        "latitude",
        "longitude",
        "altitude",
        "gps_accuracy",
        "speed",
        "bearing",
        "pressure",
        "calculated_altitude",
        "accel_x",
        "accel_y",
        "accel_z",
        "gyro_x",
        "gyro_y",
        "gyro_z",
        "mag_x",
        "mag_y",
        "mag_z",
        "battery_probe",
        "battery_level",
        "battery_voltage",
        "bms_battery_voltage",
        "signal_strength",
        "temperature",
        "satellites_in_view",
        "satellites_in_use",
        "input_voltage",
        "bms_soc",
        "charging_status",
    ]

    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    sql = f"""
    INSERT INTO soft_data (
        {col_list}
    ) VALUES ({placeholders})
    """

    values = [
        tuple(row.get(col) for col in columns)
        for row in rows
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, sql, values, page_size=500)
        conn.commit()
