# app/retention_worker.py

import logging
import os
from datetime import datetime, timedelta, timezone

from .config import get_settings
from .db import get_connection, ensure_processed_table
from .s3_client import s3

logger = logging.getLogger("s3-open-csv-worker")

settings = get_settings()


def _get_retention_days() -> int:
    """
    Read retention days from ENV (RETENTION_DAYS) or use default 30.
    """
    raw = os.getenv("RETENTION_DAYS", "30")
    try:
        days = int(raw)
        if days <= 0:
            raise ValueError
        return days
    except ValueError:
        logger.warning(
            "[RETENTION] Invalid RETENTION_DAYS=%r, falling back to 30", raw
        )
        return 30


def _get_retention_limit() -> int:
    """
    Limit how many candidates we inspect in a single run.
    ENV: RETENTION_LIMIT (default 1000)
    """
    raw = os.getenv("RETENTION_LIMIT", "1000")
    try:
        limit = int(raw)
        if limit <= 0:
            raise ValueError
        return limit
    except ValueError:
        logger.warning(
            "[RETENTION] Invalid RETENTION_LIMIT=%r, falling back to 1000", raw
        )
        return 1000


def fetch_db_candidates(threshold_ts: datetime, limit: int):
    """
    Fetch candidate files from s3_processed_files that are older than the threshold.

    We only consider:
      - status = 'success'
      - processed_finished_at < threshold_ts
    """
    ensure_processed_table()

    sql = f"""
        SELECT
            bucket,
            object_key,
            rows_total,
            processed_finished_at
        FROM {settings.processed_table}
        WHERE status = 'success'
          AND processed_finished_at IS NOT NULL
          AND processed_finished_at < %s
        ORDER BY processed_finished_at ASC
        LIMIT %s;
    """

    rows = []

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (threshold_ts, limit))
            for bucket, object_key, rows_total, processed_finished_at in cur.fetchall():
                rows.append(
                    {
                        "bucket": bucket,
                        "object_key": object_key,
                        "rows_total": rows_total,
                        "processed_finished_at": processed_finished_at,
                    }
                )

    return rows


def check_s3_exists(bucket: str, key: str) -> bool:
    """
    Check if a given object still exists in S3/MinIO using head_object.

    This is READ-ONLY and does not modify anything.
    """
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        logger.warning(
            "[RETENTION] S3 object missing or inaccessible: %s/%s (%s)",
            bucket,
            key,
            e,
        )
        return False


def run_retention_check():
    """
    Run a single retention scan in read-only mode.

    Steps:
      - compute threshold date
      - fetch candidates from DB
      - for each candidate, check if S3 object exists
      - print a summary (no deletions)
    """
    retention_days = _get_retention_days()
    limit = _get_retention_limit()

    now_utc = datetime.now(timezone.utc)
    threshold_ts = now_utc - timedelta(days=retention_days)

    logger.info(
        "[RETENTION] Starting retention scan: days=%d, limit=%d, threshold=%s",
        retention_days,
        limit,
        threshold_ts.isoformat(),
    )

    candidates = fetch_db_candidates(threshold_ts, limit)

    logger.info("[RETENTION] Found %d DB candidates older than threshold", len(candidates))

    if not candidates:
        print("No candidates found for retention (read-only).")
        return

    total = 0
    exist_count = 0
    missing_count = 0

    print("Retention candidates (read-only):")
    print("------------------------------------------------------------")

    for row in candidates:
        total += 1
        bucket = row["bucket"]
        key = row["object_key"]
        rows_total = row["rows_total"]
        finished_at = row["processed_finished_at"]

        exists = check_s3_exists(bucket, key)
        if exists:
            exist_count += 1
        else:
            missing_count += 1

        print(
            f"- {bucket}/{key} | rows_total={rows_total} | "
            f"processed_finished_at={finished_at} | s3_exists={exists}"
        )

    print("------------------------------------------------------------")
    print(
        f"Retention summary (read-only): total={total}, "
        f"s3_exists={exist_count}, s3_missing_or_error={missing_count}"
    )

    logger.info(
        "[RETENTION] Scan finished: total=%d, s3_exists=%d, s3_missing_or_error=%d",
        total,
        exist_count,
        missing_count,
    )


if __name__ == "__main__":
    run_retention_check()
