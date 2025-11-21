# app/retention_worker.py

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

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
        if days < 0:
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


def _is_delete_enabled() -> bool:
    """
    Check RETENTION_DELETE_ENABLED env var.

    Accepted "true" values (case-insensitive): true, 1, yes, on
    Anything else = False.
    """
    raw = os.getenv("RETENTION_DELETE_ENABLED", "false")
    val = (raw or "").strip().lower()
    return val in ("1", "true", "yes", "on")


def fetch_db_candidates(threshold_ts: datetime, limit: int) -> List[Dict[str, Any]]:
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

    rows: List[Dict[str, Any]] = []

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


def _update_status(
    bucket: str,
    key: str,
    status: str,
    error_code: str | None = None,
    error_message: str | None = None,
) -> None:
    """
    Update lifecycle status and error fields in s3_processed_files for a given object.
    """
    sql = f"""
        UPDATE {settings.processed_table}
        SET status = %s,
            error_code = %s,
            error_message = %s
        WHERE bucket = %s AND object_key = %s;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (status, error_code, error_message, bucket, key))
        conn.commit()

    logger.info(
        "[RETENTION][DB] Updated lifecycle: %s/%s -> status=%s, error_code=%r",
        bucket,
        key,
        status,
        error_code,
    )


def run_retention_check():
    """
    Run a single retention scan.

    Always:
      - compute threshold date
      - fetch candidates from DB
      - for each candidate, check if S3 object exists
      - print a summary

    If RETENTION_DELETE_ENABLED=false (default):
      - READ-ONLY: does not modify DB or S3.

    If RETENTION_DELETE_ENABLED=true:
      - For each candidate:
          * If S3 exists:
              - delete the object
              - set status='deleted' in s3_processed_files
          * If S3 missing:
              - set status='missing' with error_code='NotFound'
          * On delete error:
              - set status='delete_error' with error information
    """
    retention_days = _get_retention_days()
    limit = _get_retention_limit()
    delete_enabled = _is_delete_enabled()

    now_utc = datetime.now(timezone.utc)
    threshold_ts = now_utc - timedelta(days=retention_days)

    logger.info(
        "[RETENTION] Starting retention scan: days=%d, limit=%d, threshold=%s, delete_enabled=%s",
        retention_days,
        limit,
        threshold_ts.isoformat(),
        delete_enabled,
    )

    candidates = fetch_db_candidates(threshold_ts, limit)

    if not candidates:
        print("No candidates found for retention (read-only).")
        logger.info("[RETENTION] No DB candidates older than threshold")
        return

    print("Retention candidates (read-only):")
    print("------------------------------------------------------------")

    total = 0
    exist_count = 0
    missing_count = 0

    deleted_count = 0
    mark_missing_count = 0
    delete_error_count = 0

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

        # DELETE MODE (optional)
        if delete_enabled:
            if exists:
                try:
                    s3.delete_object(Bucket=bucket, Key=key)
                    _update_status(bucket, key, status="deleted")
                    deleted_count += 1
                    print("  -> deleted from S3 and marked status='deleted' in DB")
                except Exception as e:
                    err_code = type(e).__name__
                    err_msg = str(e)
                    _update_status(
                        bucket,
                        key,
                        status="delete_error",
                        error_code=err_code,
                        error_message=err_msg,
                    )
                    delete_error_count += 1
                    logger.exception(
                        "[RETENTION] Failed to delete %s/%s from S3", bucket, key
                    )
                    print(
                        "  -> delete_error, see logs; status='delete_error' in DB "
                        f"(error_code={err_code})"
                    )
            else:
                # S3 object already missing: reflect that in DB
                _update_status(
                    bucket,
                    key,
                    status="missing",
                    error_code="NotFound",
                    error_message="Object missing in S3 during retention delete",
                )
                mark_missing_count += 1
                print("  -> S3 missing; marked status='missing' in DB")

    print("------------------------------------------------------------")
    print(
        f"Retention summary (read-only part): total={total}, "
        f"s3_exists={exist_count}, s3_missing_or_error={missing_count}"
    )

    if delete_enabled:
        print(
            f"Retention delete summary: deleted={deleted_count}, "
            f"marked_missing={mark_missing_count}, delete_error={delete_error_count}"
        )
        logger.info(
            "[RETENTION] Delete summary: deleted=%d, marked_missing=%d, delete_error=%d",
            deleted_count,
            mark_missing_count,
            delete_error_count,
        )
    else:
        print("Delete mode is DISABLED (RETENTION_DELETE_ENABLED=false). No changes applied.")
        logger.info("[RETENTION] Delete mode disabled. No changes applied to S3/DB.")


if __name__ == "__main__":
    run_retention_check()
