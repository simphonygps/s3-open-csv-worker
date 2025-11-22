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

    This helper is used for non-delete statuses (e.g. 'missing', 'delete_error').
    It does NOT touch deleted_at/deleted_reason/deletion_mode.
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


def _mark_deleted_success(
    bucket: str,
    key: str,
    deleted_reason: str,
    deletion_mode: str,
) -> None:
    """
    Mark object as successfully deleted by retention.

    This sets:
      - status = 'deleted'
      - deleted_at = NOW()
      - deleted_reason
      - deletion_mode (e.g. 'auto-retention')
    """
    sql = f"""
        UPDATE {settings.processed_table}
        SET status = %s,
            deleted_at = NOW(),
            deleted_reason = %s,
            deletion_mode = %s
        WHERE bucket = %s AND object_key = %s;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, ("deleted", deleted_reason, deletion_mode, bucket, key))
        conn.commit()

    logger.info(
        "[RETENTION][DB] Marked deleted: %s/%s -> status='deleted', "
        "deleted_reason=%r, deletion_mode=%r",
        bucket,
        key,
        deleted_reason,
        deletion_mode,
    )


def _notify_retention(summary: Dict[str, Any]) -> None:
    """
    Mock notification for retention actions.

    Currently logs/prints a summary. Later this can be wired to Slack/email.
    """
    files_deleted = summary.get("files_deleted", 0) or 0
    total_size_bytes = summary.get("total_size_bytes", 0) or 0

    if total_size_bytes:
        total_size_mb = round(total_size_bytes / (1024 * 1024), 3)
    else:
        total_size_mb = 0

    msg = (
        f"[NOTIFY] Retention removed {files_deleted} files "
        f"(total_size_mb={total_size_mb})"
    )
    # Print to stdout and log to logger for operators
    print(msg)
    logger.info(msg)


def get_retention_preview() -> Dict[str, Any]:
    """
    Compute a read-only preview of retention candidates.

    Returns a JSON-serializable dict:

    {
      "config": {
        "retention_days": int,
        "retention_limit": int,
        "delete_enabled": bool,
        "threshold_utc": str
      },
      "summary": {
        "total_candidates": int,
        "s3_exists": int,
        "s3_missing_or_error": int
      },
      "candidates": [
        {
          "bucket": str,
          "object_key": str,
          "rows_total": int | None,
          "processed_finished_at": datetime,
          "s3_exists": bool
        },
        ...
      ]
    }
    """
    retention_days = _get_retention_days()
    limit = _get_retention_limit()
    delete_enabled = _is_delete_enabled()

    now_utc = datetime.now(timezone.utc)
    threshold_ts = now_utc - timedelta(days=retention_days)

    logger.info(
        "[RETENTION] Preview scan: days=%d, limit=%d, threshold=%s, delete_enabled=%s",
        retention_days,
        limit,
        threshold_ts.isoformat(),
        delete_enabled,
    )

    db_rows = fetch_db_candidates(threshold_ts, limit)

    total = 0
    exist_count = 0
    missing_count = 0
    candidates: List[Dict[str, Any]] = []

    for row in db_rows:
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

        candidates.append(
            {
                "bucket": bucket,
                "object_key": key,
                "rows_total": rows_total,
                "processed_finished_at": finished_at,
                "s3_exists": exists,
            }
        )

    preview: Dict[str, Any] = {
        "config": {
            "retention_days": retention_days,
            "retention_limit": limit,
            "delete_enabled": delete_enabled,
            "threshold_utc": threshold_ts.isoformat(),
        },
        "summary": {
            "total_candidates": total,
            "s3_exists": exist_count,
            "s3_missing_or_error": missing_count,
        },
        "candidates": candidates,
    }

    return preview


def get_retention_history(limit: int = 50) -> Dict[str, Any]:
    """
    Return last `limit` deleted objects from the lifecycle table
    (s3_processed_files). Used by /retention/history endpoint.
    """
    sql = f"""
        SELECT
            bucket,
            object_key,
            status,
            rows_total,
            rows_inserted,
            rows_failed,
            processed_finished_at,
            deleted_at,
            deleted_reason,
            deletion_mode
        FROM {settings.processed_table}
        WHERE deleted_at IS NOT NULL
        ORDER BY deleted_at DESC
        LIMIT %s;
    """

    items: List[Dict[str, Any]] = []

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            for (
                bucket,
                object_key,
                status,
                rows_total,
                rows_inserted,
                rows_failed,
                processed_finished_at,
                deleted_at,
                deleted_reason,
                deletion_mode,
            ) in cur.fetchall():
                items.append(
                    {
                        "bucket": bucket,
                        "object_key": object_key,
                        "status": status,
                        "rows_total": rows_total,
                        "rows_inserted": rows_inserted,
                        "rows_failed": rows_failed,
                        "processed_finished_at": processed_finished_at,
                        "deleted_at": deleted_at,
                        "deleted_reason": deleted_reason,
                        "deletion_mode": deletion_mode,
                    }
                )

    return {
        "count": len(items),
        "items": items,
    }


def run_retention_check():
    """
    Run a single retention scan from CLI.

    Uses get_retention_preview() to compute candidates and then,
    if delete mode is enabled, actually deletes the objects and updates DB.
    """
    preview = get_retention_preview()
    config = preview["config"]
    summary = preview["summary"]
    candidates = preview["candidates"]

    retention_days = config["retention_days"]
    limit = config["retention_limit"]
    delete_enabled = config["delete_enabled"]

    total = summary["total_candidates"]
    exist_count = summary["s3_exists"]
    missing_count = summary["s3_missing_or_error"]

    if total == 0:
        print("No candidates found for retention (read-only).")
        logger.info(
            "[RETENTION] No DB candidates older than threshold (days=%d, limit=%d)",
            retention_days,
            limit,
        )
        return

    print("Retention candidates (read-only):")
    print("------------------------------------------------------------")
    for c in candidates:
        print(
            f"- {c['bucket']}/{c['object_key']} | "
            f"rows_total={c['rows_total']} | "
            f"processed_finished_at={c['processed_finished_at']} | "
            f"s3_exists={c['s3_exists']}"
        )
    print("------------------------------------------------------------")
    print(
        f"Retention summary (read-only part): total={total}, "
        f"s3_exists={exist_count}, s3_missing_or_error={missing_count}"
    )

    # DELETE MODE (optional)
    if not delete_enabled:
        print("Delete mode is DISABLED (RETENTION_DELETE_ENABLED=false). No changes applied.")
        logger.info("[RETENTION] Delete mode disabled. No changes applied to S3/DB.")
        return

    deleted_count = 0
    mark_missing_count = 0
    delete_error_count = 0
    total_size_bytes = 0  # placeholder, 0 for now (no size tracking yet)

    deleted_reason = "retention_policy"
    deletion_mode = "auto-retention"

    for c in candidates:
        bucket = c["bucket"]
        key = c["object_key"]
        exists = c["s3_exists"]

        if exists:
            try:
                s3.delete_object(Bucket=bucket, Key=key)
                _mark_deleted_success(
                    bucket=bucket,
                    key=key,
                    deleted_reason=deleted_reason,
                    deletion_mode=deletion_mode,
                )
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

    # Mock notification (console + log)
    _notify_retention(
        {
            "files_deleted": deleted_count,
            "total_size_bytes": total_size_bytes,
        }
    )


if __name__ == "__main__":
    run_retention_check()
