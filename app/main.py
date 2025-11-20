# app/main.py

import logging

from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import JSONResponse

from .config import get_settings
from .db import ensure_processed_table, is_object_processed, mark_object_processed
from .s3_client import download_object_to_bytes
from .csv_processor import process_csv_bytes

settings = get_settings()
app = FastAPI(title="s3-open-csv-worker")

logger = logging.getLogger("s3-open-csv-worker")
logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Health endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    """Simple liveness probe."""
    return {"status": "ok"}


@app.get("/health/db")
async def health_db():
    """
    Readiness probe: verifies DB connectivity by reusing the same logic
    that ensures the idempotency table on startup.
    """
    try:
        ensure_processed_table()
        return {"status": "ok"}
    except Exception as e:
        logger.exception("DB health check failed")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "detail": str(e)},
        )


@app.get("/health/s3")
async def health_s3():
    """
    S3/MinIO health probe.

    We implement it in the simplest and most reliable way:
    - Reuse the existing download_object_to_bytes() helper.
    - Try to download a known small test object from MinIO.
    If we can read non-empty bytes, S3/MinIO is considered "ok".
    Otherwise we return 500 with the error.
    """
    bucket = "simphony-dev"
    key = "csv-worker/test-download.csv"

    try:
        logger.info("[S3-HEALTH] Checking S3 by downloading %s/%s", bucket, key)
        data = download_object_to_bytes(bucket, key)
        size = len(data) if data is not None else 0
        if size == 0:
            raise RuntimeError(f"Downloaded 0 bytes from {bucket}/{key}")

        logger.info(
            "[S3-HEALTH] Successfully downloaded %d bytes from %s/%s",
            size,
            bucket,
            key,
        )
        return {"status": "ok"}
    except Exception as e:
        logger.exception("S3 health check failed")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "detail": str(e)},
        )


# ---------------------------------------------------------------------------
# Startup hook
# ---------------------------------------------------------------------------

@app.on_event("startup")
def on_startup():
    logger.info("Starting s3-open-csv-worker...")
    ensure_processed_table()
    logger.info("Idempotency table ensured.")


# ---------------------------------------------------------------------------
# Core processing logic (S3 download + CSV dry-run + idempotency)
# ---------------------------------------------------------------------------

def handle_object(bucket: str, key: str):
    """
    Handle an S3/MinIO object notification.

    Current behavior:
      - Check idempotency (s3_processed_files)
      - Download object bytes from S3/MinIO
      - Log size and first 200 bytes
      - Dry-run CSV parsing (header + row count only)
      - Mark object as processed

    CSV → PostgreSQL insertion is intentionally NOT implemented yet.
    This keeps the pipeline simple for now: webhook → S3 download → CSV parse → DB mark.
    """
    logger.info("[S3] Handling object: %s/%s", bucket, key)

    # Idempotency check
    if is_object_processed(bucket, key):
        logger.info(
            "[S3] Already processed (found in s3_processed_files), skipping: %s/%s",
            bucket,
            key,
        )
        return

    # Download from S3/MinIO
    try:
        logger.info("[S3] Downloading object bytes from MinIO/S3: %s/%s", bucket, key)
        data = download_object_to_bytes(bucket, key)
        size = len(data) if data is not None else 0
        logger.info(
            "[S3] Downloaded %d bytes for object %s/%s",
            size,
            bucket,
            key,
        )
        preview = data[:200] if data else b""
        logger.info("[S3] First 200 bytes for %s/%s: %r", bucket, key, preview)
    except Exception:
        logger.exception(
            "[S3] Failed to download object %s/%s; not marking as processed",
            bucket,
            key,
        )
        # Do NOT mark as processed if download fails
        return

    # Dry-run CSV parsing
    try:
        logger.info("[CSV] Starting CSV dry-run processing for %s/%s", bucket, key)
        process_csv_bytes(data)
        logger.info("[CSV] Finished CSV dry-run processing for %s/%s", bucket, key)
    except Exception:
        logger.exception("[CSV] Failed to process CSV for %s/%s", bucket, key)
        # Do NOT mark as processed if CSV parsing fails
        return

    # Mark as processed only after successful download + CSV parse
    logger.info(
        "[S3] Marking object processed in s3_processed_files: %s/%s",
        bucket,
        key,
    )
    mark_object_processed(bucket, key)
    logger.info(
        "[S3] Marked object processed: %s/%s",
        bucket,
        key,
    )


# ---------------------------------------------------------------------------
# Webhook endpoint
# ---------------------------------------------------------------------------

@app.post("/minio-webhook")
async def minio_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    MinIO sends notifications here for ObjectCreated events.
    Payload format is similar to S3:
    {
      "EventName": "s3:ObjectCreated:Put",
      "Key": "path/to/file.csv",
      "Records": [...]
    }
    We support both MinIO-style and AWS-style payload formats.
    """
    payload = await request.json()
    logger.info("Received webhook: %s", payload)

    # Try S3-style "Records"
    records = payload.get("Records")
    if records:
        for record in records:
            s3_info = record.get("s3", {})
            bucket = s3_info.get("bucket", {}).get("name", settings.s3_bucket)
            obj = s3_info.get("object", {})
            key = obj.get("key")
            if not key:
                continue
            background_tasks.add_task(handle_object, bucket, key)

        return JSONResponse({"status": "ok", "records": len(records)})

    # MinIO-style fields
    event_name = payload.get("EventName") or payload.get("eventName")
    key = payload.get("Key") or payload.get("key")
    bucket = payload.get("Bucket") or payload.get("bucket") or settings.s3_bucket

    if event_name and "ObjectCreated" in event_name and key:
        background_tasks.add_task(handle_object, bucket, key)
        return JSONResponse({"status": "ok", "records": 1})

    logger.warning("Unknown webhook format, ignoring.")
    return JSONResponse({"status": "ignored"}, status_code=400)
