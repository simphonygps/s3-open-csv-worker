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
        # Uses the same connection/settings as the real worker logic.
        ensure_processed_table()
        return {"status": "ok"}
    except Exception as e:
        logger.exception("DB health check failed")
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
# Core processing logic
# ---------------------------------------------------------------------------

def handle_object(bucket: str, key: str):
    if is_object_processed(bucket, key):
        logger.info("Object already processed, skipping: %s/%s", bucket, key)
        return

    logger.info("Downloading object: %s/%s", bucket, key)
    csv_bytes = download_object_to_bytes(bucket, key)

    logger.info("Processing CSV for object: %s/%s", bucket, key)
    process_csv_bytes(csv_bytes)

    mark_object_processed(bucket, key)
    logger.info("Marked object processed: %s/%s", bucket, key)


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
