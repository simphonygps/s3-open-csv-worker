# app/csv_processor.py

import csv
import io
import logging

logger = logging.getLogger("s3-open-csv-worker")


def process_csv_bytes(data: bytes) -> None:
    """
    Dry-run CSV processing.

    Current behavior:
      - Decode bytes as UTF-8 (replace errors).
      - Parse CSV using DictReader.
      - Log header (fieldnames).
      - Count rows and log the total.

    IMPORTANT:
      - This version does NOT write to PostgreSQL yet.
      - It is only used to validate CSV structure and ensure
        that parsing works end-to-end.
    """
    if not data:
        logger.warning("[CSV] Empty data received, nothing to parse.")
        return

    try:
        text = data.decode("utf-8", errors="replace")
    except Exception:
        logger.exception("[CSV] Failed to decode CSV bytes as UTF-8")
        raise

    f = io.StringIO(text)
    reader = csv.DictReader(f)

    fieldnames = reader.fieldnames or []
    logger.info("[CSV] Fieldnames: %s", fieldnames)

    row_count = 0
    for row in reader:
        row_count += 1
        # We don't log each row to avoid noisy logs.
        # If needed, we can add debug-level logging later.

    logger.info("[CSV] Parsed %d data rows.", row_count)
