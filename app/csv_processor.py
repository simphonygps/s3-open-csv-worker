# app/csv_processor.py

import csv
import io
import logging
from datetime import datetime
from typing import Dict, List, Any

from .db import insert_soft_data_rows

logger = logging.getLogger("s3-open-csv-worker")

# CSV column name -> DB column name (match existing soft_data schema)
CSV_TO_DB: Dict[str, str] = {
    "timestamp": "timestamp",
    "deviceId": "deviceid",  # <-- important: deviceid, not device_id
    "latitude": "latitude",
    "longitude": "longitude",
    "altitude": "altitude",
    "gpsAccuracy": "gpsAccuracy",
    "speed": "speed",
    "bearing": "bearing",
    "pressure": "pressure",
    "calculatedAltitude": "calculatedAltitude",
    "accelX": "accelX",
    "accelY": "accelY",
    "accelZ": "accelZ",
    "gyroX": "gyroX",
    "gyroY": "gyroY",
    "gyroZ": "gyroZ",
    "magX": "magX",
    "magY": "magY",
    "magZ": "magZ",
    "batteryProbe": "batteryProbe",
    "batteryLevel": "batteryLevel",
    "batteryVoltage": "batteryVoltage",
    "bmsBatteryVoltage": "bmsBatteryVoltage",
    "signalStrength": "signalStrength",
    "temperature": "temperature",
    "satellitesInView": "satellitesInView",
    "satellitesInUse": "satellitesInUse",
    "inputVoltage": "inputVoltage",
    "bmsSoc": "bmsSoc",
    "chargingStatus": "chargingStatus",
}


def _parse_timestamp(value: str):
    """
    Parse ISO 8601 timestamp with timezone, e.g.:
      2025-11-17T18:26:02.542-08:00
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        logger.exception("[CSV] Failed to parse timestamp: %r", value)
        return None


def _to_float(value: str):
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _to_int(value: str):
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return None
    try:
        # Some CSVs may contain "12.0" – handle that gracefully
        return int(float(value))
    except ValueError:
        return None


def process_csv_bytes(csv_bytes: bytes) -> None:
    """
    Parse CSV bytes, map to DB columns, and insert into soft_data.

    Behavior:
      - Decode bytes as UTF-8.
      - Use DictReader to parse header + rows.
      - Map CSV columns using CSV_TO_DB.
      - Convert types (timestamp, ints, floats, text).
      - Set source='s3-open' for all rows from this worker.
      - Insert in batches via insert_soft_data_rows().
    """
    if not csv_bytes:
        logger.warning("[CSV] Empty CSV payload, nothing to process.")
        return

    try:
        text_stream = io.StringIO(csv_bytes.decode("utf-8", errors="replace"))
    except Exception:
        logger.exception("[CSV] Failed to decode CSV bytes as UTF-8.")
        raise

    reader = csv.DictReader(text_stream)

    fieldnames = reader.fieldnames or []
    logger.info("[CSV] Fieldnames: %s", fieldnames)

    batch: List[Dict[str, Any]] = []
    total_rows = 0
    BATCH_SIZE = 1000

    for row in reader:
        total_rows += 1
        mapped: Dict[str, Any] = {}

        for csv_col, db_col in CSV_TO_DB.items():
            raw = row.get(csv_col)

            if csv_col == "timestamp":
                mapped[db_col] = _parse_timestamp(raw)
            elif csv_col in ("deviceId", "chargingStatus"):
                mapped[db_col] = (raw or "").strip() or None
            elif csv_col in ("satellitesInView", "satellitesInUse"):
                mapped[db_col] = _to_int(raw)
            else:
                # Assume numeric – float
                mapped[db_col] = _to_float(raw)

        # Set source for rows coming from S3-Open worker
        mapped["source"] = "s3-open"

        batch.append(mapped)

        if len(batch) >= BATCH_SIZE:
            logger.info("[CSV] Inserting batch of %d rows into soft_data...", len(batch))
            insert_soft_data_rows(batch)
            batch.clear()

    if batch:
        logger.info("[CSV] Inserting final batch of %d rows into soft_data...", len(batch))
        insert_soft_data_rows(batch)

    logger.info("[CSV] Finished processing CSV: %d data rows inserted (source='s3-open').", total_rows)
