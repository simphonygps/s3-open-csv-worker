# app/csv_processor.py

import csv
import io
import logging
from datetime import datetime
from typing import Dict, List, Any

from .db import insert_soft_data_rows

logger = logging.getLogger("s3-open-csv-worker")

# CSV column name -> DB column name (must match soft_data exactly)
CSV_TO_DB: Dict[str, str] = {
    # core
    "timestamp": "timestamp",           # "timestamp" column (quoted in SQL)
    "deviceId": "deviceid",            # deviceid (lowercase)
    "latitude": "latitude",
    "longitude": "longitude",
    "altitude": "altitude",
    "pressure": "pressure",
    "calculatedAltitude": "calculatedaltitude",  # lower-case in DB

    # linear acceleration
    "accelX": "accelx",
    "accelY": "accely",
    "accelZ": "accelz",

    # gyroscope
    "gyroX": "gyrox",
    "gyroY": "gyroy",
    "gyroZ": "gyroz",

    # magnetometer
    "magX": "magx",
    "magY": "magy",
    "magZ": "magz",

    # GPS / motion
    "gpsAccuracy": "gpsAccuracy",      # camelCase column in DB
    "speed": "speed",
    "bearing": "bearing",

    # power / environment
    "batteryProbe": "batteryProbe",
    "batteryLevel": "batteryLevel",
    "batteryVoltage": "batteryVoltage",
    "bmsBatteryVoltage": "bmsBatteryVoltage",
    "signalStrength": "signalStrength",
    "temperature": "temperature",

    # satellites & power
    "satellitesInView": "satellitesInView",
    "satellitesInUse": "satellitesInUse",
    "inputVoltage": "inputVoltage",
    "bmsSoc": "bmsSoc",
    "chargingStatus": "chargingStatus",
}


def _parse_timestamp(value: str):
    if not value:
        return None
    # CSV uses ISO 8601 with timezone, e.g. "2025-11-17T18:26:02.542-08:00"
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _to_float(value: str):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _to_int(value: str):
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def process_csv_bytes(csv_bytes: bytes) -> dict[str, int]:
    """
    Parse CSV bytes, validate, map to DB columns, and insert into soft_data.

    Validation (basic):
      - Required fields: timestamp, deviceId, latitude, longitude
      - Row is skipped (not inserted) if:
          * timestamp is missing or cannot be parsed, OR
          * deviceId is empty, OR
          * latitude/longitude cannot be parsed as floats

    Returns a summary dict:
        {
          "rows_total": <int>,     # rows seen in CSV (excluding header)
          "rows_inserted": <int>,  # rows successfully inserted into soft_data
          "rows_failed": <int>     # rows skipped due to validation errors
        }
    """
    text_stream = io.StringIO(csv_bytes.decode("utf-8"))
    reader = csv.DictReader(text_stream)

    batch: List[Dict[str, Any]] = []

    rows_total = 0
    rows_inserted = 0
    rows_failed = 0

    for row in reader:
        rows_total += 1

        # -----------------------------
        # Basic validation
        # -----------------------------
        errors: List[str] = []

        raw_ts = (row.get("timestamp") or "").strip()
        ts = _parse_timestamp(raw_ts)
        if not raw_ts or ts is None:
            errors.append("invalid timestamp")

        raw_device_id = (row.get("deviceId") or "").strip()
        if not raw_device_id:
            errors.append("missing deviceId")

        raw_lat = (row.get("latitude") or "").strip()
        raw_lon = (row.get("longitude") or "").strip()
        lat = _to_float(raw_lat)
        lon = _to_float(raw_lon)
        if lat is None or lon is None:
            errors.append("invalid latitude/longitude")

        if errors:
            rows_failed += 1
            # We log as WARNING so operator can see if something is wrong with CSV
            logger.warning(
                "[CSV] Skipping row %d due to validation errors: %s",
                rows_total,
                "; ".join(errors),
            )
            continue

        # -----------------------------
        # Mapping to DB columns
        # -----------------------------
        mapped: Dict[str, Any] = {}

        for csv_col, db_col in CSV_TO_DB.items():
            raw = row.get(csv_col, "")

            if csv_col == "timestamp":
                # already parsed as ts
                mapped[db_col] = ts

            elif csv_col == "deviceId":
                mapped[db_col] = raw_device_id

            elif csv_col == "latitude":
                mapped[db_col] = lat

            elif csv_col == "longitude":
                mapped[db_col] = lon

            elif csv_col in ("satellitesInView", "satellitesInUse", "bmsSoc", "chargingStatus"):
                mapped[db_col] = _to_int(raw)

            else:
                # everything else we treat as numeric float
                mapped[db_col] = _to_float(raw)

        # override / set source column
        mapped["source"] = "s3-open"

        batch.append(mapped)
        rows_inserted += 1

        # Insert in chunks to avoid huge transactions
        if len(batch) >= 1000:
            insert_soft_data_rows(batch)
            batch.clear()

    # Insert remaining rows
    if batch:
        insert_soft_data_rows(batch)

    logger.info(
        "[CSV] Summary: total=%d, inserted=%d, failed=%d",
        rows_total,
        rows_inserted,
        rows_failed,
    )

    return {
        "rows_total": rows_total,
        "rows_inserted": rows_inserted,
        "rows_failed": rows_failed,
    }
