# app/csv_processor.py

import csv
import io
from datetime import datetime
from typing import Dict, List, Any

from .db import insert_soft_data_rows

# CSV column name -> DB column name (must match soft_data exactly)
CSV_TO_DB: Dict[str, str] = {
    # core
    "timestamp": "timestamp",          # "timestamp" column (quoted in SQL)
    "deviceId": "deviceid",           # deviceid (lowercase)
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
    "gpsAccuracy": "gpsAccuracy",     # camelCase column in DB
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
    return datetime.fromisoformat(value)


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


def process_csv_bytes(csv_bytes: bytes):
    """
    Parse CSV bytes, map to DB columns, and insert into soft_data.

    - Uses CSV_TO_DB to map CSV header -> soft_data column
    - Adds source='s3-open' for all inserted rows
    """
    text_stream = io.StringIO(csv_bytes.decode("utf-8"))
    reader = csv.DictReader(text_stream)

    batch: List[Dict[str, Any]] = []

    for row in reader:
        mapped: Dict[str, Any] = {}

        for csv_col, db_col in CSV_TO_DB.items():
            raw = row.get(csv_col, "")

            if csv_col == "timestamp":
                mapped[db_col] = _parse_timestamp(raw)

            elif csv_col == "deviceId":
                # deviceid is text in DB
                mapped[db_col] = raw or None

            elif csv_col in ("satellitesInView", "satellitesInUse", "bmsSoc", "chargingStatus"):
                mapped[db_col] = _to_int(raw)

            else:
                # everything else we treat as numeric float
                mapped[db_col] = _to_float(raw)

        # override / set source column
        mapped["source"] = "s3-open"

        batch.append(mapped)

        # Insert in chunks to avoid huge transactions
        if len(batch) >= 1000:
            insert_soft_data_rows(batch)
            batch.clear()

    # Insert remaining rows
    if batch:
        insert_soft_data_rows(batch)
