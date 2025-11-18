# app/csv_processor.py
import csv
import io
from datetime import datetime
from typing import Dict, List, Any

from .db import insert_soft_data_rows

# CSV column name -> DB column name
CSV_TO_DB = {
    "timestamp": "timestamp",
    "deviceId": "device_id",
    "latitude": "latitude",
    "longitude": "longitude",
    "altitude": "altitude",
    "gpsAccuracy": "gps_accuracy",
    "speed": "speed",
    "bearing": "bearing",
    "pressure": "pressure",
    "calculatedAltitude": "calculated_altitude",
    "accelX": "accel_x",
    "accelY": "accel_y",
    "accelZ": "accel_z",
    "gyroX": "gyro_x",
    "gyroY": "gyro_y",
    "gyroZ": "gyro_z",
    "magX": "mag_x",
    "magY": "mag_y",
    "magZ": "mag_z",
    "batteryProbe": "battery_probe",
    "batteryLevel": "battery_level",
    "batteryVoltage": "battery_voltage",
    "bmsBatteryVoltage": "bms_battery_voltage",
    "signalStrength": "signal_strength",
    "temperature": "temperature",
    "satellitesInView": "satellites_in_view",
    "satellitesInUse": "satellites_in_use",
    "inputVoltage": "input_voltage",
    "bmsSoc": "bms_soc",
    "chargingStatus": "charging_status",
}


def _parse_timestamp(value: str):
    if not value:
        return None
    # Your CSV uses ISO 8601 with timezone, e.g. "2025-11-17T18:26:02.542-08:00"
    # Python 3.11+ supports fromisoformat for this
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
            elif csv_col in ("deviceId", "chargingStatus"):
                mapped[db_col] = raw or None
            elif csv_col in (
                "satellitesInView",
                "satellitesInUse",
            ):
                mapped[db_col] = _to_int(raw)
            else:
                # Assume numeric – float
                mapped[db_col] = _to_float(raw)

        batch.append(mapped)

        # Insert in chunks to avoid huge transactions
        if len(batch) >= 1000:
            insert_soft_data_rows(batch)
            batch.clear()

    # Insert remaining rows
    if batch:
        insert_soft_data_rows(batch)
