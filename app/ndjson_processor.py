import gzip
import io
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from .db import insert_soft_data_rows

logger = logging.getLogger("s3-open-csv-worker")


def _decode_payload(data: bytes, payload_shape: str) -> str:
    if payload_shape == "ndjson_gz_file":
        return gzip.decompress(data).decode("utf-8")
    return data.decode("utf-8")


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _parse_ts(ts: Any) -> Optional[datetime]:
    if not isinstance(ts, str) or not ts.strip():
        return None
    iso = ts.strip()
    if iso.endswith("Z"):
        iso = iso[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(iso)
    except Exception:
        return None


def _group_first_row(payload: Dict[str, Any], code: str) -> tuple[Dict[str, int], List[Any]]:
    g = payload.get(code)
    if not isinstance(g, dict):
        return {}, []
    fields = g.get("f")
    rows = g.get("r")
    if not isinstance(fields, list) or not isinstance(rows, list) or len(rows) == 0:
        return {}, []
    row0 = rows[0]
    if not isinstance(row0, list):
        return {}, []
    return ({str(k): i for i, k in enumerate(fields)}, row0)


def _pick(idx: Dict[str, int], row: List[Any], code: str) -> Any:
    i = idx.get(code)
    if i is None or i >= len(row):
        return None
    return row[i]


def process_ndjson_bytes(data: bytes, payload_shape: str) -> dict[str, int]:
    text = _decode_payload(data, payload_shape)
    stream = io.StringIO(text)

    rows_total = 0
    rows_inserted = 0
    rows_failed = 0
    batch: List[Dict[str, Any]] = []

    for line_no, line in enumerate(stream, start=1):
        line = line.strip()
        if not line:
            continue
        rows_total += 1

        try:
            payload = json.loads(line)
        except Exception:
            rows_failed += 1
            logger.warning("[NDJSON] line %d invalid json", line_no)
            continue

        en = payload.get("EN")
        if not isinstance(en, dict):
            rows_failed += 1
            logger.warning("[NDJSON] line %d missing EN", line_no)
            continue

        tp = str(en.get("TP") or "").strip()
        pr = str(en.get("PR") or payload.get("probe_type") or "sw").strip().lower()
        di = str(en.get("DI") or "").strip()
        sq = _to_int(en.get("SQ"))
        ts = _parse_ts(en.get("TS"))

        if tp != "T2.2" or not di or sq is None or ts is None:
            rows_failed += 1
            logger.warning("[NDJSON] line %d invalid EN fields", line_no)
            continue

        gp_idx, gp_row = _group_first_row(payload, "GP")
        im_idx, im_row = _group_first_row(payload, "IM")
        sy_idx, sy_row = _group_first_row(payload, "SY")

        lat = _to_float(_pick(gp_idx, gp_row, "LA"))
        lon = _to_float(_pick(gp_idx, gp_row, "LO"))
        if lat == 0.0 and lon == 0.0:
            lat, lon = None, None

        mapped: Dict[str, Any] = {
            "timestamp": ts,
            "deviceid": di,
            "source": "s3-open",
            "msg_type": str(payload.get("message_type") or "telemetry_envelope"),
            "msg_probe": pr,
            "msg_seq": sq,
            "latitude": lat,
            "longitude": lon,
            "altitude": _to_float(_pick(gp_idx, gp_row, "AL")),
            "bearing": _to_float(_pick(gp_idx, gp_row, "HD")),
            "speed_gps": _to_float(_pick(gp_idx, gp_row, "SP")),
            "gpsAccuracy": _to_float(_pick(gp_idx, gp_row, "AC")),
            "satellites": _to_int(_pick(gp_idx, gp_row, "SV")) or _to_int(_pick(gp_idx, gp_row, "SU")),
            "accelx": _to_float(_pick(im_idx, im_row, "AX")),
            "accely": _to_float(_pick(im_idx, im_row, "AY")),
            "accelz": _to_float(_pick(im_idx, im_row, "AZ")),
            "uptime_sec": _to_int(_pick(sy_idx, sy_row, "UP")),
            "battery_probe": _to_float(_pick(sy_idx, sy_row, "BP")),
            "raw_payload": payload,
            "raw_payload_text": line,
            "_etl_flow_id": "sw_http_v22" if pr == "sw" else "sx_http_v22",
            "_etl_transport": "file",
            "_etl_source": "s3-open",
            "_etl_probe_type": pr,
            "_etl_protocol_version": "T2.2",
            "_etl_contract_version": "2.2",
            "_etl_message_type": str(payload.get("message_type") or "telemetry_envelope"),
            "_etl_payload_shape": payload_shape,
            "_etl_device_identifier_type": "software_device_id" if pr == "sw" else "hardware_device_id",
            "_etl_branch": "offline_ndjson_v22",
            "_etl_routing_reason": "matched s3-open ndjson flow",
        }

        batch.append(mapped)
        rows_inserted += 1
        if len(batch) >= 1000:
            insert_soft_data_rows(batch)
            batch.clear()

    if batch:
        insert_soft_data_rows(batch)

    logger.info(
        "[NDJSON] Summary: total=%d, inserted=%d, failed=%d",
        rows_total,
        rows_inserted,
        rows_failed,
    )

    return {
        "rows_total": rows_total,
        "rows_inserted": rows_inserted,
        "rows_failed": rows_failed,
    }
