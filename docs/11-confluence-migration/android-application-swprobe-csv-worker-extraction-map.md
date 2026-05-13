# Android Application SWProbe CSV Worker Extraction Map

Last migrated into this repo: 2026-05-13

Source section:

```text
Android application - swprobe
Page ID: 15073281
Destination repo: C:\Project\Docker compose\s3-open-csv-worker
```

This source section is older than the current Android/backend architecture. Use it as Android offline-file and legacy CSV context, not as proof that CSV is the final Android offline format or that this worker owns online HTTP telemetry.

## CSV Worker Interpretation

The Android source describes earlier mobile file behavior:

- Android generated local CSV files in older SWProbe generations.
- Android uploaded only closed files.
- Android attempted queued files oldest first.
- Failed uploads stayed queued for retry.
- Local files were deleted only after upload success.
- S3 Stage-1 used plain CSV as the verified offline parser path.

For this repository, the still-current responsibility is:

```text
S3 object bytes -> CSV or supported NDJSON parser -> soft_data
  -> telemetry_etl_records -> s3_processed_files lifecycle
```

CSV remains Stage-1 verified/legacy support. Current/future Android offline replay should use NDJSON/JSONL with one complete `2.3.0` telemetry envelope per line, but this repo's current NDJSON parser still targets the older `T2.2` / `2.2` branch.

## Source Actuality For This Repo

| Android source knowledge | Actuality | How to use in this repo |
| --- | --- | --- |
| MQTT realtime path | Legacy predecessor | Not this worker. Online telemetry belongs to the telemetry ingestor. |
| FTP/ZIP archive path | Legacy predecessor | Preserve only as history for older file-delivery concepts. |
| Android local CSV files | Stage-1/legacy parser input | This worker owns CSV parsing into canonical storage for legacy/import paths. |
| Android S3 Stage-1 CSV upload | Current historical parser responsibility | Parse CSV objects after upload/metadata path points to this worker. |
| Closed-file-only upload | Upstream client reliability rule | Parser should assume the object is stable after MinIO/S3 download; it cannot verify Android local file closure. |
| Oldest-first queue processing | Upstream client queue rule | Parser lifecycle should record object key and counters; do not infer business order only from parse time. |
| Keep failed files queued | Upstream client reliability rule | Parser errors should be clear enough for Android/S3 operators to decide whether client retry or file regeneration is needed. |
| Delete local file after upload success | Upstream client rule | Parser success is separate from Android deletion; `s3_processed_files` records parser outcome after upload. |
| NDJSON/JSONL offline replay | Current/future Android direction | This repo may own it later, but current code must be upgraded from `T2.2` to `T2.3.0` before claiming v2.3.0 support. |

## Current Code Reality Checked

Current local code:

- `app/main.py` downloads object bytes and routes by suffix.
- CSV/default path calls `process_csv_bytes()`.
- `.ndjson`, `.jsonl`, `.ndjson.gz`, and `.jsonl.gz` call `process_ndjson_bytes()`.
- `app/csv_processor.py` requires parseable `timestamp`, non-empty `deviceId`, parseable `latitude`, and parseable `longitude`.
- CSV maps known columns through `CSV_TO_DB`, writes `source='s3-open'`, and preserves `raw_payload` plus `raw_payload_text`.
- `app/ndjson_processor.py` currently requires `EN.TP == T2.2` and writes `_etl_contract_version = 2.2`, `_etl_protocol_version = T2.2`, and `_etl_branch = offline_ndjson_v22`.
- `app/db.py` inserts rows into `soft_data` and best-effort `telemetry_etl_records`.
- `app/main.py` records parser lifecycle and idempotency in `s3_processed_files`.

Current tracked caveats:

- `.ping` or other diagnostic/upload-proof files should be ignored or marked non-telemetry instead of falling through to CSV parsing.
- `.csv.gz` is detected by suffix but the CSV parser currently decodes bytes directly as UTF-8.
- Android `T2.3.0` / `2.3.0` NDJSON replay is not implemented yet.

## Parser Acceptance From This Source

For this repo, Android offline file parsing is accepted when:

1. An S3/MinIO object is delivered to this worker for parsing.
2. The object has a supported telemetry file shape.
3. CSV rows or supported NDJSON lines are parsed line by line.
4. Valid rows are inserted into `soft_data`.
5. `telemetry_etl_records` captures projection-readiness/routing observability where supported.
6. `s3_processed_files` records lifecycle status and row counters.
7. Parser success is not confused with presign success, upload metadata success, frontend visibility, or Traccar projection.

## Failure Classification

| Symptom | CSV-worker classification |
| --- | --- |
| Android says file remains queued | Upstream client state; no parser action until object is delivered. |
| Object exists but this worker has no lifecycle row | Notification/routing/worker execution gap. |
| CSV row missing timestamp/device/coordinates | Row validation failure; count as failed row. |
| `.csv.gz` fails UTF-8 decode | Known implementation gap until gzip CSV support is added or disabled. |
| NDJSON `T2.3.0` file fails | Expected current gap; parser targets `T2.2` today. |
| `s3_processed_files` success but no Map/History data | Check inserted `soft_data`, FastAPI reads, frontend, and downstream projection. |
| Traccar missing after parser success | Downstream projection/sync issue, not CSV parser ownership. |

## Documents Updated From This Extraction

- `docs/01-current-state/active-tasks.md`
- `docs/02-purpose-and-boundaries/overview.md`
- `docs/03-core-workflows/android-swprobe-csv-legacy-boundary/overview.md`
- `docs/03-core-workflows/csv-file-processing/overview.md`
- `docs/03-core-workflows/swprobes-open-csv-to-soft-data/overview.md`
- `docs/03-core-workflows/s3-open-stage1-csv-processing/overview.md`

## Secret Handling

Do not store live MinIO credentials, database passwords, private object URLs, presigned URL signatures, Traccar credentials, Android account secrets, or private runtime paths in this repo's Markdown.
