# Active Tasks

No active implementation task is declared by this bootstrap.

Initial next documentation task:

- inspect code and document exact CSV header aliases, row validation rules, idempotency key, and DB columns.
- preserve Traccar projection boundary: this worker writes Simphony canonical rows and ETL/projection metadata, but does not call Traccar directly.

## 2026-05-13 Source-Of-Truth Update

Traccar integration knowledge has been absorbed for this worker repo. Current decision: `s3-open-csv-worker` owns CSV-to-`soft_data` parsing and projection-readiness metadata only. Dedicated backend sync/projection services own Traccar compatibility execution.

## 2026-05-13 SWProbes Open Redo

`SWProbes Open` knowledge has been reapplied specifically to this repo.

Current decision: preserve S3 Open offline file parsing as this worker's active responsibility. Treat CSV as Stage-1 verified/legacy support. Treat existing NDJSON parser code as implemented older/intermediate `T2.2` / `2.2` support, not yet current Android `T2.3.0` / `2.3.0` offline replay.

Predecessor/history for this repo: WS primary telemetry, MQTT, FTP, ZIP, NiFi, and Redis-stream-only persistence. Current online Android HTTP Open `2.3.0` belongs to the telemetry ingestor, not this worker.

Implementation follow-up: decide whether this repo owns the future v2.3.0 NDJSON/JSONL parser. If yes, update `app/ndjson_processor.py` from `T2.2` / `2.2` / `offline_ndjson_v22` to the final v2.3.0 contract and add focused tests.

## 2026-05-13 S3 Open Service Redo

Repository: `s3-open-csv-worker`

Local path: `C:\Project\Docker compose\s3-open-csv-worker`

`S3 Open service` knowledge has been reconciled with this repo's current source of truth and code.

Current decision: keep this repo focused on the S3 object parsing and retention slice. Historical S3 Open Stage-1 pages are relevant here only after an object has arrived in MinIO/S3 and a webhook points the worker to the object. Presign generation, device/tenant validation, upload metadata rows, and customer-facing APIs remain outside this repo.

Active follow-ups:

- verify or harden `.ping` and other non-telemetry diagnostic files so they are ignored or marked as non-telemetry instead of falling through to CSV processing.
- verify/fix `.csv.gz` support before describing it as complete; current code detects the suffix but CSV processing does not decompress gzip bytes.
- decide whether v2.3.0 NDJSON/JSONL offline replay belongs here. Current NDJSON code is still the older/intermediate `T2.2` / `2.2` path.
- preserve `s3_processed_files` lifecycle counters as the operational proof of parser success, including partial row failures.
