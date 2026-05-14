# SWProbes Open CSV Worker Extraction Map

Last migrated into this repo: 2026-05-13

Source section: Confluence `SWProbes Open`

## Extraction Decision

The S3 Open offline file parser slice belongs in `s3-open-csv-worker`.

This repository owns:

- downloading S3/MinIO objects for parsing,
- detecting CSV vs NDJSON/JSONL by object key extension,
- parsing CSV rows,
- parsing currently implemented NDJSON/JSONL rows,
- validating required telemetry fields,
- inserting valid rows into `soft_data`,
- preserving raw payload/audit data,
- writing best-effort `telemetry_etl_records`,
- tracking idempotency and lifecycle in `s3_processed_files`,
- retention preview/history/dry-delete surfaces for processed objects.

This repository does not own:

- S3 presign generation,
- MinIO object-created metadata ingestion before parser execution,
- Android online HTTP Open `2.3.0` ingestion,
- WebSocket realtime telemetry,
- direct Traccar API calls,
- Traccar credentials,
- Traccar sync retries/admin workflows,
- account/customer/device business workflows outside file row parsing.

## Current Knowledge To Keep

The current offline file parsing sequence is:

```text
probe
  -> S3 presign/PUT
  -> MinIO/S3 object
  -> upload metadata worker
  -> s3-open-csv-worker /minio-webhook
  -> download object bytes
  -> CSV or NDJSON parser
  -> soft_data
  -> telemetry_etl_records
  -> latest/history/projection services later
```

Parser success is later than presign success and object-arrival metadata success. It is the first point where uploaded file content becomes canonical Simphony telemetry rows.

## Current Versus Predecessor

Older SWProbes Open pages describe the November/December 2025 plan:

```text
SWProbe/SXProbe = WS + S3 + plain CSV + Python ETL
```

For this repository:

- S3 offline parsing survived as an active responsibility.
- CSV is Stage-1 verified and remains legacy/import support.
- Python ETL became concrete worker code here for the CSV/file-to-database slice.
- WS primary telemetry, MQTT, FTP, ZIP upload, NiFi, and Redis-stream-only persistence are historical unless explicitly reopened.

## NDJSON/v2.3.0 Reality

Current Android online telemetry belongs to HTTP Open `2.3.0` / `T2.3.0` through the telemetry ingestor.

Future Android offline replay should use NDJSON/JSONL with one v2.3.0 envelope per line.

This repo already contains `app/ndjson_processor.py` and routes `.ndjson`, `.jsonl`, `.ndjson.gz`, and `.jsonl.gz` to it. However, that parser currently validates `EN.TP == T2.2` and writes `contract_version=2.2`, `protocol_version=T2.2`, and `offline_ndjson_v22` metadata. Therefore it is not final v2.3.0 offline support yet.

## Safety Rules

- Do not copy secrets from Confluence into repository Markdown, environment examples, tests, or code.
- Do not call Traccar from this worker.
- Do not invent GPS or identity fields only to make downstream Traccar projection easier.
- Do not treat `.ping` proof files as telemetry rows.
- Investigate missing telemetry by walking presign, PUT, object metadata, parser status, `soft_data`, `telemetry_etl_records`, latest/history APIs, then downstream Traccar projection.
