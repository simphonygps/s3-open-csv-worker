# Architecture Design s3-open-csv-worker Extraction Map

Source: Confluence `Architecture, design and high-level plans`
Destination repository: `C:\Project\Docker compose\s3-open-csv-worker`
Migration date: 2026-05-13

## Migration Judgment

The Confluence architecture/design section is very old. For this repository it is useful as origin material for the S3-compatible Open Service parser/ETL slice, not as a current instruction to make this worker own the whole upload platform or Traccar integration.

Current code and docs show that this repo owns offline object parsing and processed-file lifecycle after S3 object delivery.

## Current Code Reality Checked

- `app/main.py` exposes `/health`, `/health/db`, `/health/s3`, and `POST /minio-webhook`.
- `handle_object()` checks `s3_processed_files` idempotency, marks processing started, downloads object bytes, selects a parser branch by key suffix, and marks success or error.
- `.ndjson`, `.jsonl`, `.ndjson.gz`, and `.jsonl.gz` route to `process_ndjson_bytes()`.
- `.csv.gz` is detected as `csv_gz_file`, but the current CSV processor decodes raw bytes as UTF-8 and does not decompress gzip.
- Unknown suffixes currently fall through to CSV processing.
- `app/csv_processor.py` requires parseable `timestamp`, non-empty `deviceId`, parseable `latitude`, and parseable `longitude`.
- `app/csv_processor.py` maps known CSV columns to `soft_data`, sets `source='s3-open'`, and preserves raw payload fields.
- `app/ndjson_processor.py` currently validates `EN.TP == T2.2` and writes `contract_version=2.2` / `offline_ndjson_v22`; it is not completed Android `T2.3.0` / `2.3.0` offline replay.

## Current Knowledge To Keep

The current worker-owned path is:

```text
S3/MinIO object
  -> /minio-webhook
  -> object byte download
  -> CSV or implemented NDJSON branch
  -> row validation
  -> soft_data
  -> best-effort telemetry_etl_records
  -> s3_processed_files lifecycle
```

Parser success is not the same as presign success, upload success, object-created metadata success, FastAPI visibility, frontend visibility, or Traccar projection.

## Predecessor Knowledge From Old Architecture Pages

| Old architecture/design knowledge | s3-open-csv-worker treatment |
| --- | --- |
| S3-compatible Open Service | Historical design root for the offline parser pipeline. |
| Plain CSV Stage-1 parser | Current/legacy parser responsibility here. |
| Python ETL replacing older offline handling | Origin history for this worker's parser role. |
| NiFi as final ETL owner | Predecessor direction, not current local code. |
| FTP/ZIP upload delivery | Predecessor file-delivery history outside this worker. |
| Redis Streams as telemetry persistence | Historical ingestion architecture outside this parser. |
| WS Open as primary telemetry | Predecessor online path; current online Android telemetry belongs to the telemetry ingestor. |
| Traccar-first or direct-Traccar flow | Superseded. This worker may prepare projection metadata, but Traccar execution is downstream. |
| CSV-only Android offline future | Stage-1/legacy support. Future Android offline `2.3.0` should use NDJSON/JSONL unless a newer decision says otherwise. |

## Current Boundary For This Repo

Owned here:

- object byte download after webhook notification,
- parser branch selection by object key,
- CSV parsing and row validation,
- implemented older/intermediate NDJSON parsing,
- canonical `soft_data` insertion,
- best-effort `telemetry_etl_records` projection-readiness/routing metadata,
- `s3_processed_files` processing lifecycle and counters,
- retention preview/history/dry-delete HTTP surfaces,
- CLI retention deletion only when explicitly enabled.

Not owned here:

- S3 presign generation,
- upload URL/header policy,
- MinIO bucket or edge routing policy,
- upload metadata worker ownership of `uploads`,
- Android local queue behavior,
- Android online HTTP Open `2.3.0` ingestion,
- completed Android `T2.3.0` / `2.3.0` NDJSON replay until code is aligned,
- FastAPI latest/history APIs,
- frontend Map/History behavior,
- Traccar API calls, credentials, sync retries, or admin operations.

## Verification Implication

Old architecture pages often describe the whole S3 Open path as one ETL flow. Current verification must name the exact layer:

```text
presign accepted
  -> PUT object stored
  -> ObjectCreated metadata recorded
  -> parser downloaded object bytes
  -> parser branch selected
  -> rows validated
  -> soft_data rows inserted
  -> telemetry_etl_records written when in scope
  -> s3_processed_files status and counters recorded
  -> customer-visible telemetry verified downstream
  -> Traccar projection verified downstream, if needed
```

This repository owns the parser, canonical-row insertion, ETL metadata, lifecycle counters, and retention surfaces only.

## Files Updated From This Pass

- `docs/1-current-state/active-tasks.md`
- `docs/2-project-functionality/purpose-and-boundaries/overview.md`
- `docs/2-project-functionality/purpose-and-boundaries/architecture-and-engineering-parser-boundary.md`
- `docs/2-project-functionality/core-workflows/s3-open-stage1-csv-processing/overview.md`
- `docs/2-project-functionality/core-workflows/csv-file-processing/overview.md`
- `docs/2-project-functionality/core-workflows/swprobes-open-csv-to-soft-data/overview.md`
- `docs/2-project-functionality/core-workflows/android-swprobe-csv-legacy-boundary/overview.md`
- `docs/1-current-state/confluence-migration/architecture-design-csv-worker-extraction-map.md`

## Secret Handling

Do not store MinIO/S3 access keys, database credentials, Redis credentials, webhook tokens, presigned URL signatures, private VPS paths, Traccar credentials, or private runtime endpoints in repository Markdown. Use placeholders and sanitized object keys instead.
