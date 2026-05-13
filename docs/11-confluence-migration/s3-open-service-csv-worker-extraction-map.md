# S3 Open Service CSV Worker Extraction Map

Repository: `s3-open-csv-worker`

Local path: `C:\Project\Docker compose\s3-open-csv-worker`

Source section: Confluence `S3 Open service`, including nested Stage-1 architecture, presigned upload, MinIO webhook, CSV processing, retention, and closure/report pages.

Migration date: 2026-05-13.

## Migration Judgment

Most S3 Open service pages are November 2025 Stage-1 implementation and verification history. For this repo, they are still useful because this worker is the concrete owner of the Stage-1 parser slice:

```text
MinIO ObjectCreated event -> s3-open-csv-worker -> soft_data + s3_processed_files
```

Treat the Confluence pages as historical source material unless current code confirms the behavior. Current code confirms CSV processing, NDJSON/JSONL routing for an older envelope branch, processed-file lifecycle, retention preview/history, and best-effort ETL/projection metadata.

## Current Worker Slice

This repo begins after the object already exists in MinIO/S3 and a webhook has arrived.

Current responsibilities confirmed by code:

- accept `POST /minio-webhook` in AWS `Records` style or MinIO-style `EventName` / `Key` shape,
- schedule background processing for ObjectCreated events,
- skip only objects already marked `status='success'` in `s3_processed_files`,
- mark processing start, success, and error states,
- download object bytes from MinIO/S3,
- route `.ndjson`, `.jsonl`, `.ndjson.gz`, and `.jsonl.gz` to the NDJSON parser,
- treat other keys as CSV, including `.csv` and currently also unrecognized suffixes,
- insert valid rows into `soft_data` with `source='s3-open'`,
- write best-effort `telemetry_etl_records`,
- preserve raw payload fields for audit/debug,
- expose read-only retention preview, history, and dry-delete endpoints,
- allow real delete mode only through the CLI retention worker when enabled.

## Stage-1 Historical Contract

The old S3 Open Stage-1 target was:

```text
device -> presigned PUT -> MinIO object -> ObjectCreated webhook
  -> ingestion metadata row -> CSV worker parse
  -> soft_data -> customer visibility later
```

For this worker, the relevant Stage-1 acceptance points were:

- ObjectCreated notification can trigger processing.
- Worker can download the uploaded object.
- CSV rows can be parsed and inserted into canonical telemetry storage.
- `s3_processed_files` records status, row counters, failures, and retention lifecycle.
- Duplicate successful objects are not inserted again.
- Retention can preview/history/delete old processed objects according to policy.

Presign generation, S3 key construction, tenant/device validation, upload rate limiting, and upload metadata normalization belong to sibling services, not this worker.

## CSV Contract Kept Here

Current code requires each valid CSV row to have:

- parseable `timestamp`,
- non-empty `deviceId`,
- parseable `latitude`,
- parseable `longitude`.

Rows failing these fields are skipped and counted as failed. Valid rows map known CSV columns into `soft_data`, preserve the original row as `raw_payload` and `raw_payload_text`, and set `source='s3-open'`.

The Confluence Stage-1 pages describe strict UTF-8 comma CSV processing. Current code decodes CSV bytes as UTF-8 and uses Python `csv.DictReader`; exact header handling is governed by `CSV_TO_DB` in `app/csv_processor.py`.

## Current Caveats And Gaps

- `.ping` and other diagnostic upload-proof files should not become telemetry. Current key detection defaults unknown suffixes to `csv_file`, so this should be verified or hardened.
- `.csv.gz` is detected as `csv_gz_file`, but current CSV processing passes raw bytes directly to `process_csv_bytes`; gzip decompression is implemented for NDJSON gzip only. Do not claim working `.csv.gz` support until this is fixed or tested.
- NDJSON/JSONL support exists but validates `EN.TP == T2.2` and writes `contract_version=2.2`. It is not completed Android `T2.3.0` / `2.3.0` offline replay.
- `rows_failed > 0` with no exception is still marked `success`. This is acceptable for partial-row CSV import, but operational alerts should use counters, not only status.
- Retention HTTP endpoints are read-only; real delete mode is CLI-only.

## Historical Items Not Owned Here

- Presigned URL contract and rate limits.
- MinIO bucket/policy creation.
- Upload metadata insertion into `public.uploads`.
- Android online HTTP `2.3.0` ingestion.
- WebSocket primary telemetry ingestion.
- Direct Traccar synchronization or Traccar API credentials.
- Customer/browser latest/history APIs.

## Source-Of-Truth Decision

This repo should document S3 Open service knowledge as parser and retention knowledge, not as full platform ownership. The most important active follow-ups are to verify diagnostic-file filtering, decide/fix `.csv.gz`, and decide whether this worker will own future Android v2.3.0 NDJSON/JSONL offline replay.
