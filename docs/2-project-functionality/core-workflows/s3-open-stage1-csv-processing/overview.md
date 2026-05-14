# S3 Open Stage-1 CSV Processing

Source status: redone from the already-read full `S3 Open service` Confluence section on 2026-05-13, enhanced with older `Android application - swprobe` CSV/offline behavior, and reconciled with current `s3-open-csv-worker` code.

## Worker Role

`s3-open-csv-worker` owns telemetry parsing for S3 Open CSV objects.

The repo also contains an NDJSON/JSONL parser path selected by object key extension, but Stage-1 CSV remains the historically verified S3 Open path.

Current flow:

```text
MinIO CSV object -> s3-open-csv-worker -> s3_processed_files -> soft_data
```

Wider historical Stage-1 flow:

```text
device -> POST /s3/presign -> PUT object to MinIO
  -> ObjectCreated webhook -> upload metadata/worker chain
  -> s3-open-csv-worker -> soft_data
```

Only the final parser/lifecycle part belongs to this repo.

## Processing Expectations

- Parse files line by line.
- Convert valid rows into canonical telemetry storage.
- Insert best-effort `telemetry_etl_records` for projection-readiness/routing observability.
- Record idempotency and lifecycle in `s3_processed_files`.
- Track processing status, processed line counts, and per-line errors.
- Keep raw objects available for retention, audit, or replay until lifecycle policy removes them.

## Stage-1 Distinction

`.ping` or small diagnostic files are proof of upload path behavior. They should not be treated as telemetry rows.

Current code note: object suffix routing defaults unknown suffixes to `csv_file`. Verify that `.ping` and other upload-proof files are filtered before parse or recorded as non-telemetry lifecycle evidence.

## Current Code Caveats

- CSV rows require parseable `timestamp`, non-empty `deviceId`, parseable `latitude`, and parseable `longitude`.
- Partial row failures are counted, while the object can still be marked `success` if processing completes.
- `.csv.gz` is detected as a payload shape, but the CSV path does not currently decompress gzip bytes.
- `.ndjson.gz` and `.jsonl.gz` are decompressed in the NDJSON path.
- NDJSON/JSONL currently targets the older `T2.2` / `2.2` envelope, not Android `T2.3.0`.

## Historical Context

This worker is the concrete replacement for older FTP/NiFi/ZIP-oriented offline processing ideas in the Stage-1 S3 Open path.

Older SWProbes Open pages that mention WS/S3/plain CSV/Python ETL are predecessor architecture. Current online Android telemetry is HTTP Open `2.3.0` through the telemetry ingestor; this worker only handles offline files after S3 upload.

The very old architecture/design section describes the same S3-compatible Open Service at a higher level. For this worker, keep that material as parser/ETL origin history. It does not supersede the current boundary: presign and object metadata are upstream, parser/lifecycle is here, customer visibility and Traccar projection are downstream.

The older Android source confirms that mobile upload success and local file deletion happen before this parser's responsibility begins. Parser acceptance is not Android upload acceptance; it is object download, row parse, canonical insertion, and lifecycle status.

## Engineering Verification Meaning

For this repo, Stage-1 closure must be read at parser granularity:

- presign and PUT are upstream evidence,
- MinIO ObjectCreated and upload metadata are upstream evidence,
- object download starts this worker's proof,
- row validation and `soft_data` insertion are parser proof,
- `s3_processed_files` lifecycle counters are operational proof,
- FastAPI/latest-history/frontend/Traccar proof belongs downstream.

Use replay/local parsing to validate contract and mapping, but do not treat replay success as live runtime proof unless the live object path and lifecycle rows are also checked.
