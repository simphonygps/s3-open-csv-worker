# Android SWProbe CSV Legacy Boundary

Source status: refreshed from current `s3-open-csv-worker` docs/code, already-read `SWProbes Open` knowledge, and older `Android application - swprobe` CSV/offline behavior on 2026-05-13.

## Current Boundary

Android contract `2.3.0` prefers NDJSON/JSONL for offline envelope replay. CSV remains a legacy fallback and S3 Stage-1 validation path.

This worker owns CSV file parsing. It also currently contains an NDJSON/JSONL parser path, but that parser is aligned to `T2.2` / `2.2`, not the current `T2.3.0` / `2.3.0` target.

## Current CSV Meaning

CSV files should be treated as legacy/offline compatibility input:

```text
Android CSV queue -> S3 Open upload -> CSV worker -> soft_data
```

The current/future Android offline direction is:

```text
Android NDJSON/JSONL queue
  -> S3 Open upload
  -> parser that accepts one v2.3.0 envelope per line
  -> soft_data / telemetry_etl_records
```

This repo may become that v2.3.0 parser owner, but code must be explicitly updated from `T2.2` to `T2.3.0` first.

Older architecture/design pages that describe CSV or Python ETL are predecessor history. They explain why this worker exists, but they do not override the current Android `2.3.0` direction toward NDJSON/JSONL offline replay.

## Android Queue Context

The older Android source says mobile should upload only closed files, attempt queued files oldest first, keep failed files queued, and delete local files only after upload success. Preserve that as client-side reliability context.

This worker should not infer Android queue correctness from parser order. Parser evidence is `s3_processed_files` status/counters plus inserted `soft_data` and `telemetry_etl_records`.

## Open Question

Should future Android NDJSON/JSONL parsing live in this worker, a sibling worker, or the ingestion service?
