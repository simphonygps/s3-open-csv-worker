# Android SWProbe CSV Legacy Boundary

Source status: split from already-migrated `Android application - swprobe` knowledge and refreshed from already-read `SWProbes Open` knowledge on 2026-05-13.

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

## Open Question

Should future Android NDJSON/JSONL parsing live in this worker, a sibling worker, or the ingestion service?
