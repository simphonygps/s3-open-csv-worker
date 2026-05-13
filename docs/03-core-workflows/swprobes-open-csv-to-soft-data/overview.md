# SWProbes Open CSV To Soft Data

Source status: redone from the already-read full `SWProbes Open` Confluence section on 2026-05-12/13, refreshed with the already-read `S3 Open service` section, enhanced with older `Android application - swprobe` offline-file behavior on 2026-05-13, and reconciled with current `s3-open-csv-worker` code.

## Worker Role

`s3-open-csv-worker` owns offline file parsing for S3 Open objects.

CSV remains the Stage-1 verified and legacy/import path. Current code also routes `.ndjson`, `.jsonl`, `.ndjson.gz`, and `.jsonl.gz` to an NDJSON parser, but that parser currently targets the older/intermediate `T2.2` / `2.2` branch.

Current flow:

```text
SWProbe/SXProbe
  -> S3 presign/PUT
  -> MinIO object
  -> object metadata worker
  -> s3-open-csv-worker /minio-webhook
  -> download object bytes
  -> CSV or NDJSON parser
  -> soft_data
  -> telemetry_etl_records
  -> downstream latest/history/projection services
```

## Processing Rules

- Parse CSV line by line.
- Parse supported NDJSON/JSONL line by line when the file extension selects that parser.
- Insert valid telemetry rows into `soft_data` with S3 source context.
- Insert ETL/projection-readiness rows into `telemetry_etl_records` as best-effort observability.
- Track idempotency and lifecycle in `s3_processed_files`.
- Keep raw objects for retention/audit/replay rather than deleting immediately after parsing.
- Treat diagnostic `.ping` or tiny proof files as upload evidence, not telemetry rows.

The S3 Open Stage-1 pages confirm that parser success is not the same as presign success or upload success. For this repo, success evidence is the combination of `s3_processed_files` lifecycle status/counters and inserted canonical rows.

## v2.3.0 Alignment Gap

Current Android online telemetry is HTTP Open `2.3.0` / `T2.3.0`. Future offline replay should use NDJSON/JSONL with one complete v2.3.0 envelope per line.

This repo already contains NDJSON parser code, but it validates `EN.TP == T2.2` and writes `contract_version=2.2`. Therefore it is current implementation for the older/intermediate offline branch, not proof that v2.3.0 offline replay is complete.

## Historical Note

Older migration plans used Python ETL as the target replacement for NiFi. This worker is the current concrete owner of the CSV-to-database slice.

Older SWProbes Open pages describe `WS + S3 + plain CSV + Python ETL` as the November/December 2025 predecessor architecture. For this repo, the still-current piece is S3 offline file parsing. WebSocket primary telemetry, MQTT, FTP, ZIP upload, NiFi, and Redis-stream-only persistence remain historical unless explicitly reopened.

The older Android source adds queue semantics: closed files only, oldest queued file first, retry/keep on failure, and delete local file after upload success. For this worker those rules are diagnostic context. Parser truth is row validation, `soft_data` insertion, ETL observability, and `s3_processed_files` lifecycle counters.

## Engineering Evidence Rule

Transport/upload success is not semantic parser success. Close offline parser work only when the same object key has:

- successful object-byte download,
- selected parser branch,
- validated row counters,
- inserted `soft_data` rows for accepted telemetry,
- best-effort `telemetry_etl_records` if projection-readiness is part of the task,
- `s3_processed_files` status with counters and size.

If downstream Map/History or Traccar projection is needed, continue validation in the owning repositories after this worker evidence is complete.
