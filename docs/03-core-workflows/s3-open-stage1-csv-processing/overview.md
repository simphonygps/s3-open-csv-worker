# S3 Open Stage-1 CSV Processing

Source status: split from already-migrated S3 Open service knowledge on 2026-05-13 without rereading Confluence.

## Worker Role

`s3-open-csv-worker` owns telemetry parsing for S3 Open CSV objects.

Current flow:

```text
MinIO CSV object -> s3-open-csv-worker -> s3_processed_files -> soft_data
```

## Processing Expectations

- Parse files line by line.
- Convert valid rows into canonical telemetry storage.
- Record idempotency and lifecycle in `s3_processed_files`.
- Track processing status, processed line counts, and per-line errors.
- Keep raw objects available for retention, audit, or replay until lifecycle policy removes them.

## Stage-1 Distinction

`.ping` or small diagnostic files are proof of upload path behavior. They should not be treated as telemetry rows.

## Historical Context

This worker is the concrete replacement for older FTP/NiFi/ZIP-oriented offline processing ideas in the Stage-1 S3 Open path.
