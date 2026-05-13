# SWProbes Open CSV To Soft Data

Source status: split from already-migrated `SWProbes Open` knowledge on 2026-05-13.

## Worker Role

`s3-open-csv-worker` owns CSV telemetry parsing for S3 Open objects.

Current flow:

```text
MinIO object -> s3-open-csv-worker -> s3_processed_files -> soft_data
```

## Processing Rules

- Parse CSV line by line.
- Insert valid telemetry rows into `soft_data` with S3 source context.
- Track idempotency and lifecycle in `s3_processed_files`.
- Keep raw objects for retention/audit/replay rather than deleting immediately after parsing.
- Treat diagnostic `.ping` or tiny proof files as upload evidence, not telemetry rows.

## Historical Note

Older migration plans used Python ETL as the target replacement for NiFi. This worker is the current concrete owner of the CSV-to-database slice.
