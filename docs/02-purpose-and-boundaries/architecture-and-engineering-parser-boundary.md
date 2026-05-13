# Architecture And Engineering Parser Boundary

Source status: split from already-migrated Architecture/Engineering knowledge on 2026-05-13.

## Current Role

`s3-open-csv-worker` owns S3 Open offline telemetry file parsing and processed-file lifecycle.

Current flow:

```text
MinIO object -> detect CSV/NDJSON by key -> parse rows -> s3_processed_files -> soft_data -> telemetry_etl_records
```

## Boundary

Object arrival is not parser success. Parser evidence should include processed-file status, processed line counts, error counts, and resulting telemetry rows.

Presign success and upload metadata success are earlier milestones owned by other services. This worker's milestone is successful row parsing and canonical DB insertion.

The old `S3 Open service` Confluence pages describe the full Stage-1 chain. In this repo, keep only the parser responsibilities:

- webhook payload acceptance,
- object byte download,
- payload-shape routing,
- row validation and insertion,
- idempotency and lifecycle status,
- retention preview/history/delete bookkeeping.

Do not move S3 presign, MinIO policy, upload metadata, or customer API ownership into this repo.

## Historical Boundary

NiFi, FTP, and ZIP upload are predecessor paths. This worker is the concrete Stage-1 CSV processing owner.

NDJSON/JSONL parser code exists here, but current implementation is an older/intermediate `T2.2` branch. Treat v2.3.0 NDJSON alignment as active design/work, not done.

S3 Open Stage-1 also used `.ping` files as upload-path proof. Those files are diagnostics, not telemetry. Current parser routing should be verified so proof files do not become failed CSV imports.

## Traccar Boundary

Traccar projection is downstream compatibility. This worker may mark rows as pending for projection through ETL metadata, but it must not call Traccar directly or own sync retries.
