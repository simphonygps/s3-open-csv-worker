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

## Historical Boundary

NiFi, FTP, and ZIP upload are predecessor paths. This worker is the concrete Stage-1 CSV processing owner.

NDJSON/JSONL parser code exists here, but current implementation is an older/intermediate `T2.2` branch. Treat v2.3.0 NDJSON alignment as active design/work, not done.

## Traccar Boundary

Traccar projection is downstream compatibility. This worker may mark rows as pending for projection through ETL metadata, but it must not call Traccar directly or own sync retries.
