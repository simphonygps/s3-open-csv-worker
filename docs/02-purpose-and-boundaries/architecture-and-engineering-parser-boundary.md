# Architecture And Engineering Parser Boundary

Source status: split from already-migrated Architecture/Engineering knowledge on 2026-05-13.

## Current Role

`s3-open-csv-worker` owns CSV telemetry parsing and processed-file lifecycle.

Current flow:

```text
MinIO CSV object -> parse rows -> s3_processed_files -> soft_data -> telemetry_etl_records
```

## Boundary

Object arrival is not parser success. Parser evidence should include processed-file status, processed line counts, error counts, and resulting telemetry rows.

## Historical Boundary

NiFi, FTP, and ZIP upload are predecessor paths. This worker is the concrete Stage-1 CSV processing owner.

## Traccar Boundary

Traccar projection is downstream compatibility. This worker may mark rows as pending for projection through ETL metadata, but it must not call Traccar directly or own sync retries.
