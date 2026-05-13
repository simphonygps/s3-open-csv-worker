# Active Tasks

No active implementation task is declared by this bootstrap.

Initial next documentation task:

- inspect code and document exact CSV header aliases, row validation rules, idempotency key, and DB columns.
- preserve Traccar projection boundary: this worker writes Simphony canonical rows and ETL/projection metadata, but does not call Traccar directly.

## 2026-05-13 Source-Of-Truth Update

Traccar integration knowledge has been absorbed for this worker repo. Current decision: `s3-open-csv-worker` owns CSV-to-`soft_data` parsing and projection-readiness metadata only. Dedicated backend sync/projection services own Traccar compatibility execution.
