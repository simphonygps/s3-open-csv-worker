# Traccar Projection Boundary

Source status: split from already-read `Traccar integration` knowledge on 2026-05-13.

## Rule

`s3-open-csv-worker` is a Simphony CSV-to-storage worker. It is not a Traccar sync worker.

Correct flow:

```text
S3 CSV object
  -> s3-open-csv-worker
  -> soft_data
  -> telemetry_etl_records with pending projection state
  -> downstream Traccar sync worker
```

Wrong flow:

```text
s3-open-csv-worker -> Traccar API
s3-open-csv-worker -> Traccar credentials
s3-open-csv-worker -> Traccar job retry/admin controls
```

## Current Code Alignment

Current `app/db.py` already follows this boundary by inserting valid rows into `soft_data` and adding `telemetry_etl_records` with `traccar_sync_status = 'pending'`.

That status means downstream compatibility work may be needed. It does not mean this worker owns the Traccar push.

## Backend Sync Context

The Confluence Traccar integration section describes backend sync actions such as:

- `create_group`
- `create_user`
- `create_device`
- `link_user_to_group`
- `assign_device_to_group`
- `grant_user_device_access`

Those are business/account/device sync actions, not CSV row parser actions. If a future telemetry-position projection action is added, it should still be drained by a dedicated sync/projection worker or backend service, not hidden inside CSV parsing.

## Failure Interpretation

Separate these states:

- S3 object download failed: S3/MinIO or object notification problem.
- CSV parse failed: file/header/row validation problem.
- `soft_data` insert failed: DB/schema/data issue.
- `telemetry_etl_records` insert failed: routing observability/projection-readiness issue.
- Traccar sync failed later: downstream compatibility issue.

Do not change CSV parser behavior just because a downstream Traccar sync job failed unless the failure proves the normalized Simphony record is wrong.

