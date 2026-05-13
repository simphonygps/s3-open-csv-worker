# Purpose And Boundaries

`s3-open-csv-worker` parses legacy/offline CSV telemetry uploaded through S3 Open.

Responsibilities:

- detect/process uploaded CSV files.
- validate rows.
- map CSV fields to Simphony canonical telemetry.
- insert rows into `soft_data`.
- preserve raw/audit payload where schema requires it.
- track processed-file status and avoid duplicate row insertion.
- write projection-readiness metadata such as `telemetry_etl_records` when supported.

NDJSON/JSONL v2.3.0 processing is a future or separate extension unless implemented here explicitly.

Out of scope:

- direct Traccar API calls.
- Traccar credentials.
- Traccar sync job retry/admin operations.
- customer/account/device business workflows outside CSV row parsing.
