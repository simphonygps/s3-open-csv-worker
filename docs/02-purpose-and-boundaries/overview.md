# Purpose And Boundaries

`s3-open-csv-worker` parses offline telemetry files uploaded through S3 Open.

The original repository name is CSV-specific, but current code can route by file extension and parse both CSV and NDJSON/JSONL object bytes. The NDJSON path is implemented for the older/intermediate `T2.2` / `2.2` envelope shape and must not be mistaken for completed Android `T2.3.0` / `2.3.0` support.

Responsibilities:

- detect/process uploaded CSV files.
- detect/process `.ndjson`, `.jsonl`, `.ndjson.gz`, and `.jsonl.gz` files through the implemented NDJSON parser.
- validate rows.
- map CSV fields to Simphony canonical telemetry.
- map supported NDJSON envelope fields to Simphony canonical telemetry.
- insert rows into `soft_data`.
- preserve raw/audit payload where schema requires it.
- track processed-file status and avoid duplicate row insertion.
- write projection-readiness metadata such as `telemetry_etl_records` when supported.

Android NDJSON/JSONL v2.3.0 processing is not complete until this worker accepts `EN.TP=T2.3.0` and records `contract_version=2.3.0` / `protocol_version=T2.3.0`, or a future decision assigns that parser to another service.

Out of scope:

- S3 presign generation and rate-limit policy.
- MinIO object-arrival metadata ownership before this worker receives a parse event.
- direct Traccar API calls.
- Traccar credentials.
- Traccar sync job retry/admin operations.
- customer/account/device business workflows outside CSV row parsing.
- Android online HTTP Open `2.3.0` ingestion.

Current reading of `SWProbes Open`: this repo owns the offline file-to-canonical-telemetry parser slice. WS primary telemetry, MQTT, FTP, ZIP, NiFi, and Redis-stream-only persistence are predecessor/history for this repo unless explicitly reopened.
