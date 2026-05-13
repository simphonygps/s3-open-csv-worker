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

Current reading of `S3 Open service`: this repo owns the post-upload parser and retention stage only. The Stage-1 S3 Open pages describe the wider chain, but this worker starts at ObjectCreated webhook handling and ends with `soft_data`, `telemetry_etl_records`, and `s3_processed_files` lifecycle evidence.

Current reading of `Android application - swprobe`: Android local CSV and upload queue behavior is upstream context for this worker. Parser ownership starts after object bytes are delivered; Android queue order, local file closure, presign behavior, and local deletion are not parser guarantees.

Known current caveats from code comparison:

- unknown object suffixes currently fall through to CSV handling; diagnostic `.ping` files should be explicitly ignored or handled as non-telemetry.
- `.csv.gz` is detected by suffix, but gzip decompression is not implemented in the CSV parser path.
- NDJSON gzip is implemented, but the NDJSON envelope version is still `T2.2`, not current Android `T2.3.0`.
