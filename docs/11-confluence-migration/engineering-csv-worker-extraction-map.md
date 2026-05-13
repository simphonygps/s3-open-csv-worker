# Engineering CSV-Worker Extraction Map

Source: Confluence `Engineering`
Destination repository: `C:\Project\Docker compose\s3-open-csv-worker`
Migration date: 2026-05-12

## Migration Judgment

The Engineering section is directly useful for this parser worker because it distinguishes transport/upload success from semantic parser acceptance and canonical storage. It must not move presign, metadata-worker, FastAPI, frontend, Android online ingestion, or Traccar execution responsibilities into this repository.

For this repo, the durable rule is:

```text
object exists != object downloaded != file parsed != row accepted != soft_data inserted != downstream visibility
```

`s3-open-csv-worker` owns offline file parser success: object-byte download, payload-shape routing, row validation, canonical insertion, ETL/projection-readiness metadata, and `s3_processed_files` lifecycle evidence.

## Current Code Reality Checked

- `app/main.py` exposes `/health`, `/health/db`, `/health/s3`, `POST /minio-webhook`, and read-only retention endpoints.
- `app/main.py` detects `.ndjson`, `.jsonl`, `.ndjson.gz`, `.jsonl.gz`, and `.csv.gz`; unknown suffixes currently default to `csv_file`.
- `app/main.py` skips already successful objects, marks processing started, downloads object bytes, calls CSV or NDJSON parser, and writes `s3_processed_files` success/error lifecycle.
- `app/csv_processor.py` validates CSV rows using required `timestamp`, `deviceId`, `latitude`, and `longitude`.
- `app/csv_processor.py` maps known CSV columns to `soft_data`, preserves `raw_payload` and `raw_payload_text`, and counts failed rows.
- `app/ndjson_processor.py` supports gzip for `ndjson_gz_file`, but currently validates `EN.TP == T2.2` and writes `contract_version=2.2`.
- `app/db.py` ensures `s3_processed_files`, inserts `soft_data`, and best-effort inserts `telemetry_etl_records`.
- `app/retention_worker.py` owns read-only retention preview/history/dry-run endpoints and optional CLI delete mode.

## Current Knowledge To Keep

For this worker, Engineering verification means proving these layers separately:

1. Runtime/readiness: `/health`, `/health/db`, and `/health/s3` as appropriate.
2. Webhook route reachability: `/minio-webhook` receives the object notification.
3. Idempotency: `s3_processed_files` is checked and processing starts only when needed.
4. Object bytes: S3/MinIO download succeeds for the expected bucket/key.
5. Payload routing: key suffix selects CSV or NDJSON branch correctly.
6. Semantic parser acceptance: rows are validated, invalid rows are counted, and accepted rows are normalized.
7. Canonical storage: accepted rows insert into `soft_data`.
8. Projection-readiness: `telemetry_etl_records` is written best-effort when supported.
9. Lifecycle proof: `s3_processed_files` records `success` or `error` with counters and size.
10. Downstream proof: FastAPI latest/history, frontend visibility, and Traccar projection belong to downstream repos.

## Current Vs Predecessor

| Engineering knowledge | CSV-worker treatment |
| --- | --- |
| WS Open as primary Android telemetry transport | Predecessor/inapplicable to offline parser. |
| Redis Streams as only WS persistence route | Historical ingestion detail outside this repo. |
| `dev-etl-open-1` as mandatory DB writer | Historical/deployment-specific ETL name, not current local proof unless runtime reuses it. |
| MQTT/FTP target transport | Predecessor transport history. Current input is S3 object bytes. |
| NiFi final ETL owner | Historical architecture direction; this worker is the concrete parser owner for Stage-1 CSV. |
| JSON contract `v1.5` | Legacy telemetry contract outside current CSV parser rules. |
| S3 Stage-1 CSV processing | Current verified/legacy parser responsibility. |
| NDJSON/JSONL offline replay | Implemented for older/intermediate `T2.2` / `2.2`, not yet current Android `T2.3.0` / `2.3.0`. |
| Traccar sync/projection | Downstream compatibility path after `soft_data` and ETL metadata. |

## Runtime Evidence Checklist

When validating this repo, record:

- repository, branch, and environment label,
- route or CLI path used: webhook, health, retention preview/history/dry-run, or retention CLI,
- bucket/key and payload shape for safe test objects,
- idempotency state before processing,
- object byte download result and size,
- parser branch selected,
- row counters: `rows_total`, `rows_inserted`, `rows_failed`,
- `s3_processed_files` status, error code/message, timestamps, and size,
- sample accepted `soft_data` evidence for the same device/timestamp,
- `telemetry_etl_records` evidence when projection-readiness is in scope,
- explicit note when FastAPI, frontend, or Traccar proof is out of scope.

## Secret Handling

Do not store S3/MinIO access keys, database credentials, private VPS paths, auth headers, private keys, presigned URL signatures, webhook secrets, or Traccar credentials in repository Markdown or Confluence.

Prefer safe test object keys, row counts, hashes/previews, timestamps, and sanitized error messages.

## Open Risks

- Unknown object suffixes default to CSV processing; diagnostic `.ping` files may need explicit ignore/non-telemetry lifecycle handling.
- `.csv.gz` is detected by suffix, but CSV bytes are decoded directly as UTF-8; gzip CSV support needs verification or implementation.
- NDJSON gzip is implemented, but the NDJSON contract is still `T2.2` / `2.2`.
- Partial row failures can still end in object `success`; use counters to distinguish full vs partial parser success.
- `telemetry_etl_records` is best-effort; `soft_data` and `s3_processed_files` remain the primary parser proof.
- Retention HTTP endpoints are read-only; actual delete behavior belongs to the CLI path and must be treated carefully.

## Files Updated From This Pass

- `docs/01-current-state/active-tasks.md`
- `docs/02-purpose-and-boundaries/overview.md`
- `docs/02-purpose-and-boundaries/architecture-and-engineering-parser-boundary.md`
- `docs/03-core-workflows/csv-file-processing/overview.md`
- `docs/03-core-workflows/s3-open-stage1-csv-processing/overview.md`
- `docs/03-core-workflows/swprobes-open-csv-to-soft-data/overview.md`
- `docs/11-confluence-migration/engineering-csv-worker-extraction-map.md`
