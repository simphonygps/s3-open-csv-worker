# Active Tasks

## Priority 1: Replicate ChatGPT API Documentation Agent Into s3-open-csv-worker Repository

Status: `active-in-progress`

Progress: `structure-copied-local-prechecks-passed`

Current destination repository: `s3-open-csv-worker`

Current local path: `C:\Project\Docker compose\s3-open-csv-worker`

Current GitHub repository: `simphonygps/s3-open-csv-worker`

Local leading repository for this rollout record: `s3-open-csv-worker`

Canonical baseline source repository: `GPSTracker_ws_s3_open`

Canonical baseline source path: `C:\Project\GPSTracker_ws_s3_open`

Detailed rollout plan: `docs/1-current-state/documentation-agent-repository-rollout-plan.md`

### Purpose

Replicate the tested ChatGPT API documentation-agent baseline into this S3 Open CSV/offline parser worker repository without losing the repository-specific CSV worker ownership rules.

This repository is the current destination for the cross-repository rollout. For this local activity, `s3-open-csv-worker` owns the local task description, local verification evidence, and local completion record. The canonical shared policy, prompt, workflow, and Python runner still come from `GPSTracker_ws_s3_open`.

### Required Local Result

- `.ai/source_of_truth_policy.md` matches the canonical baseline.
- `.ai/source_of_truth_prompt.md` matches the canonical baseline.
- `.github/workflows/ai-source-of-truth.yml` matches the canonical baseline.
- `scripts/ai_update_source_of_truth.py` matches the canonical baseline.
- `.ai/repo_ownership_rules.md` remains s3-open-csv-worker-specific and is not overwritten.
- `docs/ai-source-of-truth-runs/latest-doc-agent-result.json` exists for workflow machine output.
- The source-of-truth hierarchy remains limited to the approved roots:
  - `docs/0-start-here/`
  - `docs/1-current-state/`
  - `docs/2-project-functionality/`
  - `docs/3-runtime-testing-and-operations/`
  - `docs/ai-source-of-truth-runs/`

### Repository-Specific Ownership To Preserve

This repository owns S3 Open offline CSV file parsing, supported offline file branch selection owned by this worker, CSV header aliases and file-contract behavior, row validation and row rejection behavior, mapping CSV/offline-file fields into Simphony canonical telemetry, `soft_data` insertion performed by this worker, best-effort `telemetry_etl_records` metadata written by this worker, `s3_processed_files` lifecycle/counters/partial-failure state/retention surfaces, and parser-worker runtime/deployment/verification behavior.

It must not become the leading source for presign generation, upload authorization, upload metadata receive, frontend visibility, online HTTP telemetry ingestion, or Traccar compatibility execution unless a current architecture decision explicitly assigns that ownership here.

### Verification Plan

1. Compile the Python runner locally with `python -m py_compile scripts\ai_update_source_of_truth.py`.
2. Confirm shared file hashes match the canonical baseline.
3. Push the aligned files to `docs/source-of-truth-hierarchy`, `dev`, and `main`.
4. Run the GitHub Actions workflow from `dev` with:
   - `Apply docs updates and open a draft PR = false`
   - `Append Confluence SMS updates = false`
5. If the dry run succeeds, run the controlled apply guard test from `dev` with:
   - `Apply docs updates and open a draft PR = true`
   - `Append Confluence SMS updates = false`
   - empty optional base/head SHA fields
6. Confirm the guarded apply-mode run opens no PR and reports `MANUAL_APPLY_REQUIRES_EXPLICIT_BASE_AND_HEAD_SHA`.

### Current Next Step

Commit and push the aligned setup, then perform the two GitHub workflow tests from `dev`.

Initial next documentation task, in priority order:

- inspect code and document exact CSV header aliases, row validation rules, idempotency key, and DB columns.
- preserve Traccar projection boundary: this worker writes Simphony canonical rows and ETL/projection metadata, but does not call Traccar directly.

## 2026-05-13 Source-Of-Truth Update

Traccar integration knowledge has been absorbed for this worker repo. Current decision: `s3-open-csv-worker` owns CSV-to-`soft_data` parsing and projection-readiness metadata only. Dedicated backend sync/projection services own Traccar compatibility execution.

## 2026-05-13 SWProbes Open Redo

`SWProbes Open` knowledge has been reapplied specifically to this repo.

Current decision: preserve S3 Open offline file parsing as this worker's active responsibility. Treat CSV as Stage-1 verified/legacy support. Treat existing NDJSON parser code as implemented older/intermediate `T2.2` / `2.2` support, not yet current Android `T2.3.0` / `2.3.0` offline replay.

Predecessor/history for this repo: WS primary telemetry, MQTT, FTP, ZIP, NiFi, and Redis-stream-only persistence. Current online Android HTTP Open `2.3.0` belongs to the telemetry ingestor, not this worker.

Implementation follow-up: decide whether this repo owns the future v2.3.0 NDJSON/JSONL parser. If yes, update `app/ndjson_processor.py` from `T2.2` / `2.2` / `offline_ndjson_v22` to the final v2.3.0 contract and add focused tests.

## 2026-05-13 Android Application SWProbe Source Pass

`Android application - swprobe` knowledge has been applied to this repo as Android legacy CSV/offline-file parser context.

Current decision: preserve Android closed-file queueing, oldest-first retry, keep-on-failure, and delete-after-upload-success as upstream client reliability rules. This worker owns parser success after object delivery: `soft_data`, `telemetry_etl_records`, and `s3_processed_files` lifecycle. It must not claim Android v2.3.0 NDJSON replay until `app/ndjson_processor.py` is aligned from `T2.2` / `2.2` to `T2.3.0` / `2.3.0`.

## 2026-05-13 S3 Open Service Redo

Repository: `s3-open-csv-worker`

Local path: `C:\Project\Docker compose\s3-open-csv-worker`

`S3 Open service` knowledge has been reconciled with this repo's current source of truth and code.

Current decision: keep this repo focused on the S3 object parsing and retention slice. Historical S3 Open Stage-1 pages are relevant here only after an object has arrived in MinIO/S3 and a webhook points the worker to the object. Presign generation, device/tenant validation, upload metadata rows, and customer-facing APIs remain outside this repo.

Active follow-ups, in priority order:

- verify or harden `.ping` and other non-telemetry diagnostic files so they are ignored or marked as non-telemetry instead of falling through to CSV processing.
- verify/fix `.csv.gz` support before describing it as complete; current code detects the suffix but CSV processing does not decompress gzip bytes.
- decide whether v2.3.0 NDJSON/JSONL offline replay belongs here. Current NDJSON code is still the older/intermediate `T2.2` / `2.2` path.
- preserve `s3_processed_files` lifecycle counters as the operational proof of parser success, including partial row failures.
- Treat the Confluence `Engineering` section as parser/runtime verification source material for this worker. Preserve layered download/shape/row-validation/`soft_data`/`telemetry_etl_records`/`s3_processed_files` evidence, but keep WS `v1.5`, Redis-stream-only persistence, MQTT/FTP, NiFi, FastAPI reads, frontend visibility, and Traccar execution as predecessor or downstream context unless a current task explicitly reassigns ownership.
- Treat the Confluence `Architecture, design and high-level plans` section as very old predecessor architecture. For this repo, preserve the S3-compatible Open Service parser/ETL origin story, but keep current ownership limited to object download, CSV or implemented NDJSON branch selection, row validation, `soft_data` insertion, best-effort `telemetry_etl_records`, `s3_processed_files` lifecycle, and retention surfaces.
