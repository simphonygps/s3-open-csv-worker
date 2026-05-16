# Current Progress

Use this file to track behavior-level parser-worker progress, verification evidence, blockers, and percentage/status changes.

Progress measures accepted behavior and verified evidence, not number of files changed.

## Current Status

- ChatGPT API documentation-agent rollout is active for this repository.
- Rollout status: `structure-copied-local-prechecks-passed`.
- This repository is the current destination repository for the rollout.
- `s3-open-csv-worker` owns the local rollout task record, local verification evidence, and local completion decision.
- Canonical shared files come from `C:\Project\GPSTracker_ws_s3_open`.
- Documentation and parser hardening follow-ups remain in `active-tasks.md`.
- Current priorities are tracked in `current-priorities.md`.
- Completed or superseded task summaries move to `completed-task-archive.md`.

## Current Verification Focus

- CSV header aliases and row validation.
- Idempotency key behavior.
- DB writes to `soft_data`.
- Best-effort `telemetry_etl_records` behavior.
- `s3_processed_files` lifecycle and counters.
- Non-telemetry diagnostic file handling.
- `.csv.gz` behavior.
- Decision and implementation path for v2.3.0 NDJSON/JSONL offline replay.

## Progress Log

### 2026-05-15 ChatGPT API Documentation Agent Rollout Started

Progress before: `pending-for-s3-open-csv-worker-repository`

Progress after: `structure-copied-local-prechecks-passed`

What changed:

- Shared documentation-agent files were copied or aligned from `C:\Project\GPSTracker_ws_s3_open`.
- CSV-worker-specific `.ai/repo_ownership_rules.md` was preserved.
- `docs/ai-source-of-truth-runs/latest-doc-agent-result.json` was added or confirmed for workflow machine output.
- Active task and local rollout plan were opened.
- Python runner compile check passed.
- Shared file hashes match the canonical baseline for policy, prompt, workflow YAML, and Python runner.

Next verification:

- Push the setup to GitHub and run the two manual workflow tests from `dev`.
