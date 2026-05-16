# Documentation Agent Repository Rollout Plan

Status: `active-in-progress`

Current destination repository: `s3-open-csv-worker`

Local path: `C:\Project\Docker compose\s3-open-csv-worker`

GitHub repository: `simphonygps/s3-open-csv-worker`

Canonical baseline source repository: `GPSTracker_ws_s3_open`

Canonical baseline source path: `C:\Project\GPSTracker_ws_s3_open`

## Purpose

This plan tracks the local rollout of the ChatGPT API documentation agent into the s3-open-csv-worker repository.

The work is intentionally mechanical: align the shared agent files from the tested baseline, preserve this repository's CSV/offline-parser ownership rules, verify locally, run the GitHub workflow from `dev`, and then move the local activity out of active tasks after both workflow tests pass.

## Required Files

Shared files copied or aligned from the canonical baseline:

- `.ai/source_of_truth_policy.md`
- `.ai/source_of_truth_prompt.md`
- `.github/workflows/ai-source-of-truth.yml`
- `scripts/ai_update_source_of_truth.py`

Repository-specific file that must be preserved:

- `.ai/repo_ownership_rules.md`

Workflow machine-output file:

- `docs/ai-source-of-truth-runs/latest-doc-agent-result.json`

## Approved Source-Of-Truth Roots

Normal Markdown updates are allowed only under:

```text
docs/0-start-here/
docs/1-current-state/
docs/2-project-functionality/
docs/3-runtime-testing-and-operations/
```

The agent may create descriptive subfolders inside those roots. It must not create loose topical Markdown files directly under `docs/`.

`docs/ai-source-of-truth-runs/` is reserved for machine-readable workflow result JSON.

## Ownership Boundary

This repository owns S3 Open CSV/offline parser behavior:

- S3 Open offline CSV file parsing
- supported offline file branch selection owned by this worker
- CSV header aliases and file-contract behavior
- row validation and row rejection behavior
- mapping CSV/offline-file fields into Simphony canonical telemetry
- `soft_data` insertion performed by this worker
- best-effort `telemetry_etl_records` metadata written by this worker
- `s3_processed_files` lifecycle, counters, partial-failure state, and retention surfaces
- parser-worker runtime, deployment, and verification behavior

This repository does not own presign generation, upload authorization, upload metadata receive, frontend visibility, online HTTP telemetry ingestion, or Traccar compatibility execution unless a current architecture decision explicitly assigns that ownership here.

## Mechanical Steps

1. Inspect existing source-of-truth hierarchy and current git status.
2. Copy or align the four shared canonical files from `C:\Project\GPSTracker_ws_s3_open`.
3. Preserve `.ai/repo_ownership_rules.md`.
4. Ensure `docs/ai-source-of-truth-runs/latest-doc-agent-result.json` exists.
5. Update this repository's active task, progress, and activity log.
6. Run `python -m py_compile scripts\ai_update_source_of_truth.py`.
7. Compare shared file hashes with the canonical baseline.
8. Commit and push the setup to `docs/source-of-truth-hierarchy`, `dev`, and `main`.
9. Run the dry-run GitHub workflow test from `dev`.
10. Run the manual apply guard workflow test from `dev`.
11. If both tests pass, move this local activity to runtime/testing/operations and completed task archive.

## Test Parameters

Dry-run smoke test:

```text
Use workflow from: Branch dev
Optional base SHA for manual dry run: leave empty
Optional head SHA for manual dry run: leave empty
Apply docs updates and open a draft PR: false
Append Confluence SMS updates: false
```

Manual apply guard test:

```text
Use workflow from: Branch dev
Optional base SHA for manual dry run: leave empty
Optional head SHA for manual dry run: leave empty
Apply docs updates and open a draft PR: true
Append Confluence SMS updates: false
```

Expected guard result:

- workflow succeeds;
- no PR is opened;
- artifact JSON reports `review_required=true`;
- artifact JSON includes `MANUAL_APPLY_REQUIRES_EXPLICIT_BASE_AND_HEAD_SHA`.

## Current Status

Status: `structure-copied-local-prechecks-passed`

Completed so far:

- Shared baseline files were copied into this repository.
- CSV-worker-specific `.ai/repo_ownership_rules.md` was preserved.
- `docs/ai-source-of-truth-runs/latest-doc-agent-result.json` was added or confirmed.
- Local source-of-truth task tracking was opened.
- Python runner compile check passed.
- Shared file hashes match the canonical baseline for policy, prompt, workflow YAML, and Python runner.

Next step:

- Commit and push the setup for GitHub workflow testing.
