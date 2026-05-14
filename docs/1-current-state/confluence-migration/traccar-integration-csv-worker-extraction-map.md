# Traccar Integration CSV Worker Extraction Map

Last migrated into this repo: 2026-05-13

This page records how the Confluence `Traccar integration` section applies to `s3-open-csv-worker`. The section was already read during cross-repository migration; this repo receives the worker-specific interpretation.

## Current Worker Position

- `s3-open-csv-worker` parses S3 Open CSV telemetry and writes canonical Simphony rows into `soft_data`.
- It also writes best-effort routing/projection observability into `telemetry_etl_records`.
- Traccar is downstream projection/compatibility, not this worker's direct API target.
- This worker should not call Traccar APIs, hold Traccar credentials, or retry Traccar sync jobs.
- Its Traccar-related responsibility is to preserve enough normalized telemetry and ETL metadata for backend sync workers to project eligible rows later.

## Source Pages And Actuality

| Confluence page | Date signal | CSV-worker actuality | How to use in this repo |
| --- | --- | --- | --- |
| `0 Phase - Target architecture - FINAL CLEAN` | 2026-03-22 | historical/foundation | Preserve Simphony/Traccar separation. Do not make CSV parsing depend on Traccar being available. |
| `0 Phase - Simphony Platform - Target Architecture` | 2026-03-22 | historical/foundation | Platform context only. |
| `Access & Domain Model - Phase 1` | 2026-04-07 | current backend domain foundation | Useful context for downstream ownership/access, not CSV parsing logic. |
| `DeviceEligibilityCheck - Service Specification` | 2026-04-18 | current onboarding contract | Not part of CSV worker scope. |
| `Phase 1 - implemented services` | 2026-04-26 | mixed/current snapshot | Use only as point-in-time backend capability context. |
| `Phase 1 - Traccar integration v.1` | 2026-04-26 | precondition/history with sensitive values | Extract mapping/sync concepts only. Do not copy credentials. |
| `Phase 1 - Traccar integration plan` | 2026-04-26 | current backend outbox/sync design | Confirms this worker should feed canonical/ETL records, not call Traccar. |
| `Phase 1 - Traccar processor description part 1` | 2026-04-26 | current backend implementation snapshot | Use for sync status vocabulary and supported action context. |
| `Phase 1 - admin web page` | 2026-04-27 | frontend/operator implementation | No worker UI scope. |
| `Phase 1 - admin operator console hierarchy` | 2026-04-27 | frontend/operator navigation | No worker scope. |
| `Phase 1 - daily 20 report` | 2026-04-27 | current-ish cross-repo report | Confirms Traccar Sync Monitor exists outside this worker. |
| `Unified Feature And Migration Plan` | 2026-05-10/11 | newer strategic direction | Treat Traccar sync as optional compatibility; Symphony-primary storage remains canonical. |

## Worker Documents Created From This Extraction

- `docs/2-project-functionality/core-workflows/traccar-projection-boundary/overview.md`
- `docs/2-project-functionality/csv-and-file-contracts/traccar-projection-fields/overview.md`

## Secret Handling

Confluence Traccar pages contain live-looking credentials and runtime access details. This repo must not store those values in docs, env examples, tests, logs, or source files.

