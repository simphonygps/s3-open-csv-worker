# Repository Ownership Rules

## Core Rule

Each active task has exactly one leading repository.

The leading repository owns the full task or feature description.

Secondary repositories own only their local role in the larger feature.

## Default Ownership

Use these defaults when evidence supports them:

- frontend UI feature -> frontend repo owns the active task
- backend workflow or service -> fastapi-app repo owns the active task
- mobile probe or Android app behavior -> mobile repo owns the active task
- telemetry ingestion -> ws-ingestor-open repo owns the active task
- S3 presign, upload policy, or upload authorization -> s3-service-api repo owns the active task
- MinIO object-created metadata ingestion -> ingestion-worker repo owns the active task
- S3 Open CSV/offline file parsing -> s3-open-csv-worker repo owns the active task
- ETL transformation -> etl owns the active task
- cross-cutting deployment or infrastructure change -> repository that contains the changed deployment source owns the active task
- documentation-only source-of-truth change -> repository whose documentation is being corrected owns the active task

## This Repository

This repository represents the Simphony S3 Open CSV/offline file parser worker unless project context states otherwise.

For this repository, the leading task area normally includes:

- S3 Open offline CSV file parsing
- supported offline file branch selection owned by this worker
- CSV header aliases and file-contract behavior
- row validation and row rejection behavior
- mapping CSV/offline-file fields into Simphony canonical telemetry
- `soft_data` insertion performed by this worker
- best-effort `telemetry_etl_records` metadata written by this worker
- `s3_processed_files` lifecycle, counters, partial-failure state, and retention surfaces
- parser-worker runtime, deployment, and verification behavior

This repository does not normally lead presign generation, upload authorization, upload metadata receive, frontend visibility, online HTTP telemetry ingestion, or Traccar compatibility execution unless a current architecture decision explicitly assigns that ownership here.

## Multi-Repository Features

If a feature affects multiple repositories:

- update source-of-truth docs in all affected repositories when evidence is supplied
- keep active task status in exactly one leading repository
- do not duplicate active task tracking across repositories
- document local responsibility in secondary repositories

The leading repository should contain:

- full feature description
- task status
- primary workflow
- main implementation summary
- cross-repository participation summary
- links or references to secondary repository docs when supplied

Secondary repositories should contain:

- local role
- local changed files and components
- local API, DTO, UI, data, storage, or integration behavior
- local limitations

## Confluence SMS Ownership

Confluence SMS does not replace repository source-of-truth documentation.

Confluence SMS owns project-level orientation and current-task tracking for:

- process functions and actions
- architecture and interconnections
- business processes
- data flow across repositories
- database and storage concepts
- product functionality overview
- user, administrator, and operator workflows
- container farm and deployment architecture
- strategic decisions
- open questions
- brief records in `07 active projects and current tasks`

When a task changes these areas, repository docs must describe implementation details, and Confluence SMS must receive a brief project-level update.

## Unknown Ownership

If repository ownership cannot be determined from supplied evidence, do not guess.

Return:

```plaintext
NOT_ENOUGH_EVIDENCE_TO_UPDATE_THIS_SECTION
```

and request human review for ownership.
