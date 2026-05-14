# Active Task Start Rule

Before starting implementation work, decide whether this repository is the leading repository for the task.

This decision must be written explicitly before or at the start of the work. Do not leave repository ownership implicit.

## Where To Track The Task

Use:

```plaintext
docs/1-current-state/active-tasks.md
```

Every active task entry must include:

- task name
- status
- leading repository
- whether this repository is leading or secondary
- related repositories
- short purpose
- expected result
- current next step
- acceptance criteria

## Leading Repository Rule

If this repository is the leading repository, the active task entry must say:

```plaintext
Repository role: leading
Leading repository: s3-open-csv-worker
```

The leading repository owns:

- the full task description
- the main feature narrative
- the status of the active task
- the complete acceptance criteria
- the final completed-task summary
- references to secondary repositories when they participate

For this `s3-open-csv-worker` repository, leading ownership normally applies to:

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

## Secondary Repository Rule

If this repository is not the leading repository but participates in the task, the active task entry must say:

```plaintext
Repository role: secondary
Leading repository: <owner-or-repo-name>
```

A secondary repository must describe only its local responsibility:

- local changed files and folders
- local parsers, validators, mappers, database writes, lifecycle records, configuration, or tests
- local CSV/file-contract, storage, ETL metadata, or parser-runtime behavior
- local verification evidence
- local limitations or remaining work

Do not duplicate the full task narrative in secondary repositories.

## Required Active Task Entry Shape

Use this minimum shape when creating a new active task:

```markdown
## Priority N: <Task Name>

Status: `active`

Repository role: `leading | secondary`

Leading repository: `<repo-name>`

Related repositories:

- `<repo-name>`

### Purpose

Short description of the task and why it exists.

### Expected Result

Short description of what should be true when the task is finished.

### Current Focus

What is being worked now.

### Acceptance Criteria

- Specific behavior or verification requirement.

### Current Next Step

One concrete next action.
```

## If Ownership Is Unclear

If repository ownership is unclear, do not start implementation as if ownership is known.

Write the task as `active` with:

```plaintext
Repository role: unknown
Leading repository: NOT_CONFIRMED
```

Then make confirming ownership the current next step.

## Completion Rule

When the task is completed, the leading repository moves the task summary to:

```plaintext
docs/1-current-state/completed-task-archive.md
```

Secondary repositories keep only local implementation notes in the relevant stable documentation area.
