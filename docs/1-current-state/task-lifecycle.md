# Task Lifecycle

This repository keeps current work, priorities, progress, and completed-task archive in `docs/1-current-state/`.

## States

- `planned`: confirmed work that is not started yet.
- `active`: current task being worked now.
- `blocked`: current task that cannot continue until a dependency is resolved.
- `done`: task reached acceptance criteria.
- `archived`: completed or superseded task moved out of active tracking.

## Rules

1. Keep unfinished current work in `active-tasks.md`.
2. Keep priority order visible in `current-priorities.md` and in active task sections where needed.
3. Track behavior-level progress and verification evidence in `current-progress.md`.
4. When a task reaches 100% acceptance, move its summary to `completed-task-archive.md`.
5. Move accepted parser-worker behavior into the proper stable source-of-truth docs:
   - CSV processing behavior in `docs/2-project-functionality/core-workflows/`
   - CSV/file contracts in `docs/2-project-functionality/csv-and-file-contracts/`
   - storage effects in `docs/2-project-functionality/storage-model/`
   - S3/MinIO integration in `docs/2-project-functionality/s3-and-minio-integration/`
   - deployment/config notes in `docs/3-runtime-testing-and-operations/deployment-and-config/`
6. If no active task exists, write `No active task` explicitly in `active-tasks.md`.
7. Start a new task only by explicitly adding it to `active-tasks.md`.
8. Update progress when meaningful behavior, verification evidence, or blocker state changes.
9. Every active task must explicitly state repository role: `leading`, `secondary`, or `unknown`.
10. Every active task must name the leading repository. If the leading repository is not confirmed, write `Leading repository: NOT_CONFIRMED` and make ownership confirmation the next step.
11. The leading repository owns the full task description, status, acceptance criteria, and completed-task archive entry. Secondary repositories document only their local responsibility.

## Progress Meaning

Progress measures completed behavior and verified acceptance criteria.

It does not measure number of edited files.

## Daily And Milestone Reports
Long-running tasks must produce daily or milestone reports in `activity-log.md` before completion.

`current-progress.md` must stay as the short progress summary; `activity-log.md` is the detailed history.

