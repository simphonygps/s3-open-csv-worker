# Source Of Truth Rules

- Read `0-start-here/` before coding. Keep daily workflow, VPS access preconditions, branch discipline, source-of-truth rules, and AI/development policy there.
- Keep active work, current priorities, progress tracking, task lifecycle, completed-task archive, migration evidence, and stable reference documents in `1-current-state/`.
- Keep stable contracts and reference documents in `1-current-state/reference-documents-and-contracts/`. Track compliance work as a current task, not as a separate top-level folder.
- Keep accepted purpose/boundary decisions, CSV processing workflows, CSV/file contracts, storage model, S3/MinIO integration, and code explanations in `2-project-functionality/`.
- Keep deployment/config notes, testing, and verification knowledge in `3-runtime-testing-and-operations/`.
- Avoid new top-level documentation folders unless the knowledge cannot fit one of the four repository-level buckets.
- Do not copy sample files containing private telemetry or secrets.
- Before starting a new task, follow `docs/0-start-here/active-task-start-rule.md`.
- Every active task must explicitly state `Repository role: leading | secondary | unknown`.
- Every active task must name the leading repository. If ownership is unclear, write `Leading repository: NOT_CONFIRMED` and make ownership confirmation the next step.

- Long-running tasks must produce daily or milestone reports in `docs/1-current-state/activity-log.md` before completion.
- Keep `docs/1-current-state/current-progress.md` as the short progress summary and `activity-log.md` as the detailed history.

