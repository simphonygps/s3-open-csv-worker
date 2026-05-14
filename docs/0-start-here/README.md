# s3-open-csv-worker Source Of Truth

This documentation tree is the local source of truth for `s3-open-csv-worker`.

## Repository Role

Offline CSV file parser for S3 Open uploads. It reads uploaded CSV files, validates rows, maps fields into Simphony canonical telemetry, and writes rows into `soft_data`.

## Start Here

- `daily-workflow.md`
- daily-progress-and-source-of-truth.md - daily/session/milestone progress reporting rule.
- `vps-access-precondition.md`
- `source-of-truth-rules.md`
- `git-branching-and-deployment.md`
- `active-task-start-rule.md`
- `../1-current-state/task-lifecycle.md`
- `../1-current-state/active-tasks.md`
- `../1-current-state/current-priorities.md`
- `../1-current-state/current-progress.md`
- `../1-current-state/completed-task-archive.md`
- `../2-project-functionality/purpose-and-boundaries/overview.md`

## Daily Working Model

Daily preconditions, access rules, branch discipline, source-of-truth rules, and AI/development policy stay in this folder. Current work, accepted CSV processing behavior, and reusable runtime/testing knowledge live in the other top-level folders named by `source-of-truth-rules.md`.

Before starting a new task, read `active-task-start-rule.md` and explicitly record whether this repository is the leading repository or a secondary repository for the task.

At the end of each work day or meaningful milestone, follow `daily-progress-and-source-of-truth.md` and update the current progress/activity records. Long features may produce many progress reports before completion.

