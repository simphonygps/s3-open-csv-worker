# Daily And Milestone Progress Reports

Long features may take many days. Source-of-truth docs must be ready to absorb progress reports before the feature is complete.

Update progress at the end of each work day, and also after any meaningful milestone inside the same day.

A milestone can be:
- a design decision
- a code implementation step
- a verification result
- a blocker discovery
- a cross-repository dependency clarification
- a Confluence/source-of-truth migration step
- a partial acceptance criterion becoming true
- a failed test or failed runtime check that changes the next step

Do not wait until the whole feature is finished.

## Files To Update
For daily or milestone progress, update:
- `docs/1-current-state/current-progress.md`
- `docs/1-current-state/activity-log.md`
- `docs/1-current-state/active-tasks.md` when status or next step changes
- Any specific contract/integration/runtime/functionality doc affected by the work

## Required Report Content
Each report must describe:
- what activity or feature was worked on
- what changed
- which repositories changed or were inspected
- what steps were completed
- what commands were run, if important
- what was verified
- what evidence was produced
- what remains blocked
- what is the next step
- whether the progress percentage changed
- which source-of-truth docs were updated

## Progress Percent Rule
Progress should measure working behavior, not number of files changed.

If code changed but no end-to-end or acceptance-relevant verification was done, progress may stay unchanged.

## Activity Log Rule
Use `docs/1-current-state/activity-log.md` for detailed daily/session/milestone history.

Append a new entry. Do not rewrite older entries unless correcting a factual error.

The activity log is allowed to contain several entries for the same day when meaningful milestones happen.

## Current Progress Rule
Use `docs/1-current-state/current-progress.md` for the short current-state summary.

Do not use `current-progress.md` as the detailed log. It is a summary.

## Active Task Rule
Use `docs/1-current-state/active-tasks.md` to keep the current task status and next step accurate.

For a long task, keep the task active and append progress evidence in `activity-log.md`.

Move the task to `completed-task-archive.md` only when it reaches its acceptance criteria.

## Stable Documentation Rule
When a milestone produces accepted behavior, move the stable knowledge into the relevant product, contract, integration, runtime, or testing documentation.

The activity log records what happened. Stable docs describe the current system.

