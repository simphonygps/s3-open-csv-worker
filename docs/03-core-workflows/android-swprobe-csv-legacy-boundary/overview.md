# Android SWProbe CSV Legacy Boundary

Source status: split from already-migrated `Android application - swprobe` knowledge on 2026-05-13 without rereading Confluence.

## Current Boundary

Android contract `2.3.0` prefers NDJSON/JSONL for offline envelope replay. CSV remains a legacy fallback and S3 Stage-1 validation path.

This worker owns CSV file parsing, not NDJSON contract parsing unless an explicit future decision assigns that responsibility here.

## Current CSV Meaning

CSV files should be treated as legacy/offline compatibility input:

```text
Android CSV queue -> S3 Open upload -> CSV worker -> soft_data
```

## Open Question

Should future Android NDJSON/JSONL parsing live in this worker, a sibling worker, or the ingestion service?
