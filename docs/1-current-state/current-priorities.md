# Current Priorities

1. Preserve S3 Open offline file parsing as this worker's active responsibility.
2. Document exact CSV header aliases, row validation rules, idempotency key, and DB columns.
3. Verify or harden `.ping` and other non-telemetry diagnostic files so they are ignored or marked as non-telemetry instead of falling through to CSV processing.
4. Verify/fix `.csv.gz` support before describing it as complete.
5. Decide whether v2.3.0 NDJSON/JSONL offline replay belongs here.
6. If v2.3.0 NDJSON/JSONL belongs here, update `app/ndjson_processor.py` from `T2.2` / `2.2` / `offline_ndjson_v22` to final `T2.3.0` / `2.3.0` contract and add focused tests.
7. Preserve `s3_processed_files` lifecycle counters as operational proof of parser success, including partial row failures.
8. Preserve `soft_data`, `telemetry_etl_records`, and `s3_processed_files` ownership boundaries.
9. Preserve Traccar projection boundary: this worker writes Simphony canonical rows and ETL/projection metadata, but does not call Traccar directly.
10. Treat WS primary telemetry, MQTT, FTP, ZIP, NiFi, FastAPI reads, frontend visibility, and Traccar execution as predecessor/downstream context unless a current task explicitly reassigns ownership.

Task tracking source files:

- `active-tasks.md`: unfinished current work and planned backlog.
- `current-progress.md`: parser worker progress and verification status.
- `completed-task-archive.md`: completed or superseded tasks after acceptance.
- `task-lifecycle.md`: rules for moving tasks between states.
