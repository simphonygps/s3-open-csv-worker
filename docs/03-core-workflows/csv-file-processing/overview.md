# CSV File Processing

High-level flow:

```text
S3 uploaded CSV -> worker reads file -> parse rows -> validate -> normalize -> soft_data
```

CSV is Stage-1 verified legacy/import support. New v2.3.0 offline telemetry should prefer NDJSON/JSONL where backend support exists.

From the historical `S3 Open service` Stage-1 section, CSV means the first verified offline object format after presigned upload and MinIO ObjectCreated notification. Treat that as predecessor/current legacy support, not the preferred future Android offline contract.

Current CSV validation requires:

- parseable `timestamp`,
- non-empty `deviceId`,
- parseable `latitude`,
- parseable `longitude`.

Rows that fail these required fields are skipped and counted as failed. Valid rows preserve structured `raw_payload` and `raw_payload_text` for audit/debug parity with other flows.

Current code maps only known columns from `CSV_TO_DB` and sets `source='s3-open'`. Additional CSV fields remain available only inside raw payload unless code explicitly maps them.

For downstream Traccar compatibility, the CSV worker should preserve normalized identity, timestamp, GPS, and ETL metadata. It should not reshape CSV rows into Traccar API calls.

The worker also contains an NDJSON parser path selected by file extension. That path currently expects `EN.TP=T2.2`; do not describe it as completed v2.3.0 offline support until code and tests are aligned.

Operational caveat: `.csv.gz` is detected by key suffix in the webhook flow, but the CSV parser currently decodes bytes directly as UTF-8. Gzip CSV support needs verification or implementation before being treated as active.

The older Android source explains why CSV objects may arrive late or in batches: mobile keeps failed files queued and retries. Treat parser `received`/`processed` time as worker timing, not necessarily telemetry capture order. Use row timestamps and object key evidence when reconstructing history.

## Engineering Runtime Evidence

Do not close CSV parser work with object upload or metadata evidence alone. Prove the parser layer:

- object bytes downloaded,
- CSV decoded and header parsed,
- required row fields validated,
- invalid rows counted,
- valid rows inserted into `soft_data`,
- `telemetry_etl_records` written when projection-readiness is in scope,
- `s3_processed_files` status and counters match the parser summary.

Partial row failures are not silent success. Preserve `rows_total`, `rows_inserted`, and `rows_failed` in validation notes.
