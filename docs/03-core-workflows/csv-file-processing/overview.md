# CSV File Processing

High-level flow:

```text
S3 uploaded CSV -> worker reads file -> parse rows -> validate -> normalize -> soft_data
```

CSV is Stage-1 verified legacy/import support. New v2.3.0 offline telemetry should prefer NDJSON/JSONL where backend support exists.

Current CSV validation requires:

- parseable `timestamp`,
- non-empty `deviceId`,
- parseable `latitude`,
- parseable `longitude`.

Rows that fail these required fields are skipped and counted as failed. Valid rows preserve structured `raw_payload` and `raw_payload_text` for audit/debug parity with other flows.

For downstream Traccar compatibility, the CSV worker should preserve normalized identity, timestamp, GPS, and ETL metadata. It should not reshape CSV rows into Traccar API calls.

The worker also contains an NDJSON parser path selected by file extension. That path currently expects `EN.TP=T2.2`; do not describe it as completed v2.3.0 offline support until code and tests are aligned.
