# CSV File Processing

High-level flow:

```text
S3 uploaded CSV -> worker reads file -> parse rows -> validate -> normalize -> soft_data
```

CSV is legacy/import support. New v2.3.0 offline telemetry should prefer NDJSON/JSONL where backend support exists.

For downstream Traccar compatibility, the CSV worker should preserve normalized identity, timestamp, GPS, and ETL metadata. It should not reshape CSV rows into Traccar API calls.
