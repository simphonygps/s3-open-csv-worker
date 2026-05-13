# Traccar Projection Fields

Source status: split from already-read `Traccar integration` knowledge on 2026-05-13.

## Purpose

This document lists the CSV-worker fields that matter for downstream Traccar projection while preserving the worker boundary.

The worker writes canonical Simphony telemetry first. Projection workers can use that normalized data later.

## Required Projection-Relevant Data

For a parsed CSV row to be useful downstream, preserve:

- stable device id,
- event timestamp,
- latitude,
- longitude,
- altitude when available,
- speed when available,
- heading/course when available,
- GPS accuracy when available,
- source context such as `s3-open`,
- enough payload metadata for deduplication and replay analysis.

## ETL Metadata

`telemetry_etl_records` should identify:

- `dedup_key`,
- `flow_id`,
- `transport`,
- `source`,
- `probe_type`,
- `protocol_version`,
- `contract_version`,
- `message_type`,
- `payload_shape`,
- `device_id`,
- routing status,
- processing status,
- `traccar_sync_status`.

Current code sets Traccar sync status to `pending` for inserted offline CSV rows. That is correct as projection readiness metadata, not direct sync execution.

## Do Not Invent Traccar Values

If optional telemetry fields are missing or malformed, the worker should validate according to the CSV/Simphony rules and record parse failures honestly. It should not invent values only to satisfy presumed Traccar requirements.

Downstream projection code owns defaulting, transform errors, and compatibility decisions.

