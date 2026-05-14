# Local Repository Locations

This note records the current local workspace layout for Codex and human developers. Read it before searching for sibling projects.

## Workspace roots

| Area | Local path | Notes |
| --- | --- | --- |
| Android software probe | `C:\Project\GPSTracker_ws_s3_open` | Current Android mobile application repository. |
| Docker Compose workspace | `C:\Project\Docker compose` | Main local workspace for backend, frontend, ingestion, ETL, and service repositories. |
| Firmware workspace | `C:\Project\Freematics\Freematics-master\firmware_v5` | Firmware source tree that contains the active telelogger repository. |

## Known repositories

| Repository | Local path | Purpose |
| --- | --- | --- |
| `GPSTracker_ws_s3_open` | `C:\Project\GPSTracker_ws_s3_open` | Android software probe application. |
| `frontend` | `C:\Project\Docker compose\frontend` | Simphony frontend application. |
| `fastapi-app` | `C:\Project\Docker compose\fastapi-app` | Main backend API and business services. |
| `ws-ingestor-open` | `C:\Project\Docker compose\ws-ingestor-open` | Telemetry ingestion service. |
| `s3-service-api` | `C:\Project\Docker compose\s3-service-api` | S3/MinIO presigned upload API service. |
| `ingestion-worker` | `C:\Project\Docker compose\ingestion-worker` | MinIO ingestion worker and object-event processor. |
| `s3-open-csv-worker` | `C:\Project\Docker compose\s3-open-csv-worker` | CSV/object processing worker. |
| `etl` | `C:\Project\Docker compose\etl` | ETL and telemetry transformation repository. |
| `traccar-web-reference` | `C:\Project\Docker compose\traccar-web-reference` | Traccar web reference repository. |
| `telelogger_wss` | `C:\Project\Freematics\Freematics-master\firmware_v5\telelogger_wss` | Firmware telemetry logger repository. |

## Working rule

If any repository is moved, update this file in every source-of-truth repository during the same change. Until the automatic documentation process is active, this update is manual.
