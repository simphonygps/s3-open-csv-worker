# DEV parser SSS boundary

## Live checkpoint — 2026-09-29 08:31 UTC

This supersedes candidate-only wording below. Image `eaf047dd...` is live after
operation `9589ce74-7aa6-49d0-aabe-25737374f38f` at 08:28:33; repeat 08:29:00
passed unchanged. Native DB/S3 positive and wrong-password/key denial, existing
lifecycle schema, parser fixtures without writes, container/host health passed.
Original stopped container retained, three existing versions reused, no issuer
rotation or stored data copied/deleted. Collector `5741c213...` admitted exact
references before cutover. Coordinator generation 23 / 45 projections; final
fresh Full/Partial reconstruction remains a bundle acceptance gate.

First native fixture failed because the test newline was double escaped;
corrected fixture passed against the same image. Both attempts are retained.

2026-09-29, active development. Coordinator: `simphonygps/ios`, AP-00-SSS-V1.

Candidate only until a dated coordinator adoption receipt supersedes this text.
Exact original nine-file source matched `origin/dev` 2f920176. The clean image
includes eight runtime originals plus the protected reader; excludes the legacy
manual `app/test_retention.py` script. No database or object data enters the image.

`PG_PASSWORD_FILE`, `S3_ACCESS_KEY_FILE`, `S3_SECRET_KEY_FILE` select fixed
per-consumer root-owned 0400 projections. Literal/secondary DSN credentials are
rejected. Missing, empty, unsafe or ambiguous files fail closed. Settings repr,
health errors and parser/retention error records do not expose credential-bearing
exception strings. Object payload previews are no longer logged.

Public endpoint/database/user/bucket/retention settings remain separate. Startup
caches settings: rotation requires controlled recreation, not just file replacement.
Database role remains the existing application role; no password rotation yet.
Same parsers, lifecycle/processed table, retention parameters and object namespace.
No retention execution, schema redesign or data migration is part of this change.

48 local tests plus six subtests passed with exact observed dependencies. These
prove readers, parser shape/counters, idempotency skip and protected error handling,
not live restoration or upstream MinIO delivery. T2.2 remains T2.2; `.csv.gz`,
`.ping` routing and T2.3.0 are separate existing follow-ups, not fixed by SSS.

Deployment gate: native DB/S3 positive and negative auth, schema pre-existence,
preserved feature files, backup reference admission, recoverable exchange and
repeat. Whole bundle requires final refreshed Full/Partial exact reconstruction.
