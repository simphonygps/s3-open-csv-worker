# VPS Access Precondition

Before using VPS, Docker, deploy scripts, logs, containers, Redis, MinIO, or database access for this repository, read the canonical shared access instructions:

```text
C:\Project\GPSTracker_ws_s3_open\docs\03-development-culture\universal-access-instructions.md
```

## Helium Mobile IPv6 Access

Verified 2026-08-20 PT: when the workstation is on Helium Mobile and IPv4 VPS
access is unavailable, use IPv6 SSH:

```bash
ssh -6 -i ~/.ssh/devsvc_key -o IdentitiesOnly=yes \
  devsvc@2605:a141:2274:6452::1
```

The same IPv6 session also listed running Docker/Compose services, queried the
`motion_data` database through its container, and returned healthy FastAPI and
WS responses from inside their containers.

A private `myvps-dev-ipv6` SSH alias may use that host, user, identity file,
`IdentitiesOnly yes`, and `AddressFamily inet6`. Substitute it for
`myvps-dev` in existing VPS commands. Once authenticated, the same CSV worker
deploy/pull scripts, Docker/Compose, logs, MinIO/Redis/PostgreSQL container
checks, and SSH tunnels work through the shell.

The only direct VPS endpoint proved over Helium Mobile was IPv6 SSH on TCP port
22. Public application/database ports were not tested; continue to use SSH,
localhost, container, or approved tunnel paths.

Use VPS only for runtime verification, deploy/pull checks, logs, containers, database checks, and operational evidence. Source code changes must be made in the local repository and moved through Git.

Do not copy credentials, tokens, private keys, signed URLs, private paths, database passwords, auth headers, or live secret values into repository docs, code, logs, or chat.
