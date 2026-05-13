# Git Branching And Deployment

## Branch Policy

- `main` is production-only.
- Daily work happens on `dev`.
- Feature work starts from `dev` in a functional branch.
- Merge functional branches back into `dev` only after the work is complete, reviewed, and documented.
- Do not merge into `main` until the production release process is explicitly started.

## Functional Branch Flow

```text
dev -> feature/<short-purpose> -> verify -> update docs -> merge to dev
```

## Documentation Automation Note

An automatic documentation process is planned for functional-branch-to-`dev` merges. Until it is active, developers and Codex must update source-of-truth docs manually in the same branch as the code change.
