#!/usr/bin/env python3
"""Delta-based source-of-truth documentation agent runner.

Phase 1 behavior is the default: gather compact merge evidence, call OpenAI,
and write a structured result JSON for human review.

Phase 2 behavior is opt-in with DOCS_AGENT_APPLY=true: apply safe Markdown
documentation updates returned by the model, create a documentation branch,
commit the changes, push the branch, and open a GitHub pull request.

Phase 4 behavior extends apply mode across repositories: target files are
grouped by repository, applied in mapped or cloned worktrees, and published as
separate documentation pull requests.

Phase 5 behavior is opt-in with DOCS_CONFLUENCE_APPLY=true: append concise
project-level updates into Confluence SMS pages, including the mandatory
`07 - Active Projects And Current Tasks` tracking page.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from openai import OpenAI


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "docs" / "ai-source-of-truth-runs"
OUTPUT_FILE = OUTPUT_DIR / "latest-doc-agent-result.json"
RUN_LOG_FILE = ROOT / "docs" / "1-current-state" / "chatgpt-api-doc-agent-log.md"
EXTERNAL_REPOS_DIR = ROOT / ".docs-agent-repos"
MAX_CHANGED_FILES = int(os.environ.get("DOCS_MAX_CHANGED_FILES", "100"))
MAX_DIFF_CHARS = int(os.environ.get("DOCS_MAX_DIFF_CHARS", "80000"))
APPLY_CHANGES = os.environ.get("DOCS_AGENT_APPLY", "").lower() == "true"
APPLY_CONFLUENCE = os.environ.get("DOCS_CONFLUENCE_APPLY", "").lower() == "true"
DOCS_BRANCH_PREFIX = os.environ.get("DOCS_BRANCH_PREFIX", "docs/ai-source-of-truth")
CONFLUENCE_SPACE_KEY = os.environ.get("CONFLUENCE_SPACE_KEY", "SMS")
CONFLUENCE_ACTIVE_TASKS_TITLE = os.environ.get(
    "CONFLUENCE_ACTIVE_TASKS_TITLE",
    "07 - Active Projects And Current Tasks",
)


def run_git(args: list[str], *, check: bool = True, cwd: Path = ROOT) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.stdout.strip()


def read_text(path: str) -> str:
    file_path = ROOT / path
    if not file_path.exists():
        return ""
    return file_path.read_text(encoding="utf-8")


def load_event() -> dict[str, Any]:
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        return {}
    path = Path(event_path)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def get_diff_range(event: dict[str, Any]) -> tuple[str, str]:
    manual_base = os.environ.get("DOCS_BASE_SHA")
    manual_head = os.environ.get("DOCS_HEAD_SHA")
    if manual_base and manual_head:
        return manual_base, manual_head

    pull_request = event.get("pull_request") or {}
    base_sha = pull_request.get("base", {}).get("sha")
    head_sha = pull_request.get("head", {}).get("sha")
    if base_sha and head_sha:
        return base_sha, head_sha

    head = os.environ.get("GITHUB_SHA") or "HEAD"
    base = f"{head}~1"
    return base, head


def collect_existing_docs() -> dict[str, str]:
    docs: dict[str, str] = {}
    docs_root = ROOT / "docs"
    if not docs_root.exists():
        return docs

    priority_names = {
        "active-tasks.md",
        "current-priorities.md",
        "current-progress.md",
        "task-lifecycle.md",
        "completed-task-archive.md",
        "README.md",
    }
    paths = sorted(
        docs_root.rglob("*.md"),
        key=lambda path: (path.name not in priority_names, path.as_posix()),
    )
    for path in paths:
        if len(docs) >= 30:
            break
        relative = path.relative_to(ROOT).as_posix()
        docs[relative] = path.read_text(encoding="utf-8", errors="replace")[:12000]
    return docs


def build_input() -> dict[str, Any]:
    event = load_event()
    base_sha, head_sha = get_diff_range(event)
    pull_request = event.get("pull_request") or {}

    changed_files = run_git(["diff", "--name-only", base_sha, head_sha]).splitlines()
    diff_stat = run_git(["diff", "--stat", base_sha, head_sha])
    if len(changed_files) > MAX_CHANGED_FILES:
        diff = ""
        diff_limited = True
    else:
        diff = run_git(["diff", base_sha, head_sha, "--", *changed_files]) if changed_files else ""
        diff_limited = len(diff) > MAX_DIFF_CHARS

    return {
        "repository": os.environ.get("GITHUB_REPOSITORY", ""),
        "base_sha": base_sha,
        "head_sha": head_sha,
        "merge_commit_sha": pull_request.get("merge_commit_sha") or os.environ.get("GITHUB_SHA", ""),
        "pr": {
            "number": pull_request.get("number"),
            "title": pull_request.get("title"),
            "body": pull_request.get("body"),
            "merged": pull_request.get("merged"),
            "merged_branch": pull_request.get("head", {}).get("ref"),
            "base_branch": pull_request.get("base", {}).get("ref"),
            "head_branch": pull_request.get("head", {}).get("ref"),
            "html_url": pull_request.get("html_url"),
        },
        "commit_messages": run_git(["log", "--format=%s%n%b", f"{base_sha}..{head_sha}"]),
        "changed_files": changed_files,
        "limits": {
            "max_changed_files": MAX_CHANGED_FILES,
            "max_diff_chars": MAX_DIFF_CHARS,
            "diff_limited": diff_limited,
        },
        "diff_stat": diff_stat,
        "diff": diff[:MAX_DIFF_CHARS],
        "existing_docs": collect_existing_docs(),
        "policy": read_text(".ai/source_of_truth_policy.md"),
        "prompt": read_text(".ai/source_of_truth_prompt.md"),
        "repo_ownership_rules": read_text(".ai/repo_ownership_rules.md"),
    }


def call_openai(payload: dict[str, Any]) -> dict[str, Any]:
    client = OpenAI()
    response = client.responses.create(
        model=os.environ.get("OPENAI_DOCS_MODEL", "gpt-4.1-mini"),
        input=[
            {
                "role": "system",
                "content": payload["policy"],
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "instructions": payload["prompt"],
                        "response_rule": "Return only valid JSON. Do not wrap the JSON in Markdown.",
                        "repo_ownership_rules": payload["repo_ownership_rules"],
                        "evidence": {
                            key: value
                            for key, value in payload.items()
                            if key not in {"policy", "prompt", "repo_ownership_rules"}
                        },
                    },
                    ensure_ascii=True,
                ),
            },
        ],
    )

    text = response.output_text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {
            "updates_required": False,
            "review_required": True,
            "summary": "Model did not return valid JSON.",
            "raw_output": text,
            "limitations": ["NOT_ENOUGH_EVIDENCE_TO_UPDATE_THIS_SECTION"],
        }


def write_result(result: dict[str, Any]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(result, indent=2, ensure_ascii=True), encoding="utf-8")
    print(f"Wrote documentation agent result to {OUTPUT_FILE.relative_to(ROOT)}")


def ensure_safe_doc_path(path_value: str, repo_root: Path = ROOT) -> Path:
    if not path_value:
        raise ValueError("Target file path is empty.")
    path = Path(path_value.replace("\\", "/"))
    if path.is_absolute():
        raise ValueError(f"Absolute paths are not allowed: {path_value}")
    if ".." in path.parts:
        raise ValueError(f"Parent traversal is not allowed: {path_value}")
    if path.parts[0] != "docs":
        raise ValueError(f"Only docs/ Markdown files may be updated: {path_value}")
    if path.suffix.lower() != ".md":
        raise ValueError(f"Only Markdown files may be updated: {path_value}")

    resolved = (repo_root / path).resolve()
    docs_root = (repo_root / "docs").resolve()
    if docs_root not in resolved.parents:
        raise ValueError(f"Resolved path escapes docs/: {path_value}")
    return resolved


def current_repo_matches(target_repository: str | None, current_repository: str) -> bool:
    if not target_repository:
        return True
    if not current_repository:
        return True
    target_name = target_repository.split("/")[-1].lower()
    current_name = current_repository.split("/")[-1].lower()
    return target_repository.lower() == current_repository.lower() or target_name == current_name


def canonical_repo_name(repository: str | None, current_repository: str) -> str:
    if repository:
        return repository
    if current_repository:
        return current_repository
    return Path(ROOT).name


def repo_short_name(repository: str) -> str:
    return repository.replace("\\", "/").rstrip("/").split("/")[-1]


def grouped_target_files(result: dict[str, Any], current_repository: str) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for item in result.get("target_files", []):
        if not isinstance(item, dict):
            continue
        repo = canonical_repo_name(item.get("repository") or result.get("target_repository"), current_repository)
        groups.setdefault(repo, []).append(item)
    return groups


def local_repo_path_map() -> dict[str, Path]:
    mapping: dict[str, Path] = {}
    raw = os.environ.get("DOCS_REPO_PATHS_JSON", "")
    if raw:
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("DOCS_REPO_PATHS_JSON must be a JSON object.")
        for key, value in data.items():
            path = Path(str(value)).expanduser()
            mapping[str(key)] = path
            mapping[repo_short_name(str(key))] = path

    current_repository = os.environ.get("GITHUB_REPOSITORY", "")
    if current_repository:
        mapping[current_repository] = ROOT
        mapping[repo_short_name(current_repository)] = ROOT
    mapping[ROOT.name] = ROOT
    return mapping


def full_github_repo_name(repository: str, current_repository: str) -> str:
    if "/" in repository:
        return repository
    if "/" in current_repository:
        owner = current_repository.split("/", 1)[0]
        return f"{owner}/{repository}"
    return repository


def clone_repository(repository: str, current_repository: str) -> Path:
    full_name = full_github_repo_name(repository, current_repository)
    if "/" not in full_name:
        raise RuntimeError(
            f"Cannot clone repository '{repository}'. Provide owner/repo or DOCS_REPO_PATHS_JSON."
        )
    token = os.environ.get("GH_DOCS_BOT_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GH_DOCS_BOT_TOKEN or GITHUB_TOKEN is required for cross-repository clone.")

    target_dir = EXTERNAL_REPOS_DIR / full_name.replace("/", "__")
    if target_dir.exists():
        return target_dir

    target_dir.parent.mkdir(parents=True, exist_ok=True)
    remote = f"https://x-access-token:{token}@github.com/{full_name}.git"
    subprocess.run(
        ["git", "clone", "--depth", "1", remote, str(target_dir)],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return target_dir


def resolve_repo_root(repository: str, current_repository: str) -> Path:
    mapping = local_repo_path_map()
    root = mapping.get(repository) or mapping.get(repo_short_name(repository))
    if root:
        resolved = root.resolve()
        if not (resolved / ".git").exists():
            raise RuntimeError(f"Mapped repository path is not a Git repository: {resolved}")
        return resolved
    return clone_repository(repository, current_repository)


def branch_name(result: dict[str, Any], payload: dict[str, Any]) -> str:
    pr_number = payload.get("pr", {}).get("number")
    if pr_number:
        return f"{DOCS_BRANCH_PREFIX}-pr-{pr_number}"
    short_sha = str(payload.get("head_sha") or "manual")[:12].replace("/", "-")
    task_name = result.get("task", {}).get("name") or result.get("summary") or "manual"
    slug = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(task_name)).strip("-")
    slug = "-".join(part for part in slug.split("-") if part)[:40] or "manual"
    return f"{DOCS_BRANCH_PREFIX}-{slug}-{short_sha}"


def configure_git_identity(repo_root: Path) -> None:
    run_git(["config", "user.name", os.environ.get("DOCS_BOT_NAME", "sms-docs-bot")], cwd=repo_root)
    run_git(
        ["config", "user.email", os.environ.get("DOCS_BOT_EMAIL", "sms-docs-bot@example.invalid")],
        cwd=repo_root,
    )


def apply_documentation_updates(
    result: dict[str, Any],
    payload: dict[str, Any],
    repo_root: Path,
    target_files: list[dict[str, Any]],
) -> list[str]:
    if not result.get("updates_required"):
        print("No documentation updates required.")
        return []
    if result.get("review_required"):
        raise RuntimeError("Model marked result as review_required; refusing to apply automatically.")
    if payload.get("limits", {}).get("diff_limited"):
        raise RuntimeError("Diff was limited; refusing to apply automatically.")

    if not target_files:
        print("No target files matched this repository.")
        return []

    updated_paths: list[str] = []
    for item in target_files:
        operation = str(item.get("operation") or "update").lower()
        if operation not in {"create", "update"}:
            raise ValueError(f"Unsupported documentation operation: {operation}")
        content = item.get("content")
        if not isinstance(content, str) or not content.strip():
            raise ValueError(f"Target file has no content: {item.get('path')}")

        target_path = ensure_safe_doc_path(str(item.get("path") or ""), repo_root)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(content.rstrip() + "\n", encoding="utf-8")
        updated_paths.append(target_path.relative_to(repo_root).as_posix())

    return updated_paths


def has_staged_or_unstaged_changes(paths: list[str], repo_root: Path) -> bool:
    if not paths:
        return False
    status = run_git(["status", "--short", "--", *paths], cwd=repo_root)
    return bool(status.strip())


def github_api_request(method: str, url: str, token: str, payload: dict[str, Any]) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "sms-source-of-truth-doc-agent",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API request failed: {exc.code} {body}") from exc


def create_pull_request(
    repository: str,
    branch: str,
    result: dict[str, Any],
    payload: dict[str, Any],
    paths: list[str],
) -> str:
    token = os.environ.get("GH_DOCS_BOT_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GH_DOCS_BOT_TOKEN or GITHUB_TOKEN is required to open a documentation PR.")
    if not repository:
        raise RuntimeError("GITHUB_REPOSITORY is required to open a documentation PR.")

    pr_number = payload.get("pr", {}).get("number")
    title_suffix = f" for PR #{pr_number}" if pr_number else ""
    title = f"docs: source-of-truth update{title_suffix}"
    source_link = payload.get("pr", {}).get("html_url") or "manual workflow dispatch"
    base_branch = payload.get("pr", {}).get("base_branch") or os.environ.get("DOCS_PR_BASE_BRANCH") or "dev"
    body = "\n".join(
        [
            f"Source PR: {source_link}",
            "",
            f"Affected repository: {repository}",
            "",
            "Affected docs:",
            *[f"- `{path}`" for path in paths],
            "",
            f"Summary: {result.get('summary', 'Source-of-truth documentation update.')}",
            "",
            "Evidence: PR delta only. No full repository upload was requested.",
        ]
    )

    response = github_api_request(
        "POST",
        f"https://api.github.com/repos/{repository}/pulls",
        token,
        {
            "title": title,
            "head": branch,
            "base": base_branch,
            "body": body,
            "draft": True,
        },
    )
    return response.get("html_url", "")


def publish_documentation_pr(
    repository: str,
    result: dict[str, Any],
    payload: dict[str, Any],
    paths: list[str],
    repo_root: Path,
) -> str:
    if not paths:
        return ""
    configure_git_identity(repo_root)
    branch = branch_name(result, payload)
    run_git(["checkout", "-B", branch], cwd=repo_root)

    if not has_staged_or_unstaged_changes(paths, repo_root):
        print("Documentation target files were unchanged after applying model output.")
        return ""

    run_git(["add", "--", *paths], cwd=repo_root)
    pr_number = payload.get("pr", {}).get("number")
    commit_suffix = f" for PR #{pr_number}" if pr_number else ""
    run_git(["commit", "-m", f"docs: update source of truth{commit_suffix}"], cwd=repo_root)

    token = os.environ.get("GH_DOCS_BOT_TOKEN") or os.environ.get("GITHUB_TOKEN")
    full_name = full_github_repo_name(repository, payload.get("repository", ""))
    if token and repository:
        remote = f"https://x-access-token:{token}@github.com/{full_name}.git"
        run_git(["push", remote, f"HEAD:{branch}", "--force-with-lease"], cwd=repo_root)
    else:
        run_git(["push", "origin", branch, "--force-with-lease"], cwd=repo_root)

    pr_url = create_pull_request(full_name, branch, result, payload, paths)
    if pr_url:
        print(f"Opened documentation PR: {pr_url}")
    return pr_url


def confluence_auth_header() -> str:
    import base64

    email = os.environ.get("CONFLUENCE_EMAIL")
    token = os.environ.get("CONFLUENCE_API_TOKEN")
    if not email or not token:
        raise RuntimeError("CONFLUENCE_EMAIL and CONFLUENCE_API_TOKEN are required for Confluence updates.")
    raw = f"{email}:{token}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def confluence_base_url() -> str:
    base_url = os.environ.get("CONFLUENCE_BASE_URL", "").rstrip("/")
    if not base_url:
        raise RuntimeError("CONFLUENCE_BASE_URL is required for Confluence updates.")
    if not base_url.endswith("/wiki"):
        base_url += "/wiki"
    return base_url


def confluence_request(
    method: str,
    path: str,
    *,
    query: dict[str, str] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = f"{confluence_base_url()}{path}"
    if query:
        url += "?" + urlencode(query)
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Accept": "application/json",
            "Authorization": confluence_auth_header(),
            "Content-Type": "application/json",
            "User-Agent": "sms-source-of-truth-doc-agent",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Confluence API request failed: {exc.code} {body}") from exc


def confluence_find_page(title: str) -> dict[str, Any]:
    cql = f'space = "{CONFLUENCE_SPACE_KEY}" AND type = page AND title = "{title}"'
    response = confluence_request(
        "GET",
        "/rest/api/content/search",
        query={"cql": cql, "limit": "1", "expand": "body.storage,version,space"},
    )
    results = response.get("results") or []
    if not results:
        fuzzy_cql = f'space = "{CONFLUENCE_SPACE_KEY}" AND type = page AND title ~ "{title}"'
        response = confluence_request(
            "GET",
            "/rest/api/content/search",
            query={"cql": fuzzy_cql, "limit": "1", "expand": "body.storage,version,space"},
        )
        results = response.get("results") or []
    if not results:
        raise RuntimeError(f"Confluence page not found in {CONFLUENCE_SPACE_KEY}: {title}")
    return results[0]


def markdown_to_confluence_storage(markdown: str) -> str:
    lines = []
    for raw_line in markdown.splitlines():
        line = raw_line.rstrip()
        if not line:
            lines.append("<p />")
        elif line.startswith("### "):
            lines.append(f"<h3>{escape(line[4:])}</h3>")
        elif line.startswith("## "):
            lines.append(f"<h2>{escape(line[3:])}</h2>")
        elif line.startswith("# "):
            lines.append(f"<h1>{escape(line[2:])}</h1>")
        elif line.startswith("- "):
            lines.append(f"<p>&bull; {escape(line[2:])}</p>")
        else:
            lines.append(f"<p>{escape(line)}</p>")
    return "".join(lines)


def append_confluence_page(title: str, heading: str, markdown: str) -> str:
    page = confluence_find_page(title)
    page_id = page["id"]
    current_body = page.get("body", {}).get("storage", {}).get("value", "")
    current_version = int(page.get("version", {}).get("number", 1))
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    append_html = (
        "<hr />"
        f"<h2>{escape(timestamp)} - {escape(heading)}</h2>"
        f"{markdown_to_confluence_storage(markdown)}"
    )
    confluence_request(
        "PUT",
        f"/rest/api/content/{page_id}",
        payload={
            "id": page_id,
            "type": "page",
            "title": page["title"],
            "version": {"number": current_version + 1},
            "body": {"storage": {"value": current_body + append_html, "representation": "storage"}},
        },
    )
    webui = page.get("_links", {}).get("webui", "")
    return f"{confluence_base_url()}{webui}" if webui else page_id


def confluence_update_required(result: dict[str, Any]) -> bool:
    if result.get("confluence_update_required") is True:
        return True
    if result.get("confluence_updates"):
        return True
    text = " ".join(
        [
            str(result.get("summary", "")),
            json.dumps(result.get("implementation_components", {}), ensure_ascii=True),
        ]
    ).lower()
    triggers = [
        "architecture",
        "database",
        "storage",
        "business process",
        "data flow",
        "integration",
        "product functionality",
        "user workflow",
        "user experience",
        "administrator",
        "operator",
        "deployment",
        "container",
    ]
    return any(trigger in text for trigger in triggers)


def build_active_tasks_confluence_update(result: dict[str, Any], payload: dict[str, Any]) -> dict[str, str]:
    task = result.get("task") if isinstance(result.get("task"), dict) else {}
    pr_number = payload.get("pr", {}).get("number")
    source = f"PR #{pr_number}" if pr_number else "manual or local run"
    affected_repos = task.get("affected_repositories") or []
    target_files = [
        str(item.get("path"))
        for item in result.get("target_files", [])
        if isinstance(item, dict) and item.get("path")
    ]
    lines = [
        f"Task: {task.get('name') or result.get('summary') or 'Source-of-truth documentation update'}",
        f"Status: {task.get('status') or 'unknown'}",
        f"Source: {source}",
        f"Leading repository: {task.get('leading_repository') or payload.get('repository', '')}",
        f"Affected repositories: {', '.join(map(str, affected_repos)) if affected_repos else payload.get('repository', '')}",
        f"Summary: {result.get('summary', '')}",
    ]
    if target_files:
        lines.append("Affected docs:")
        lines.extend(f"- {path}" for path in target_files)
    lines.append("Business/process/operator impact: see repository source-of-truth docs and generated documentation PRs.")
    return {
        "target_area": CONFLUENCE_ACTIVE_TASKS_TITLE,
        "reason": "Mandatory active project and current task tracking.",
        "content": "\n".join(lines),
    }


def normalize_confluence_updates(result: dict[str, Any], payload: dict[str, Any]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for item in result.get("confluence_updates", []):
        if not isinstance(item, dict):
            continue
        target = str(item.get("target_area") or item.get("page_title") or "").strip()
        content = str(item.get("content") or "").strip()
        if target and content:
            updates.append(
                {
                    "target_area": target,
                    "reason": str(item.get("reason") or "Documentation agent update."),
                    "content": content,
                }
            )

    if confluence_update_required(result):
        has_active_update = any(update["target_area"] == CONFLUENCE_ACTIVE_TASKS_TITLE for update in updates)
        if not has_active_update:
            updates.insert(0, build_active_tasks_confluence_update(result, payload))
    return updates


def apply_confluence_updates(result: dict[str, Any], payload: dict[str, Any]) -> list[dict[str, str]]:
    applied: list[dict[str, str]] = []
    for update in normalize_confluence_updates(result, payload):
        url = append_confluence_page(update["target_area"], update["reason"], update["content"])
        applied.append({"target_area": update["target_area"], "url": url})
    return applied


def append_run_log(
    result: dict[str, Any],
    payload: dict[str, Any],
    *,
    mode: str,
    applied_paths: list[str] | None = None,
    pr_url: str = "",
    cross_repo_results: list[dict[str, Any]] | None = None,
    confluence_results: list[dict[str, str]] | None = None,
) -> None:
    applied_paths = applied_paths or []
    cross_repo_results = cross_repo_results or []
    confluence_results = confluence_results or []
    target_files = [
        str(item.get("path"))
        for item in result.get("target_files", [])
        if isinstance(item, dict) and item.get("path")
    ]
    limitations = result.get("limitations") or []
    pr_number = payload.get("pr", {}).get("number")
    source = f"PR #{pr_number}" if pr_number else "manual or local run"
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    RUN_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not RUN_LOG_FILE.exists():
        RUN_LOG_FILE.write_text(
            "# ChatGPT API Doc Agent Log\n\n"
            "This file records short source-of-truth documentation agent runs.\n\n",
            encoding="utf-8",
        )

    lines = [
        f"## {timestamp}",
        "",
        f"- Mode: `{mode}`",
        f"- Source: {source}",
        f"- Repository: `{payload.get('repository', '')}`",
        f"- Updates required: `{bool(result.get('updates_required'))}`",
        f"- Review required: `{bool(result.get('review_required'))}`",
        f"- Result JSON: `{OUTPUT_FILE.relative_to(ROOT).as_posix()}`",
        f"- Summary: {result.get('summary', '')}",
    ]
    if target_files:
        lines.append("- Target files:")
        lines.extend(f"  - `{path}`" for path in target_files)
    if applied_paths:
        lines.append("- Applied files:")
        lines.extend(f"  - `{path}`" for path in applied_paths)
    if pr_url:
        lines.append(f"- Documentation PR: {pr_url}")
    if cross_repo_results:
        lines.append("- Repository results:")
        for repo_result in cross_repo_results:
            lines.append(f"  - `{repo_result.get('repository', '')}`")
            for path in repo_result.get("applied_paths", []):
                lines.append(f"    - `{path}`")
            if repo_result.get("pr_url"):
                lines.append(f"    - PR: {repo_result.get('pr_url')}")
    if confluence_results:
        lines.append("- Confluence updates:")
        for confluence_result in confluence_results:
            lines.append(
                f"  - `{confluence_result.get('target_area', '')}`: {confluence_result.get('url', '')}"
            )
    if limitations:
        lines.append("- Limitations:")
        lines.extend(f"  - `{item}`" for item in limitations)
    lines.append("")
    lines.append("---")
    lines.append("")

    with RUN_LOG_FILE.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines))


def apply_and_publish_all_repositories(result: dict[str, Any], payload: dict[str, Any]) -> list[dict[str, Any]]:
    if not result.get("updates_required"):
        print("No documentation updates required.")
        return []
    if result.get("review_required"):
        raise RuntimeError("Model marked result as review_required; refusing to apply automatically.")
    if payload.get("limits", {}).get("diff_limited"):
        raise RuntimeError("Diff was limited; refusing to apply automatically.")

    current_repository = payload.get("repository", "")
    groups = grouped_target_files(result, current_repository)
    if not groups:
        print("No target files returned by model.")
        return []

    repo_results: list[dict[str, Any]] = []
    for repository, target_files in groups.items():
        repo_root = resolve_repo_root(repository, current_repository)
        applied_paths = apply_documentation_updates(result, payload, repo_root, target_files)
        pr_url = publish_documentation_pr(repository, result, payload, applied_paths, repo_root)
        repo_results.append(
            {
                "repository": repository,
                "repo_root": str(repo_root),
                "applied_paths": applied_paths,
                "pr_url": pr_url,
            }
        )
    return repo_results


def main() -> None:
    if not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is required.")

    payload = build_input()
    result = call_openai(payload)
    write_result(result)

    if not APPLY_CHANGES:
        confluence_results = apply_confluence_updates(result, payload) if APPLY_CONFLUENCE else []
        append_run_log(result, payload, mode="dry-run", confluence_results=confluence_results)
        print("DOCS_AGENT_APPLY is not true; leaving result as dry-run JSON.")
        return

    repo_results = apply_and_publish_all_repositories(result, payload)
    confluence_results = apply_confluence_updates(result, payload) if APPLY_CONFLUENCE else []
    applied_paths = [
        f"{repo_result['repository']}:{path}"
        for repo_result in repo_results
        for path in repo_result.get("applied_paths", [])
    ]
    append_run_log(
        result,
        payload,
        mode="apply",
        applied_paths=applied_paths,
        cross_repo_results=repo_results,
        confluence_results=confluence_results,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
