#!/usr/bin/env python3
"""Shared helpers for reusable PR review automation scripts."""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any


CODEX_HOME = Path.home() / ".codex"
AUTOMATIONS_DIR = CODEX_HOME / "automations"
AUTOMATIONS_DB = CODEX_HOME / "sqlite" / "codex-dev.db"
DEFAULT_WORKTREE_ROOT = CODEX_HOME / "worktrees" / "pr-review"
COPILOT_LOGINS = {
    "copilot-pull-request-reviewer[bot]",
    "github-copilot[bot]",
    "copilot[bot]",
}


class ScriptError(RuntimeError):
    """Raised for expected script failures."""


def run(
    cmd: list[str],
    *,
    cwd: str | Path | None = None,
    check: bool = True,
    capture_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd is not None else None,
        check=False,
        capture_output=capture_output,
        text=True,
    )
    if check and result.returncode != 0:
        raise ScriptError(
            f"command failed ({result.returncode}): {' '.join(cmd)}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return result


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip())
    slug = slug.strip("-.").lower()
    return slug or "value"


def json_print(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def parse_args_with_common(
    description: str,
    *,
    include_automation_id: bool = False,
    include_poll_minutes: bool = False,
    require_branch: bool = True,
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--repo-root", required=True, help="Absolute path to the primary repo checkout.")
    parser.add_argument("--repo-name", required=True, help="Repository name used for worktree/automation IDs.")
    parser.add_argument("--pr", required=True, type=int, help="GitHub pull request number.")
    if require_branch:
        parser.add_argument("--branch", required=True, help="PR branch name.")
    if include_automation_id:
        parser.add_argument("--automation-id", help="Codex automation ID to update or remove.")
    if include_poll_minutes:
        parser.add_argument("--poll-minutes", type=int, default=5, help="Polling interval in minutes.")
    parser.add_argument(
        "--worktree-root",
        default=str(DEFAULT_WORKTREE_ROOT),
        help="Root directory under which PR-specific worktrees are created.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="json",
        help="Output format.",
    )
    return parser


def repo_owner_and_name(repo_root: str | Path) -> tuple[str, str]:
    remote = run(
        ["git", "-C", str(repo_root), "remote", "get-url", "origin"],
    ).stdout.strip()

    patterns = (
        r"github\.com[:/](?P<owner>[^/]+)/(?P<repo>[^/.]+?)(?:\.git)?$",
        r"git@github\.com:(?P<owner>[^/]+)/(?P<repo>[^/.]+?)(?:\.git)?$",
    )
    for pattern in patterns:
        match = re.search(pattern, remote)
        if match:
            return match.group("owner"), match.group("repo")
    raise ScriptError(f"unable to parse GitHub owner/repo from remote URL: {remote}")


def verify_repo_name(repo_root: str | Path, repo_name: str) -> tuple[str, str]:
    owner, actual_repo_name = repo_owner_and_name(repo_root)
    if actual_repo_name != repo_name:
        raise ScriptError(
            f"--repo-name mismatch: expected {actual_repo_name!r} based on origin remote, got {repo_name!r}"
        )
    return owner, actual_repo_name


def verify_gh_auth() -> None:
    run(["gh", "auth", "status"])


def github_graphql(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    result = run(
        [
            "gh",
            "api",
            "graphql",
            "-f",
            f"query={query}",
            "-F",
            f"owner={variables['owner']}",
            "-F",
            f"repo={variables['repo']}",
            "-F",
            f"pr={variables['pr']}",
        ]
    )
    data = json.loads(result.stdout)
    if "errors" in data:
        raise ScriptError(f"GitHub GraphQL errors: {json.dumps(data['errors'], indent=2)}")
    return data


def is_copilot_login(login: str | None) -> bool:
    if not login:
        return False
    normalized = login.lower()
    return normalized in COPILOT_LOGINS or "copilot" in normalized


def fetch_review_threads(repo_root: str | Path, repo_name: str, pr_number: int) -> dict[str, Any]:
    owner, repo = verify_repo_name(repo_root, repo_name)
    query = textwrap.dedent(
        """
        query($owner: String!, $repo: String!, $pr: Int!) {
          repository(owner: $owner, name: $repo) {
            pullRequest(number: $pr) {
              number
              url
              title
              state
              reviewThreads(first: 100) {
                nodes {
                  id
                  isResolved
                  isOutdated
                  path
                  line
                  originalLine
                  comments(first: 100) {
                    nodes {
                      id
                      author {
                        login
                      }
                      body
                      createdAt
                      url
                      path
                      line
                    }
                  }
                }
              }
            }
          }
        }
        """
    ).strip()
    payload = github_graphql(query, {"owner": owner, "repo": repo, "pr": pr_number})
    pull_request = payload["data"]["repository"]["pullRequest"]
    if not pull_request:
        raise ScriptError(f"pull request #{pr_number} not found for {owner}/{repo}")
    return pull_request


def classify_threads(pull_request: dict[str, Any]) -> dict[str, Any]:
    threads = pull_request["reviewThreads"]["nodes"] or []
    copilot_threads: list[dict[str, Any]] = []
    unresolved_copilot_threads: list[dict[str, Any]] = []
    unresolved_non_copilot_threads: list[dict[str, Any]] = []

    for thread in threads:
        comments = thread["comments"]["nodes"] or []
        comment_logins = [comment.get("author", {}).get("login") for comment in comments]
        summary = {
            "id": thread["id"],
            "isResolved": bool(thread["isResolved"]),
            "isOutdated": bool(thread["isOutdated"]),
            "path": thread.get("path"),
            "line": thread.get("line"),
            "originalLine": thread.get("originalLine"),
            "authors": [login for login in comment_logins if login],
            "comments": [
                {
                    "id": comment["id"],
                    "author": comment.get("author", {}).get("login"),
                    "body": comment.get("body"),
                    "createdAt": comment.get("createdAt"),
                    "url": comment.get("url"),
                    "path": comment.get("path"),
                    "line": comment.get("line"),
                }
                for comment in comments
            ],
        }
        is_copilot_thread = any(is_copilot_login(login) for login in comment_logins)
        if is_copilot_thread:
            copilot_threads.append(summary)
            if not thread["isResolved"]:
                unresolved_copilot_threads.append(summary)
        elif not thread["isResolved"]:
            unresolved_non_copilot_threads.append(summary)

    status = "clean" if not unresolved_copilot_threads else "needs_work"
    return {
        "status": status,
        "pr": {
            "number": pull_request["number"],
            "url": pull_request["url"],
            "title": pull_request["title"],
            "state": pull_request["state"],
        },
        "totals": {
            "threads": len(threads),
            "copilot_threads": len(copilot_threads),
            "unresolved_copilot_threads": len(unresolved_copilot_threads),
            "unresolved_non_copilot_threads": len(unresolved_non_copilot_threads),
        },
        "unresolved_copilot_threads": unresolved_copilot_threads,
        "unresolved_non_copilot_threads": unresolved_non_copilot_threads,
    }


def worktree_path(repo_name: str, pr_number: int, branch: str, worktree_root: str | Path) -> Path:
    return Path(worktree_root) / slugify(repo_name) / f"pr-{pr_number}-{slugify(branch)}"


def tracked_worktrees(repo_root: str | Path) -> dict[str, dict[str, Any]]:
    result = run(["git", "-C", str(repo_root), "worktree", "list", "--porcelain"])
    entries: dict[str, dict[str, Any]] = {}
    current: dict[str, Any] | None = None
    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()
        if line.startswith("worktree "):
            if current:
                entries[current["path"]] = current
            current = {"path": line.split(" ", 1)[1]}
        elif current is not None and line:
            key, _, value = line.partition(" ")
            current[key] = value
    if current:
        entries[current["path"]] = current
    return entries


def branch_checked_out_elsewhere(repo_root: str | Path, branch: str, path_to_ignore: str | Path | None = None) -> str | None:
    for path, entry in tracked_worktrees(repo_root).items():
        if path_to_ignore and Path(path) == Path(path_to_ignore):
            continue
        if entry.get("branch", "").endswith(f"/{branch}"):
            return path
    return None


def git_status_is_clean(worktree: str | Path) -> bool:
    result = run(
        ["git", "-C", str(worktree), "status", "--porcelain", "--untracked-files=normal"],
    )
    return result.stdout.strip() == ""


def ensure_worktree(repo_root: str | Path, repo_name: str, pr_number: int, branch: str, worktree_root: str | Path) -> dict[str, Any]:
    verify_repo_name(repo_root, repo_name)
    target = worktree_path(repo_name, pr_number, branch, worktree_root)
    target.parent.mkdir(parents=True, exist_ok=True)

    run(["git", "-C", str(repo_root), "fetch", "origin", branch])

    tracked = tracked_worktrees(repo_root)
    target_key = str(target)
    existing = tracked.get(target_key)
    if existing:
        if not git_status_is_clean(target):
            raise ScriptError(f"existing worktree is dirty: {target}")
        return {"status": "ready", "worktree": str(target), "created": False}

    if target.exists() and not any(target.iterdir()):
        target.rmdir()
    elif target.exists():
        raise ScriptError(f"target worktree path exists but is not a registered git worktree: {target}")

    other_path = branch_checked_out_elsewhere(repo_root, branch)
    if other_path:
        raise ScriptError(
            f"branch {branch!r} is already checked out in another worktree: {other_path}"
        )

    run(
        [
            "git",
            "-C",
            str(repo_root),
            "worktree",
            "add",
            "-B",
            branch,
            str(target),
            f"origin/{branch}",
        ]
    )
    if not git_status_is_clean(target):
        raise ScriptError(f"newly created worktree is unexpectedly dirty: {target}")
    return {"status": "ready", "worktree": str(target), "created": True}


def sync_worktree_to_remote(repo_root: str | Path, branch: str, worktree: str | Path) -> dict[str, Any]:
    if not git_status_is_clean(worktree):
        raise ScriptError(f"refusing to sync dirty worktree: {worktree}")

    run(["git", "-C", str(repo_root), "fetch", "origin", branch])
    remote_ref = f"origin/{branch}"
    remote_head = run(["git", "-C", str(repo_root), "rev-parse", remote_ref]).stdout.strip()
    local_head = run(["git", "-C", str(worktree), "rev-parse", "HEAD"]).stdout.strip()

    if local_head != remote_head:
        run(["git", "-C", str(worktree), "reset", "--hard", remote_ref])
    run(["git", "-C", str(worktree), "clean", "-fd"])
    return {"status": "ready", "worktree": str(worktree), "head": remote_head, "changed": local_head != remote_head}


def codex_exec_review(worktree: str | Path, pr_number: int, branch: str) -> dict[str, Any]:
    prompt = textwrap.dedent(
        f"""
        Use the skill at /Users/jordan/.codex/skills/pr-review-executor/SKILL.md.

        You are working in a dedicated PR review worktree for PR #{pr_number} on branch {branch}.
        Handle all unresolved Copilot code review feedback on the current branch.
        Apply only targeted fixes for Copilot feedback, run npm run typecheck and any targeted validation needed for touched files, commit scoped changes, push, explicitly request reviewer copilot-pull-request-reviewer, and resolve threads only after the fix is pushed.

        If there are no unresolved Copilot review comments when you inspect the PR, report that clearly and make no code changes.
        """
    ).strip()

    result = run(
        [
            "codex",
            "exec",
            "--cd",
            str(worktree),
            "--dangerously-bypass-approvals-and-sandbox",
            "--add-dir",
            "/Users/jordan/source/tools",
            "--add-dir",
            "/Users/jordan/.codex/skills",
            prompt,
        ],
        check=False,
    )

    return {
        "status": "ok" if result.returncode == 0 else "error",
        "exit_code": result.returncode,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
    }


def automation_dir(automation_id: str) -> Path:
    return AUTOMATIONS_DIR / automation_id


def automation_toml_text(
    *,
    automation_id: str,
    name: str,
    prompt: str,
    status: str,
    rrule: str,
    cwds: list[str],
    created_at_ms: int,
    updated_at_ms: int,
) -> str:
    escaped_prompt = prompt.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    cwd_items = ", ".join(f'"{cwd}"' for cwd in cwds)
    return textwrap.dedent(
        f"""
        version = 1
        id = "{automation_id}"
        name = "{name}"
        prompt = "{escaped_prompt}"
        status = "{status}"
        rrule = "{rrule}"
        execution_environment = "local"
        cwds = [{cwd_items}]
        created_at = {created_at_ms}
        updated_at = {updated_at_ms}
        """
    ).strip() + "\n"


def upsert_automation_record(
    *,
    automation_id: str,
    name: str,
    prompt: str,
    status: str,
    rrule: str,
    cwds: list[str],
) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    AUTOMATIONS_DIR.mkdir(parents=True, exist_ok=True)
    automation_path = automation_dir(automation_id)
    automation_path.mkdir(parents=True, exist_ok=True)

    if AUTOMATIONS_DB.exists():
        connection = sqlite3.connect(AUTOMATIONS_DB)
        try:
            connection.execute(
                """
                INSERT INTO automations (id, name, prompt, status, next_run_at, last_run_at, cwds, rrule, created_at, updated_at)
                VALUES (?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  name=excluded.name,
                  prompt=excluded.prompt,
                  status=excluded.status,
                  cwds=excluded.cwds,
                  rrule=excluded.rrule,
                  updated_at=excluded.updated_at
                """,
                (
                    automation_id,
                    name,
                    prompt,
                    status,
                    json.dumps(cwds),
                    rrule,
                    now_ms,
                    now_ms,
                ),
            )
            connection.commit()
        finally:
            connection.close()

    toml = automation_toml_text(
        automation_id=automation_id,
        name=name,
        prompt=prompt,
        status=status,
        rrule=rrule,
        cwds=cwds,
        created_at_ms=now_ms,
        updated_at_ms=now_ms,
    )
    (automation_path / "automation.toml").write_text(toml, encoding="utf-8")

    memory_path = automation_path / "memory.md"
    if not memory_path.exists():
        memory_path.write_text(
            "# PR Review Automation\n\nThis automation is managed by shared tooling in /Users/jordan/source/tools.\n",
            encoding="utf-8",
        )

    return {"status": "ready", "automation_id": automation_id, "automation_dir": str(automation_path)}


def disable_or_delete_automation(automation_id: str) -> dict[str, Any]:
    deleted = False
    paused = False
    if AUTOMATIONS_DB.exists():
        connection = sqlite3.connect(AUTOMATIONS_DB)
        try:
            update_cursor = connection.execute(
                "UPDATE automations SET status = ?, updated_at = ? WHERE id = ?",
                ("PAUSED", int(time.time() * 1000), automation_id),
            )
            paused = update_cursor.rowcount > 0
            delete_cursor = connection.execute("DELETE FROM automations WHERE id = ?", (automation_id,))
            deleted = delete_cursor.rowcount > 0
            connection.commit()
        finally:
            connection.close()

    automation_path = automation_dir(automation_id)
    if automation_path.exists():
        for child in sorted(automation_path.rglob("*"), reverse=True):
            if child.is_file() or child.is_symlink():
                child.unlink()
            elif child.is_dir():
                child.rmdir()
        automation_path.rmdir()
        deleted = True

    return {"status": "ready", "paused": paused, "deleted": deleted, "automation_id": automation_id}


def remove_worktree(repo_root: str | Path, worktree: str | Path) -> dict[str, Any]:
    worktree_path_obj = Path(worktree)
    if not worktree_path_obj.exists():
        return {"status": "ready", "removed": False, "worktree": str(worktree_path_obj)}

    if not git_status_is_clean(worktree_path_obj):
        raise ScriptError(f"refusing to remove dirty worktree: {worktree_path_obj}")

    run(["git", "-C", str(repo_root), "worktree", "remove", str(worktree_path_obj)])
    run(["git", "-C", str(repo_root), "worktree", "prune"])
    return {"status": "ready", "removed": True, "worktree": str(worktree_path_obj)}


def emit_payload(payload: dict[str, Any], output_format: str) -> None:
    if output_format == "json":
        json_print(payload)
        return

    status = payload.get("status", "unknown")
    print(f"status={status}")
    for key, value in payload.items():
        if key == "status":
            continue
        if isinstance(value, (dict, list)):
            print(f"{key}={json.dumps(value, sort_keys=True)}")
        else:
            print(f"{key}={value}")


def handle_main(main_fn) -> None:
    try:
        main_fn()
    except ScriptError as exc:
        emit_payload({"status": "blocked", "error": str(exc)}, "json")
        sys.exit(1)
    except KeyboardInterrupt:
        emit_payload({"status": "error", "error": "interrupted"}, "json")
        sys.exit(130)
