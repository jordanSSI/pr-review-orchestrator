#!/usr/bin/env python3
"""Track active PRs against Codex threads and resume the right thread on review activity."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import sqlite3
import subprocess
import tempfile
import textwrap
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from pr_review_common import (
    CODEX_HOME,
    ScriptError,
    ensure_worktree,
    fetch_review_threads,
    remove_worktree,
    repo_owner_and_name,
    run,
    slugify,
    sync_worktree_to_remote,
    verify_gh_auth,
)


PROJECT_DIR = Path(__file__).resolve().parent
VAR_DIR = PROJECT_DIR / "var"
CODEX_STATE_DB = CODEX_HOME / "state_5.sqlite"
COORDINATOR_DB = VAR_DIR / "pr-review-coordinator.db"
DEFAULT_POLL_SECONDS = 300


SCHEMA = """
CREATE TABLE IF NOT EXISTS tracked_prs (
    key TEXT PRIMARY KEY,
    repo_root TEXT NOT NULL,
    repo_owner TEXT NOT NULL,
    repo_name TEXT NOT NULL,
    pr_number INTEGER NOT NULL,
    pr_url TEXT NOT NULL,
    pr_title TEXT NOT NULL,
    pr_state TEXT NOT NULL,
    branch TEXT NOT NULL,
    base_branch TEXT,
    worktree_path TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    thread_title TEXT,
    status TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    last_review_signature TEXT,
    last_review_status TEXT,
    last_review_comment_at TEXT,
    last_polled_at INTEGER,
    last_prompted_at INTEGER,
    last_run_status TEXT,
    last_run_summary TEXT,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS tracked_prs_repo_pr
    ON tracked_prs(repo_root, pr_number);
"""


@dataclass
class TrackedPR:
    key: str
    repo_root: str
    repo_owner: str
    repo_name: str
    pr_number: int
    pr_url: str
    pr_title: str
    pr_state: str
    branch: str
    base_branch: str | None
    worktree_path: str
    thread_id: str
    thread_title: str | None
    status: str
    active: int
    last_review_signature: str | None
    last_review_status: str | None
    last_review_comment_at: str | None
    last_polled_at: int | None
    last_prompted_at: int | None
    last_run_status: str | None
    last_run_summary: str | None
    last_error: str | None
    created_at: int
    updated_at: int


def now_ms() -> int:
    return int(time.time() * 1000)


def connect_db() -> sqlite3.Connection:
    VAR_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(COORDINATOR_DB)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA)
    return connection


def row_to_tracked_pr(row: sqlite3.Row) -> TrackedPR:
    return TrackedPR(**dict(row))


def lookup_thread(thread_id: str) -> dict[str, Any] | None:
    if not CODEX_STATE_DB.exists():
        return None
    connection = sqlite3.connect(CODEX_STATE_DB)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            "SELECT id, cwd, title, archived, git_branch, git_origin_url FROM threads WHERE id = ?",
            (thread_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        connection.close()


def latest_thread_for_repo(repo_root: str) -> dict[str, Any] | None:
    if not CODEX_STATE_DB.exists():
        return None
    connection = sqlite3.connect(CODEX_STATE_DB)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            """
            SELECT id, cwd, title, archived, git_branch, git_origin_url
            FROM threads
            WHERE archived = 0 AND cwd = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (repo_root,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        connection.close()


def resolve_thread(repo_root: str, explicit_thread_id: str | None) -> dict[str, Any]:
    thread_id = explicit_thread_id or os.environ.get("CODEX_THREAD_ID")
    thread = lookup_thread(thread_id) if thread_id else None
    if thread:
        return thread

    fallback = latest_thread_for_repo(repo_root)
    if fallback:
        return fallback

    raise ScriptError(
        "unable to determine Codex thread id; run this from the target Codex thread or pass --thread-id"
    )


def ensure_repo_name(repo_root: str, repo_name: str | None) -> tuple[str, str]:
    owner, detected_repo = repo_owner_and_name(repo_root)
    if repo_name and repo_name != detected_repo:
        raise ScriptError(f"--repo-name mismatch: expected {detected_repo!r}, got {repo_name!r}")
    return owner, detected_repo


def current_branch(repo_root: str) -> str:
    return run(["git", "-C", repo_root, "branch", "--show-current"]).stdout.strip()


def repo_default_branch(repo_root: str) -> str:
    try:
        result = run(["git", "-C", repo_root, "symbolic-ref", "refs/remotes/origin/HEAD"])
        ref = result.stdout.strip()
        return ref.rsplit("/", 1)[-1]
    except ScriptError:
        result = run(["gh", "repo", "view", "--json", "defaultBranchRef", "-q", ".defaultBranchRef.name"], cwd=repo_root)
        return result.stdout.strip()


def has_uncommitted_changes(repo_root: str) -> bool:
    result = run(["git", "-C", repo_root, "status", "--porcelain", "--untracked-files=normal"])
    return bool(result.stdout.strip())


def ensure_branch(repo_root: str, branch: str) -> dict[str, Any]:
    branch_before = current_branch(repo_root)
    if branch_before == branch:
        return {"status": "ready", "branch": branch, "created": False}

    existing = run(["git", "-C", repo_root, "branch", "--list", branch]).stdout.strip()
    if existing:
        run(["git", "-C", repo_root, "switch", branch])
        return {"status": "ready", "branch": branch, "created": False}

    run(["git", "-C", repo_root, "switch", "-c", branch])
    return {"status": "ready", "branch": branch, "created": True}


def commit_all_changes(repo_root: str, message: str) -> dict[str, Any]:
    if not has_uncommitted_changes(repo_root):
        return {"status": "ready", "committed": False}

    run(["git", "-C", repo_root, "add", "-A"])
    run(["git", "-C", repo_root, "commit", "-m", message])
    sha = run(["git", "-C", repo_root, "rev-parse", "HEAD"]).stdout.strip()
    return {"status": "ready", "committed": True, "sha": sha}


def push_branch(repo_root: str, branch: str) -> dict[str, Any]:
    run(["git", "-C", repo_root, "push", "-u", "origin", branch])
    sha = run(["git", "-C", repo_root, "rev-parse", "HEAD"]).stdout.strip()
    return {"status": "ready", "branch": branch, "sha": sha}


def find_open_pr_for_branch(repo_root: str, branch: str) -> dict[str, Any] | None:
    result = run(
        ["gh", "pr", "list", "--head", branch, "--state", "open", "--json", "number,url,title,headRefName,baseRefName,state"],
        cwd=repo_root,
    )
    prs = json.loads(result.stdout)
    return prs[0] if prs else None


def create_or_reuse_pr(
    repo_root: str,
    branch: str,
    base_branch: str,
    title: str,
    body: str,
    draft: bool,
) -> dict[str, Any]:
    existing = find_open_pr_for_branch(repo_root, branch)
    if existing:
        return {"status": "ready", "created": False, **existing}

    cmd = ["gh", "pr", "create", "--head", branch, "--base", base_branch, "--title", title, "--body", body]
    if draft:
        cmd.append("--draft")
    result = run(cmd, cwd=repo_root)
    pr_url = result.stdout.strip()
    pr = find_open_pr_for_branch(repo_root, branch)
    if not pr:
        raise ScriptError(f"created PR but could not resolve metadata for {pr_url}")
    return {"status": "ready", "created": True, **pr}


def serialize_unresolved_threads(pull_request: dict[str, Any]) -> list[dict[str, Any]]:
    unresolved: list[dict[str, Any]] = []
    for thread in pull_request["reviewThreads"]["nodes"] or []:
        if thread["isResolved"]:
            continue
        comments = thread["comments"]["nodes"] or []
        latest_comment = comments[-1] if comments else None
        unresolved.append(
            {
                "id": thread["id"],
                "path": thread.get("path"),
                "line": thread.get("line") or thread.get("originalLine"),
                "isOutdated": bool(thread.get("isOutdated")),
                "authors": [comment.get("author", {}).get("login") for comment in comments if comment.get("author")],
                "latest_comment_id": latest_comment.get("id") if latest_comment else None,
                "latest_comment_at": latest_comment.get("createdAt") if latest_comment else None,
                "latest_comment_author": latest_comment.get("author", {}).get("login") if latest_comment else None,
                "latest_comment_url": latest_comment.get("url") if latest_comment else None,
                "latest_comment_body": (latest_comment.get("body") or "").strip() if latest_comment else None,
            }
        )
    unresolved.sort(key=lambda item: ((item["path"] or ""), item["line"] or 0, item["id"]))
    return unresolved


def review_snapshot(repo_root: str, repo_name: str, pr_number: int) -> dict[str, Any]:
    pull_request = fetch_review_threads(repo_root, repo_name, pr_number)
    unresolved = serialize_unresolved_threads(pull_request)
    signature = hashlib.sha256(json.dumps(unresolved, sort_keys=True).encode("utf-8")).hexdigest()
    latest_comment_at = None
    if unresolved:
        latest_comment_at = max(item["latest_comment_at"] or "" for item in unresolved) or None
    return {
        "pr": {
            "number": pull_request["number"],
            "url": pull_request["url"],
            "title": pull_request["title"],
            "state": pull_request["state"],
        },
        "status": "awaiting_final_test" if not unresolved else "needs_review",
        "signature": signature,
        "latest_comment_at": latest_comment_at,
        "unresolved_threads": unresolved,
    }


def summarize_threads(unresolved_threads: list[dict[str, Any]]) -> str:
    if not unresolved_threads:
        return "No unresolved review threads remain."

    lines: list[str] = []
    for thread in unresolved_threads[:12]:
        body = (thread.get("latest_comment_body") or "").replace("\r", " ").replace("\n", " ").strip()
        if len(body) > 240:
            body = body[:237] + "..."
        location = thread.get("path") or "<unknown file>"
        if thread.get("line"):
            location = f"{location}:{thread['line']}"
        author = thread.get("latest_comment_author") or "unknown"
        lines.append(f"- {location} [{author}] {body}")
    if len(unresolved_threads) > 12:
        lines.append(f"- ... {len(unresolved_threads) - 12} more unresolved threads")
    return "\n".join(lines)


def resume_prompt(record: TrackedPR, snapshot: dict[str, Any]) -> str:
    return textwrap.dedent(
        f"""
        Continue this existing Codex thread for PR follow-up.

        Repository: {record.repo_root}
        PR: #{record.pr_number} {record.pr_title}
        PR URL: {record.pr_url}
        Branch: {record.branch}
        Dedicated PR worktree: {record.worktree_path}

        Use the skill at /Users/jordan/.codex/skills/pr-review-executor/SKILL.md.

        Work only against the dedicated PR worktree for code changes. Do not use the main checkout for edits.
        Pull the latest PR branch state into that worktree before making changes.
        Address unresolved GitHub review feedback with minimal targeted fixes.
        Run the relevant validation for the touched files, including repo typecheck if available.
        Commit and push scoped review-follow-up changes when needed.
        Re-request reviewer `copilot-pull-request-reviewer` after every push.
        Resolve threads only after fixes are pushed, or leave a clear rationale when no code change is needed.

        Current unresolved review threads:
        {summarize_threads(snapshot["unresolved_threads"])}

        If no code changes are required after inspection, say so clearly in your final summary.
        """
    ).strip()


def run_codex_resume(record: TrackedPR, snapshot: dict[str, Any], dry_run: bool) -> dict[str, Any]:
    prompt = resume_prompt(record, snapshot)
    if dry_run:
        return {
            "status": "dry_run",
            "thread_id": record.thread_id,
            "prompt_preview": prompt,
        }

    with tempfile.NamedTemporaryFile(prefix="codex-pr-followup-", suffix=".txt", delete=False) as output_file:
        output_path = output_file.name

    try:
        result = subprocess.run(
            [
                "codex",
                "exec",
                "resume",
                record.thread_id,
                prompt,
                "--dangerously-bypass-approvals-and-sandbox",
                "--output-last-message",
                output_path,
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        last_message = Path(output_path).read_text(encoding="utf-8").strip() if Path(output_path).exists() else ""
        return {
            "status": "ok" if result.returncode == 0 else "error",
            "exit_code": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
            "last_message": last_message,
        }
    finally:
        Path(output_path).unlink(missing_ok=True)


def upsert_tracked_pr(record: dict[str, Any]) -> TrackedPR:
    current_time = now_ms()
    payload = {
        **record,
        "updated_at": current_time,
    }
    connection = connect_db()
    try:
        existing = connection.execute("SELECT created_at FROM tracked_prs WHERE key = ?", (payload["key"],)).fetchone()
        created_at = existing["created_at"] if existing else current_time
        payload["created_at"] = created_at
        columns = sorted(payload.keys())
        placeholders = ", ".join(f":{column}" for column in columns)
        assignments = ", ".join(f"{column} = excluded.{column}" for column in columns if column not in {"key", "created_at"})
        connection.execute(
            f"""
            INSERT INTO tracked_prs ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT(key) DO UPDATE SET
            {assignments}
            """,
            payload,
        )
        connection.commit()
        row = connection.execute("SELECT * FROM tracked_prs WHERE key = ?", (payload["key"],)).fetchone()
        return row_to_tracked_pr(row)
    finally:
        connection.close()


def update_tracked_pr(key: str, **changes: Any) -> TrackedPR:
    if not changes:
        raise ScriptError("update_tracked_pr requires at least one change")
    changes["updated_at"] = now_ms()
    assignments = ", ".join(f"{column} = :{column}" for column in changes)
    connection = connect_db()
    try:
        changes["key"] = key
        connection.execute(f"UPDATE tracked_prs SET {assignments} WHERE key = :key", changes)
        connection.commit()
        row = connection.execute("SELECT * FROM tracked_prs WHERE key = ?", (key,)).fetchone()
        if not row:
            raise ScriptError(f"tracked PR not found: {key}")
        return row_to_tracked_pr(row)
    finally:
        connection.close()


def list_tracked_prs(active_only: bool = False) -> list[TrackedPR]:
    connection = connect_db()
    try:
        sql = "SELECT * FROM tracked_prs"
        params: tuple[Any, ...] = ()
        if active_only:
            sql += " WHERE active = 1"
        sql += " ORDER BY updated_at DESC"
        return [row_to_tracked_pr(row) for row in connection.execute(sql, params).fetchall()]
    finally:
        connection.close()


def get_tracked_pr(key: str) -> TrackedPR:
    connection = connect_db()
    try:
        row = connection.execute("SELECT * FROM tracked_prs WHERE key = ?", (key,)).fetchone()
        if not row:
            raise ScriptError(f"tracked PR not found: {key}")
        return row_to_tracked_pr(row)
    finally:
        connection.close()


def tracked_pr_key(repo_name: str, pr_number: int) -> str:
    return f"{slugify(repo_name)}-pr-{pr_number}"


def register_tracking(
    *,
    repo_root: str,
    repo_name: str | None,
    pr_number: int,
    branch: str,
    worktree_root: str,
    thread_id: str | None,
) -> dict[str, Any]:
    verify_gh_auth()
    owner, detected_repo_name = ensure_repo_name(repo_root, repo_name)
    pr = run(
        ["gh", "pr", "view", str(pr_number), "--json", "number,url,title,headRefName,baseRefName,state"],
        cwd=repo_root,
    )
    pr_info = json.loads(pr.stdout)
    thread = resolve_thread(repo_root, thread_id)
    worktree = ensure_worktree(repo_root, detected_repo_name, pr_number, branch, worktree_root)
    snapshot = review_snapshot(repo_root, detected_repo_name, pr_number)

    record = upsert_tracked_pr(
        {
            "key": tracked_pr_key(detected_repo_name, pr_number),
            "repo_root": repo_root,
            "repo_owner": owner,
            "repo_name": detected_repo_name,
            "pr_number": pr_info["number"],
            "pr_url": pr_info["url"],
            "pr_title": pr_info["title"],
            "pr_state": pr_info["state"],
            "branch": pr_info["headRefName"],
            "base_branch": pr_info.get("baseRefName"),
            "worktree_path": worktree["worktree"],
            "thread_id": thread["id"],
            "thread_title": thread.get("title"),
            "status": snapshot["status"],
            "active": 1,
            "last_review_signature": snapshot["signature"],
            "last_review_status": snapshot["status"],
            "last_review_comment_at": snapshot["latest_comment_at"],
            "last_polled_at": now_ms(),
            "last_prompted_at": None,
            "last_run_status": "registered",
            "last_run_summary": "PR tracking registered",
            "last_error": None,
        }
    )

    return {
        "status": "ready",
        "tracked_pr": tracked_pr_to_dict(record),
        "review": snapshot,
        "worktree": worktree,
        "thread": thread,
    }


def handoff_pr(
    *,
    repo_root: str,
    repo_name: str | None,
    branch: str,
    base_branch: str | None,
    commit_message: str,
    pr_title: str,
    pr_body: str,
    draft: bool,
    worktree_root: str,
    thread_id: str | None,
) -> dict[str, Any]:
    verify_gh_auth()
    owner, detected_repo_name = ensure_repo_name(repo_root, repo_name)
    base = base_branch or repo_default_branch(repo_root)
    branch_result = ensure_branch(repo_root, branch)
    commit_result = commit_all_changes(repo_root, commit_message)
    push_result = push_branch(repo_root, branch)
    pr_result = create_or_reuse_pr(repo_root, branch, base, pr_title, pr_body, draft)
    worktree_result = ensure_worktree(repo_root, detected_repo_name, pr_result["number"], branch, worktree_root)
    thread = resolve_thread(repo_root, thread_id)
    snapshot = review_snapshot(repo_root, detected_repo_name, pr_result["number"])

    record = upsert_tracked_pr(
        {
            "key": tracked_pr_key(detected_repo_name, pr_result["number"]),
            "repo_root": repo_root,
            "repo_owner": owner,
            "repo_name": detected_repo_name,
            "pr_number": pr_result["number"],
            "pr_url": pr_result["url"],
            "pr_title": pr_result["title"],
            "pr_state": pr_result["state"],
            "branch": pr_result["headRefName"],
            "base_branch": pr_result.get("baseRefName"),
            "worktree_path": worktree_result["worktree"],
            "thread_id": thread["id"],
            "thread_title": thread.get("title"),
            "status": snapshot["status"],
            "active": 1,
            "last_review_signature": snapshot["signature"],
            "last_review_status": snapshot["status"],
            "last_review_comment_at": snapshot["latest_comment_at"],
            "last_polled_at": now_ms(),
            "last_prompted_at": None,
            "last_run_status": "registered",
            "last_run_summary": "PR handoff completed and tracking registered",
            "last_error": None,
        }
    )

    return {
        "status": "ready",
        "branch": branch_result,
        "commit": commit_result,
        "push": push_result,
        "pull_request": pr_result,
        "worktree": worktree_result,
        "thread": thread,
        "tracked_pr": tracked_pr_to_dict(record),
    }


def tracked_pr_to_dict(record: TrackedPR) -> dict[str, Any]:
    return {
        "key": record.key,
        "repo_root": record.repo_root,
        "repo_name": record.repo_name,
        "pr_number": record.pr_number,
        "pr_url": record.pr_url,
        "pr_title": record.pr_title,
        "pr_state": record.pr_state,
        "branch": record.branch,
        "base_branch": record.base_branch,
        "worktree_path": record.worktree_path,
        "thread_id": record.thread_id,
        "thread_title": record.thread_title,
        "status": record.status,
        "active": bool(record.active),
        "last_review_status": record.last_review_status,
        "last_review_comment_at": record.last_review_comment_at,
        "last_polled_at": record.last_polled_at,
        "last_prompted_at": record.last_prompted_at,
        "last_run_status": record.last_run_status,
        "last_run_summary": record.last_run_summary,
        "last_error": record.last_error,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


def poll_record(record: TrackedPR, dry_run: bool) -> dict[str, Any]:
    snapshot = review_snapshot(record.repo_root, record.repo_name, record.pr_number)
    base_changes = {
        "pr_state": snapshot["pr"]["state"],
        "pr_title": snapshot["pr"]["title"],
        "pr_url": snapshot["pr"]["url"],
        "last_review_signature": snapshot["signature"],
        "last_review_status": snapshot["status"],
        "last_review_comment_at": snapshot["latest_comment_at"],
        "last_polled_at": now_ms(),
        "status": snapshot["status"],
        "last_error": None,
    }

    if snapshot["pr"]["state"] != "OPEN":
        cleanup_result = {"status": "skipped", "removed": False}
        worktree_path = Path(record.worktree_path)
        if worktree_path.exists():
            cleanup_result = remove_worktree(record.repo_root, worktree_path)
        updated = update_tracked_pr(
            record.key,
            **base_changes,
            active=0,
            last_run_status="closed",
            last_run_summary=f"PR is {snapshot['pr']['state']}; tracking disabled",
        )
        return {
            "status": "closed",
            "tracked_pr": tracked_pr_to_dict(updated),
            "review": snapshot,
            "cleanup": cleanup_result,
        }

    if snapshot["status"] == "awaiting_final_test":
        updated = update_tracked_pr(
            record.key,
            **base_changes,
            last_run_status="idle",
            last_run_summary="No unresolved review threads",
        )
        return {
            "status": "idle",
            "tracked_pr": tracked_pr_to_dict(updated),
            "review": snapshot,
            "triggered": False,
        }

    if (
        record.last_review_signature == snapshot["signature"]
        and record.last_prompted_at
        and record.last_run_status in {"ok", "dry_run"}
    ):
        updated = update_tracked_pr(
            record.key,
            **base_changes,
            last_run_status="idle",
            last_run_summary="No new review activity since the last follow-up run",
        )
        return {
            "status": "idle",
            "tracked_pr": tracked_pr_to_dict(updated),
            "review": snapshot,
            "triggered": False,
        }

    worktree = ensure_worktree(record.repo_root, record.repo_name, record.pr_number, record.branch, str(Path(record.worktree_path).parent.parent))
    sync_result = sync_worktree_to_remote(record.repo_root, record.branch, worktree["worktree"])
    codex_result = run_codex_resume(record, snapshot, dry_run)
    updated = update_tracked_pr(
        record.key,
        **base_changes,
        status="needs_review",
        last_prompted_at=now_ms(),
        last_run_status=codex_result["status"],
        last_run_summary=(codex_result.get("last_message") or codex_result.get("stderr") or codex_result.get("stdout") or "")[:4000],
        last_error=None if codex_result["status"] in {"ok", "dry_run"} else (codex_result.get("stderr") or "codex resume failed"),
    )
    return {
        "status": codex_result["status"],
        "tracked_pr": tracked_pr_to_dict(updated),
        "review": snapshot,
        "worktree": worktree,
        "sync": sync_result,
        "codex": codex_result,
        "triggered": True,
    }


def poll_all(active_only: bool, dry_run: bool) -> dict[str, Any]:
    records = list_tracked_prs(active_only=active_only)
    results = []
    for record in records:
        if active_only and not record.active:
            continue
        try:
            results.append(poll_record(record, dry_run=dry_run))
        except Exception as exc:  # noqa: BLE001
            updated = update_tracked_pr(record.key, last_error=str(exc), last_run_status="error", last_run_summary=str(exc))
            results.append({"status": "error", "tracked_pr": tracked_pr_to_dict(updated), "error": str(exc)})
    return {"status": "ready", "results": results, "count": len(results)}


def format_timestamp(value_ms: int | None) -> str:
    if not value_ms:
        return ""
    return datetime.fromtimestamp(value_ms / 1000, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def html_page(title: str, body: str) -> bytes:
    markup = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f3efe7;
      --ink: #1f2933;
      --muted: #5b6870;
      --line: #d5cec3;
      --card: #fffaf2;
      --accent: #0f766e;
      --warn: #b45309;
      --bad: #b42318;
    }}
    body {{ margin: 0; font: 14px/1.45 Menlo, Monaco, monospace; background: linear-gradient(180deg, #efe7da 0%, #f8f3eb 100%); color: var(--ink); }}
    header {{ padding: 24px 28px 12px; }}
    h1 {{ margin: 0; font-size: 24px; }}
    p {{ margin: 6px 0 0; color: var(--muted); }}
    main {{ padding: 0 28px 28px; }}
    .actions {{ display: flex; gap: 12px; margin: 16px 0 20px; }}
    button {{ border: 1px solid var(--line); background: var(--card); padding: 8px 12px; border-radius: 8px; cursor: pointer; font: inherit; }}
    table {{ width: 100%; border-collapse: collapse; background: var(--card); border: 1px solid var(--line); border-radius: 12px; overflow: hidden; }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid var(--line); vertical-align: top; text-align: left; }}
    th {{ background: #ebe3d4; font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; }}
    tr:last-child td {{ border-bottom: none; }}
    .pill {{ display: inline-block; padding: 2px 8px; border-radius: 999px; background: #d7edea; color: var(--accent); }}
    .warn {{ background: #fce7c3; color: var(--warn); }}
    .bad {{ background: #f7d8d5; color: var(--bad); }}
    code {{ font: inherit; }}
    form {{ display: inline; }}
    .small {{ color: var(--muted); font-size: 12px; }}
  </style>
</head>
<body>
{body}
</body>
</html>
"""
    return markup.encode("utf-8")


def status_badge(status: str | None) -> str:
    value = html.escape(status or "unknown")
    cls = "pill"
    if status in {"needs_review"}:
        cls = "pill warn"
    elif status in {"error", "closed"}:
        cls = "pill bad"
    return f'<span class="{cls}">{value}</span>'


class DashboardHandler(BaseHTTPRequestHandler):
    poll_lock = threading.Lock()

    def _send_html(self, content: bytes, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _redirect(self, location: str = "/") -> None:
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", location)
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        records = list_tracked_prs(active_only=False)
        rows = []
        for record in records:
            rows.append(
                f"""
                <tr>
                  <td>{status_badge(record.status)}</td>
                  <td><a href="{html.escape(record.pr_url)}">{html.escape(record.repo_name)} #{record.pr_number}</a><div class="small">{html.escape(record.pr_title)}</div></td>
                  <td><code>{html.escape(record.branch)}</code><div class="small">{html.escape(record.thread_id)}</div></td>
                  <td><code>{html.escape(record.worktree_path)}</code></td>
                  <td>{html.escape(record.last_run_status or "")}<div class="small">{html.escape(record.last_run_summary or "")}</div></td>
                  <td>{html.escape(format_timestamp(record.last_polled_at))}</td>
                  <td>
                    <form method="post" action="/poll-one?key={html.escape(record.key)}"><button>Run now</button></form>
                    <form method="post" action="/untrack?key={html.escape(record.key)}"><button>Untrack</button></form>
                  </td>
                </tr>
                """
            )
        body = f"""
        <header>
          <h1>PR Review Coordinator</h1>
          <p>Tracks active PRs and resumes the mapped Codex thread when GitHub review activity changes.</p>
        </header>
        <main>
          <div class="actions">
            <form method="post" action="/poll"><button>Poll all now</button></form>
          </div>
          <table>
            <thead>
              <tr>
                <th>Status</th>
                <th>PR</th>
                <th>Branch / Thread</th>
                <th>Worktree</th>
                <th>Last run</th>
                <th>Last poll</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {''.join(rows) or '<tr><td colspan="7">No tracked PRs</td></tr>'}
            </tbody>
          </table>
        </main>
        """
        self._send_html(html_page("PR Review Coordinator", body))

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        if parsed.path == "/poll":
            with self.poll_lock:
                poll_all(active_only=True, dry_run=False)
            self._redirect("/")
            return
        if parsed.path == "/poll-one":
            key = params.get("key", [None])[0]
            if key:
                with self.poll_lock:
                    poll_record(get_tracked_pr(key), dry_run=False)
            self._redirect("/")
            return
        if parsed.path == "/untrack":
            key = params.get("key", [None])[0]
            if key:
                update_tracked_pr(key, active=0, status="untracked", last_run_status="paused", last_run_summary="Tracking disabled from dashboard")
            self._redirect("/")
            return
        self._send_html(html_page("Not found", "<main><p>Unknown action</p></main>"), status=404)


def run_server(host: str, port: int, poll_seconds: int) -> None:
    def poll_loop() -> None:
        while True:
            try:
                with DashboardHandler.poll_lock:
                    poll_all(active_only=True, dry_run=False)
            except Exception:
                pass
            time.sleep(poll_seconds)

    thread = threading.Thread(target=poll_loop, daemon=True, name="pr-review-poller")
    thread.start()
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(json.dumps({"status": "ready", "url": f"http://{host}:{port}", "poll_seconds": poll_seconds}))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def emit(payload: dict[str, Any], output_format: str) -> None:
    if output_format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    for key, value in payload.items():
        if isinstance(value, (dict, list)):
            value = json.dumps(value, sort_keys=True)
        print(f"{key}={value}")


def parse_args() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    handoff = subparsers.add_parser("handoff", help="Create branch/commit/PR/worktree and register tracking.")
    handoff.add_argument("--repo-root", required=True)
    handoff.add_argument("--repo-name")
    handoff.add_argument("--branch", required=True)
    handoff.add_argument("--base-branch")
    handoff.add_argument("--commit-message", required=True)
    handoff.add_argument("--pr-title", required=True)
    handoff.add_argument("--pr-body", default="")
    handoff.add_argument("--draft", action="store_true")
    handoff.add_argument("--thread-id")
    handoff.add_argument("--worktree-root", default=str(CODEX_HOME / "worktrees" / "pr-review"))
    handoff.add_argument("--format", choices=("json", "text"), default="json")

    track = subparsers.add_parser("track", help="Register an existing PR against the current Codex thread.")
    track.add_argument("--repo-root", required=True)
    track.add_argument("--repo-name")
    track.add_argument("--pr", required=True, type=int)
    track.add_argument("--branch", required=True)
    track.add_argument("--thread-id")
    track.add_argument("--worktree-root", default=str(CODEX_HOME / "worktrees" / "pr-review"))
    track.add_argument("--format", choices=("json", "text"), default="json")

    poll = subparsers.add_parser("poll-once", help="Poll tracked PRs and resume threads when review changed.")
    poll.add_argument("--all", action="store_true", help="Include inactive records.")
    poll.add_argument("--dry-run", action="store_true")
    poll.add_argument("--format", choices=("json", "text"), default="json")

    status = subparsers.add_parser("status", help="List tracked PR state.")
    status.add_argument("--all", action="store_true", help="Include inactive records.")
    status.add_argument("--format", choices=("json", "text"), default="json")

    untrack = subparsers.add_parser("untrack", help="Disable tracking for one PR.")
    untrack.add_argument("--key", required=True)
    untrack.add_argument("--cleanup-worktree", action="store_true")
    untrack.add_argument("--format", choices=("json", "text"), default="json")

    serve = subparsers.add_parser("serve", help="Run a lightweight dashboard and background poller.")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--poll-seconds", type=int, default=DEFAULT_POLL_SECONDS)

    return parser


def main() -> None:
    args = parse_args().parse_args()
    if args.command == "handoff":
        payload = handoff_pr(
            repo_root=args.repo_root,
            repo_name=args.repo_name,
            branch=args.branch,
            base_branch=args.base_branch,
            commit_message=args.commit_message,
            pr_title=args.pr_title,
            pr_body=args.pr_body,
            draft=args.draft,
            worktree_root=args.worktree_root,
            thread_id=args.thread_id,
        )
        emit(payload, args.format)
        return

    if args.command == "track":
        payload = register_tracking(
            repo_root=args.repo_root,
            repo_name=args.repo_name,
            pr_number=args.pr,
            branch=args.branch,
            worktree_root=args.worktree_root,
            thread_id=args.thread_id,
        )
        emit(payload, args.format)
        return

    if args.command == "poll-once":
        payload = poll_all(active_only=not args.all, dry_run=args.dry_run)
        emit(payload, args.format)
        return

    if args.command == "status":
        payload = {
            "status": "ready",
            "tracked_prs": [tracked_pr_to_dict(record) for record in list_tracked_prs(active_only=not args.all)],
        }
        emit(payload, args.format)
        return

    if args.command == "untrack":
        record = get_tracked_pr(args.key)
        cleanup = None
        if args.cleanup_worktree:
            cleanup = remove_worktree(record.repo_root, record.worktree_path)
        updated = update_tracked_pr(
            args.key,
            active=0,
            status="untracked",
            last_run_status="paused",
            last_run_summary="Tracking disabled manually",
        )
        emit({"status": "ready", "tracked_pr": tracked_pr_to_dict(updated), "cleanup": cleanup}, args.format)
        return

    if args.command == "serve":
        run_server(args.host, args.port, args.poll_seconds)
        return

    raise ScriptError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    try:
        main()
    except ScriptError as exc:
        emit({"status": "blocked", "error": str(exc)}, "json")
        raise SystemExit(1) from exc
