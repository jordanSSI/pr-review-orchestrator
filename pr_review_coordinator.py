#!/usr/bin/env python3
"""Track active PRs against Codex threads and coordinate review follow-up."""

from __future__ import annotations

import argparse
import errno
import html
import json
import os
import sqlite3
import subprocess
import sys
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
    ensure_existing_worktree,
    ensure_worktree,
    git_status_is_clean,
    pull_request_snapshot,
    remove_worktree,
    repo_owner_and_name,
    resolve_codex_executable,
    resolve_provider_executable,
    run,
    slugify,
    sync_worktree_to_remote,
    verify_gh_auth,
)


PROJECT_DIR = Path(__file__).resolve().parent
VAR_DIR = PROJECT_DIR / "var"
LOCKS_DIR = VAR_DIR / "locks"
CODEX_STATE_DB = CODEX_HOME / "state_5.sqlite"
COORDINATOR_DB = VAR_DIR / "pr-review-coordinator.db"
DEFAULT_POLL_SECONDS = 300
DEFAULT_WORKER_COUNT = 4
ACTIVE_STATUSES = {"needs_review", "needs_ci_fix"}
PRIORITY_ORDER = {
    "needs_review": 0,
    "needs_ci_fix": 1,
    "pending_copilot_review": 2,
    "awaiting_final_test": 3,
    "busy": 4,
    "running": 5,
    "idle": 6,
    "untracked": 7,
    "closed": 8,
    "error": 9,
}


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
    worktree_managed INTEGER NOT NULL DEFAULT 1,
    thread_id TEXT NOT NULL,
    thread_title TEXT,
    status TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    last_review_signature TEXT,
    last_handled_signature TEXT,
    last_review_status TEXT,
    last_review_comment_at TEXT,
    pending_copilot_review INTEGER NOT NULL DEFAULT 0,
    unresolved_thread_count INTEGER NOT NULL DEFAULT 0,
    actionable_comment_count INTEGER NOT NULL DEFAULT 0,
    failing_check_count INTEGER NOT NULL DEFAULT 0,
    unresolved_threads_json TEXT,
    actionable_comments_json TEXT,
    failing_checks_json TEXT,
    ci_summary TEXT,
    run_state TEXT,
    run_reason TEXT,
    current_job_id INTEGER,
    lock_started_at INTEGER,
    lock_owner_pid INTEGER,
    last_polled_at INTEGER,
    last_prompted_at INTEGER,
    last_run_started_at INTEGER,
    last_run_finished_at INTEGER,
    last_run_status TEXT,
    last_run_summary TEXT,
    last_error TEXT,
    provider TEXT NOT NULL DEFAULT 'codex',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS tracked_prs_repo_pr
    ON tracked_prs(repo_root, pr_number);
CREATE INDEX IF NOT EXISTS tracked_prs_active_status
    ON tracked_prs(active, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    tracked_pr_key TEXT,
    status TEXT NOT NULL,
    requested_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER,
    requested_by TEXT,
    payload_json TEXT,
    result_summary TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS jobs_status_requested_at
    ON jobs(status, requested_at);

CREATE TABLE IF NOT EXISTS run_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at INTEGER NOT NULL,
    level TEXT NOT NULL,
    event_type TEXT NOT NULL,
    tracked_pr_key TEXT,
    message TEXT NOT NULL,
    details_json TEXT
);
CREATE INDEX IF NOT EXISTS run_events_created_at
    ON run_events(created_at DESC);
CREATE INDEX IF NOT EXISTS run_events_pr_created_at
    ON run_events(tracked_pr_key, created_at DESC);
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
    worktree_managed: int
    thread_id: str
    thread_title: str | None
    status: str
    active: int
    last_review_signature: str | None
    last_handled_signature: str | None
    last_review_status: str | None
    last_review_comment_at: str | None
    pending_copilot_review: int
    unresolved_thread_count: int
    actionable_comment_count: int
    failing_check_count: int
    unresolved_threads_json: str | None
    actionable_comments_json: str | None
    failing_checks_json: str | None
    ci_summary: str | None
    run_state: str | None
    run_reason: str | None
    current_job_id: int | None
    lock_started_at: int | None
    lock_owner_pid: int | None
    last_polled_at: int | None
    last_prompted_at: int | None
    last_run_started_at: int | None
    last_run_finished_at: int | None
    last_run_status: str | None
    last_run_summary: str | None
    last_error: str | None
    provider: str
    created_at: int
    updated_at: int


@dataclass
class Job:
    id: int
    action: str
    tracked_pr_key: str | None
    status: str
    requested_at: int
    started_at: int | None
    finished_at: int | None
    requested_by: str | None
    payload_json: str | None
    result_summary: str | None
    error: str | None


def now_ms() -> int:
    return int(time.time() * 1000)


def format_timestamp(value_ms: int | None) -> str:
    if not value_ms:
        return ""
    return datetime.fromtimestamp(value_ms / 1000, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


def connect_db() -> sqlite3.Connection:
    VAR_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(COORDINATOR_DB)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA)
    ensure_columns(
        connection,
        "tracked_prs",
        {
            "worktree_managed": "INTEGER NOT NULL DEFAULT 1",
            "pending_copilot_review": "INTEGER NOT NULL DEFAULT 0",
            "last_handled_signature": "TEXT",
            "unresolved_thread_count": "INTEGER NOT NULL DEFAULT 0",
            "actionable_comment_count": "INTEGER NOT NULL DEFAULT 0",
            "failing_check_count": "INTEGER NOT NULL DEFAULT 0",
            "unresolved_threads_json": "TEXT",
            "actionable_comments_json": "TEXT",
            "failing_checks_json": "TEXT",
            "ci_summary": "TEXT",
            "run_state": "TEXT",
            "run_reason": "TEXT",
            "current_job_id": "INTEGER",
            "lock_started_at": "INTEGER",
            "lock_owner_pid": "INTEGER",
            "last_run_started_at": "INTEGER",
            "last_run_finished_at": "INTEGER",
            "provider": "TEXT NOT NULL DEFAULT 'codex'",
        },
    )
    connection.commit()
    return connection


def ensure_columns(connection: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {
        row["name"]
        for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
    }
    for name, ddl in columns.items():
        if name not in existing:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def row_to_tracked_pr(row: sqlite3.Row) -> TrackedPR:
    return TrackedPR(**dict(row))


def row_to_job(row: sqlite3.Row) -> Job:
    return Job(**dict(row))


def tracked_pr_key(repo_name: str, pr_number: int) -> str:
    return f"{slugify(repo_name)}-pr-{pr_number}"


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
        "worktree_managed": bool(record.worktree_managed),
        "thread_id": record.thread_id,
        "thread_title": record.thread_title,
        "status": record.status,
        "active": bool(record.active),
        "pending_copilot_review": bool(record.pending_copilot_review),
        "last_handled_signature": record.last_handled_signature,
        "unresolved_thread_count": record.unresolved_thread_count,
        "actionable_comment_count": record.actionable_comment_count,
        "failing_check_count": record.failing_check_count,
        "ci_summary": record.ci_summary,
        "run_state": record.run_state,
        "run_reason": record.run_reason,
        "current_job_id": record.current_job_id,
        "last_review_status": record.last_review_status,
        "last_review_comment_at": record.last_review_comment_at,
        "last_polled_at": record.last_polled_at,
        "last_prompted_at": record.last_prompted_at,
        "last_run_started_at": record.last_run_started_at,
        "last_run_finished_at": record.last_run_finished_at,
        "last_run_status": record.last_run_status,
        "last_run_summary": record.last_run_summary,
        "last_error": record.last_error,
        "provider": record.provider,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


def log_line(level: str, event_type: str, message: str, **details: Any) -> None:
    payload = {
        "ts": datetime.now(tz=timezone.utc).isoformat(),
        "level": level,
        "event": event_type,
        "message": message,
    }
    for key, value in details.items():
        if value is not None:
            payload[key] = value
    text = json.dumps(payload, sort_keys=True)
    stream = sys.stderr if level in {"error", "warn"} else sys.stdout
    print(text, file=stream, flush=True)


def insert_run_event(level: str, event_type: str, message: str, *, tracked_pr_key: str | None = None, details: dict[str, Any] | None = None) -> None:
    connection = connect_db()
    try:
        connection.execute(
            """
            INSERT INTO run_events (created_at, level, event_type, tracked_pr_key, message, details_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (now_ms(), level, event_type, tracked_pr_key, message, json_dumps(details) if details is not None else None),
        )
        connection.commit()
    finally:
        connection.close()


def record_event(level: str, event_type: str, message: str, *, tracked_pr_key: str | None = None, details: dict[str, Any] | None = None) -> None:
    log_line(level, event_type, message, tracked_pr_key=tracked_pr_key, details=details)
    insert_run_event(level, event_type, message, tracked_pr_key=tracked_pr_key, details=details)


def lock_path(key: str) -> Path:
    return LOCKS_DIR / f"{key}.json"


def pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError as exc:
        return exc.errno == errno.EPERM
    return True


def read_lock(key: str) -> dict[str, Any] | None:
    path = lock_path(key)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        path.unlink(missing_ok=True)
        return None
    pid = data.get("pid")
    if not isinstance(pid, int) or not pid_is_alive(pid):
        path.unlink(missing_ok=True)
        return None
    return data


def acquire_lock(record: TrackedPR, job_id: int | None) -> dict[str, Any] | None:
    LOCKS_DIR.mkdir(parents=True, exist_ok=True)
    existing = read_lock(record.key)
    if existing:
        return existing
    payload = {
        "pid": os.getpid(),
        "key": record.key,
        "thread_id": record.thread_id,
        "worktree_path": record.worktree_path,
        "job_id": job_id,
        "started_at": now_ms(),
    }
    lock_path(record.key).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    update_tracked_pr(
        record.key,
        lock_started_at=payload["started_at"],
        lock_owner_pid=payload["pid"],
        run_state="running",
        current_job_id=job_id,
    )
    return None


def release_lock(key: str) -> None:
    lock_path(key).unlink(missing_ok=True)
    try:
        update_tracked_pr(key, lock_started_at=None, lock_owner_pid=None, current_job_id=None, run_state=None)
    except ScriptError:
        return


def cleanup_stale_runtime_state() -> None:
    for record in list_tracked_prs(active_only=False):
        if record.lock_owner_pid and pid_is_alive(record.lock_owner_pid):
            continue
        if record.lock_owner_pid or record.lock_started_at or record.run_state == "running":
            update_tracked_pr(
                record.key,
                lock_started_at=None,
                lock_owner_pid=None,
                current_job_id=None,
                run_state=None,
                run_reason=None,
            )
    connection = connect_db()
    try:
        connection.execute(
            """
            UPDATE jobs
            SET status = 'failed',
                finished_at = ?,
                error = COALESCE(error, 'Interrupted before completion'),
                result_summary = COALESCE(result_summary, 'Interrupted before completion')
            WHERE status = 'running'
            """,
            (now_ms(),),
        )
        connection.commit()
    finally:
        connection.close()


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


def resolve_thread(repo_root: str, explicit_thread_id: str | None, provider: str = "codex") -> dict[str, Any]:
    normalized_provider = (provider or "codex").strip().lower()
    if normalized_provider == "cursor":
        synthetic_id = explicit_thread_id or f"cursor-{repo_root}-{int(time.time() * 1000)}"
        return {"id": synthetic_id, "cwd": repo_root, "title": "Cursor", "archived": 0, "git_branch": None, "git_origin_url": None}

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
        result = run(
            ["gh", "repo", "view", "--json", "defaultBranchRef", "-q", ".defaultBranchRef.name"],
            cwd=repo_root,
        )
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


def create_or_reuse_pr(repo_root: str, branch: str, base_branch: str, title: str, body: str, draft: bool) -> dict[str, Any]:
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


def switch_repo_to_base_branch(repo_root: str, base_branch: str, feature_branch: str) -> dict[str, Any]:
    current = current_branch(repo_root)
    if current != feature_branch:
        return {"status": "ready", "switched": False, "branch": current}
    run(["git", "-C", repo_root, "fetch", "origin", base_branch])
    local_exists = bool(run(["git", "-C", repo_root, "branch", "--list", base_branch]).stdout.strip())
    if local_exists:
        run(["git", "-C", repo_root, "switch", base_branch])
    else:
        run(["git", "-C", repo_root, "switch", "-C", base_branch, f"origin/{base_branch}"])
    return {"status": "ready", "switched": True, "branch": base_branch}


def summarize_failing_checks(failing_checks: list[dict[str, Any]]) -> str:
    if not failing_checks:
        return ""
    return "; ".join(item.get("summary") or item.get("name") or "Unknown failing check" for item in failing_checks[:5])


def state_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": snapshot["status"],
        "pr_state": snapshot["pr"]["state"],
        "pr_title": snapshot["pr"]["title"],
        "pr_url": snapshot["pr"]["url"],
        "last_review_signature": snapshot["signature"],
        "last_review_status": snapshot["status"],
        "last_review_comment_at": snapshot["latest_comment_at"],
        "pending_copilot_review": 1 if snapshot["pending_copilot_review"] else 0,
        "unresolved_thread_count": len(snapshot["unresolved_threads"]),
        "actionable_comment_count": len(snapshot.get("actionable_pr_comments", [])),
        "failing_check_count": len(snapshot["failing_checks"]),
        "unresolved_threads_json": json_dumps(snapshot["unresolved_threads"]),
        "actionable_comments_json": json_dumps(snapshot.get("actionable_pr_comments", [])),
        "failing_checks_json": json_dumps(snapshot["failing_checks"]),
        "ci_summary": summarize_failing_checks(snapshot["failing_checks"]) or None,
        "last_polled_at": now_ms(),
    }


def execution_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "last_handled_signature": snapshot["signature"],
        "last_prompted_at": now_ms(),
    }


def assert_thread_available(thread_id: str, key: str) -> None:
    connection = connect_db()
    try:
        row = connection.execute(
            "SELECT key, repo_name, pr_number FROM tracked_prs WHERE active = 1 AND thread_id = ? AND key != ? LIMIT 1",
            (thread_id, key),
        ).fetchone()
    finally:
        connection.close()
    if row:
        raise ScriptError(
            f"thread {thread_id} is already attached to active PR {row['repo_name']} #{row['pr_number']}; active PRs must not share the same Codex thread"
        )


def upsert_tracked_pr(record: dict[str, Any]) -> TrackedPR:
    current_time = now_ms()
    payload = {**record, "updated_at": current_time}
    connection = connect_db()
    try:
        existing = connection.execute("SELECT created_at FROM tracked_prs WHERE key = ?", (payload["key"],)).fetchone()
        payload["created_at"] = existing["created_at"] if existing else current_time
        columns = sorted(payload.keys())
        placeholders = ", ".join(f":{column}" for column in columns)
        assignments = ", ".join(
            f"{column} = excluded.{column}" for column in columns if column not in {"key", "created_at"}
        )
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


def list_recent_events(limit: int = 30) -> list[dict[str, Any]]:
    connection = connect_db()
    try:
        rows = connection.execute(
            "SELECT * FROM run_events ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def list_recent_jobs(limit: int = 20) -> list[dict[str, Any]]:
    connection = connect_db()
    try:
        rows = connection.execute(
            "SELECT * FROM jobs ORDER BY requested_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def get_job(job_id: int) -> Job:
    connection = connect_db()
    try:
        row = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if not row:
            raise ScriptError(f"job not found: {job_id}")
        return row_to_job(row)
    finally:
        connection.close()


def enqueue_job(action: str, *, tracked_pr_key: str | None = None, requested_by: str = "cli", payload: dict[str, Any] | None = None) -> dict[str, Any]:
    if action not in {"poll-all", "poll-one", "run-one", "untrack", "untrack-cleanup"}:
        raise ScriptError(f"unsupported job action: {action}")
    if action in {"poll-one", "run-one", "untrack", "untrack-cleanup"} and not tracked_pr_key:
        raise ScriptError(f"job action {action!r} requires a tracked PR key")
    connection = connect_db()
    try:
        duplicate = connection.execute(
            """
            SELECT * FROM jobs
            WHERE action = ? AND COALESCE(tracked_pr_key, '') = COALESCE(?, '') AND status IN ('queued', 'running')
            ORDER BY id DESC
            LIMIT 1
            """,
            (action, tracked_pr_key),
        ).fetchone()
        if duplicate:
            job = row_to_job(duplicate)
            return {"status": "ready", "job": job.__dict__, "duplicate": True}
        requested_at = now_ms()
        cursor = connection.execute(
            """
            INSERT INTO jobs (action, tracked_pr_key, status, requested_at, requested_by, payload_json)
            VALUES (?, ?, 'queued', ?, ?, ?)
            """,
            (action, tracked_pr_key, requested_at, requested_by, json_dumps(payload) if payload is not None else None),
        )
        connection.commit()
        job = get_job(cursor.lastrowid)
        record_event("info", "job_enqueued", f"Queued job {action}", tracked_pr_key=tracked_pr_key, details={"job_id": job.id})
        return {"status": "ready", "job": job.__dict__, "duplicate": False}
    finally:
        connection.close()


def job_priority(action: str) -> int:
    if action in {"untrack", "untrack-cleanup"}:
        return 0
    if action in {"poll-one", "poll-all"}:
        return 1
    if action == "run-one":
        return 2
    return 99


def claim_next_job() -> Job | None:
    connection = connect_db()
    try:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            """
            SELECT *
            FROM jobs
            WHERE status = 'queued'
            ORDER BY
                CASE action
                    WHEN 'untrack' THEN 0
                    WHEN 'untrack-cleanup' THEN 0
                    WHEN 'poll-one' THEN 1
                    WHEN 'poll-all' THEN 1
                    WHEN 'run-one' THEN 2
                    ELSE 99
                END ASC,
                requested_at ASC,
                id ASC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            connection.commit()
            return None
        job = row_to_job(row)
        updated = connection.execute(
            "UPDATE jobs SET status = 'running', started_at = ? WHERE id = ? AND status = 'queued'",
            (now_ms(), job.id),
        )
        connection.commit()
        if updated.rowcount != 1:
            return None
        return get_job(job.id)
    finally:
        connection.close()


def decode_job_payload(job: Job) -> dict[str, Any]:
    if not job.payload_json:
        return {}
    try:
        data = json.loads(job.payload_json)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def list_pending_jobs() -> list[Job]:
    connection = connect_db()
    try:
        rows = connection.execute(
            """
            SELECT *
            FROM jobs
            WHERE status IN ('queued', 'running')
            ORDER BY
                CASE action
                    WHEN 'untrack' THEN 0
                    WHEN 'untrack-cleanup' THEN 0
                    WHEN 'poll-one' THEN 1
                    WHEN 'poll-all' THEN 1
                    WHEN 'run-one' THEN 2
                    ELSE 99
                END ASC,
                requested_at ASC,
                id ASC
            """
        ).fetchall()
        return [row_to_job(row) for row in rows]
    finally:
        connection.close()


def pending_jobs_by_pr() -> dict[str, list[Job]]:
    jobs_by_pr: dict[str, list[Job]] = {}
    for job in list_pending_jobs():
        if not job.tracked_pr_key:
            continue
        jobs_by_pr.setdefault(job.tracked_pr_key, []).append(job)
    return jobs_by_pr


def finish_job(job_id: int, status: str, summary: str, *, error: str | None = None) -> Job:
    connection = connect_db()
    try:
        connection.execute(
            """
            UPDATE jobs
            SET status = ?, finished_at = ?, result_summary = ?, error = ?
            WHERE id = ?
            """,
            (status, now_ms(), summary[:4000], error[:4000] if error else None, job_id),
        )
        connection.commit()
        return get_job(job_id)
    finally:
        connection.close()


def refresh_record_state(record: TrackedPR, snapshot: dict[str, Any], *, run_status: str | None = None, run_summary: str | None = None, error: str | None = None, run_reason: str | None = None, prompted: bool = False, finished: bool = False, job_id: int | None = None) -> TrackedPR:
    changes = state_payload(snapshot)
    if run_status is not None:
        changes["last_run_status"] = run_status
    if run_summary is not None:
        changes["last_run_summary"] = run_summary[:4000]
    changes["last_error"] = error[:4000] if error else None
    if run_reason is not None:
        changes["run_reason"] = run_reason
    if prompted:
        changes["last_prompted_at"] = now_ms()
    if finished:
        changes["last_run_finished_at"] = now_ms()
        changes["run_state"] = None
        changes["current_job_id"] = None
    elif job_id is not None:
        changes["current_job_id"] = job_id
    return update_tracked_pr(record.key, **changes)


def update_execution_state(
    key: str,
    *,
    run_state: str | None = None,
    run_reason: str | None = None,
    current_job_id: int | None = None,
    last_run_status: str | None = None,
    last_run_summary: str | None = None,
    last_run_started_at: int | None = None,
    last_run_finished_at: int | None = None,
    last_error: str | None = None,
    last_handled_signature: str | None = None,
    last_prompted_at: int | None = None,
) -> TrackedPR:
    changes: dict[str, Any] = {}
    if run_state is not None:
        changes["run_state"] = run_state
    if run_reason is not None:
        changes["run_reason"] = run_reason
    if current_job_id is not None:
        changes["current_job_id"] = current_job_id
    if last_run_status is not None:
        changes["last_run_status"] = last_run_status
    if last_run_summary is not None:
        changes["last_run_summary"] = last_run_summary[:4000]
    if last_run_started_at is not None:
        changes["last_run_started_at"] = last_run_started_at
    if last_run_finished_at is not None:
        changes["last_run_finished_at"] = last_run_finished_at
    if last_error is not None or last_error is None:
        changes["last_error"] = last_error[:4000] if last_error else None
    if last_handled_signature is not None:
        changes["last_handled_signature"] = last_handled_signature
    if last_prompted_at is not None:
        changes["last_prompted_at"] = last_prompted_at
    return update_tracked_pr(key, **changes)


def register_tracking(
    *,
    repo_root: str,
    repo_name: str | None,
    pr_number: int,
    branch: str,
    worktree_root: str,
    worktree_path: str | None,
    thread_id: str | None,
    worktree_layout: str,
    provider: str = "codex",
) -> dict[str, Any]:
    verify_gh_auth()
    owner, detected_repo_name = ensure_repo_name(repo_root, repo_name)
    pr = run(
        ["gh", "pr", "view", str(pr_number), "--json", "number,url,title,headRefName,baseRefName,state"],
        cwd=repo_root,
    )
    pr_info = json.loads(pr.stdout)
    thread = resolve_thread(repo_root, thread_id, provider=provider)
    key = tracked_pr_key(detected_repo_name, pr_number)
    assert_thread_available(thread["id"], key)
    if worktree_path:
        worktree = ensure_existing_worktree(repo_root, detected_repo_name, branch, worktree_path)
        worktree_managed = 0
    else:
        worktree = ensure_worktree(
            repo_root,
            detected_repo_name,
            pr_number,
            branch,
            worktree_root,
            layout=worktree_layout,
        )
        worktree_managed = 1
    snapshot = pull_request_snapshot(repo_root, detected_repo_name, pr_number)
    record = upsert_tracked_pr(
        {
            "key": key,
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
            "worktree_managed": worktree_managed,
            "thread_id": thread["id"],
            "thread_title": thread.get("title"),
            "active": 1,
            "run_state": None,
            "run_reason": None,
            "current_job_id": None,
            "lock_started_at": None,
            "lock_owner_pid": None,
            "last_handled_signature": None,
            "last_prompted_at": None,
            "last_run_started_at": None,
            "last_run_finished_at": now_ms(),
            "last_run_status": "registered",
            "last_run_summary": "PR tracking registered",
            "last_error": None,
            "provider": (provider or "codex").strip().lower(),
            **state_payload(snapshot),
        }
    )
    record_event("info", "tracking_registered", f"Registered tracking for PR #{pr_number}", tracked_pr_key=record.key)
    return {
        "status": "ready",
        "tracked_pr": tracked_pr_to_dict(record),
        "review": snapshot,
        "worktree": worktree,
        "thread": thread,
    }


def handoff_pr(*, repo_root: str, repo_name: str | None, branch: str, base_branch: str | None, commit_message: str, pr_title: str, pr_body: str, draft: bool, worktree_root: str, worktree_path: str | None, thread_id: str | None, worktree_layout: str, provider: str = "codex") -> dict[str, Any]:
    verify_gh_auth()
    owner, detected_repo_name = ensure_repo_name(repo_root, repo_name)
    base = base_branch or repo_default_branch(repo_root)
    branch_result = ensure_branch(repo_root, branch)
    commit_result = commit_all_changes(repo_root, commit_message)
    push_result = push_branch(repo_root, branch)
    pr_result = create_or_reuse_pr(repo_root, branch, base, pr_title, pr_body, draft)
    repo_reset = switch_repo_to_base_branch(repo_root, base, branch)
    thread = resolve_thread(repo_root, thread_id, provider=provider)
    key = tracked_pr_key(detected_repo_name, pr_result["number"])
    assert_thread_available(thread["id"], key)
    if worktree_path:
        worktree_result = ensure_existing_worktree(repo_root, detected_repo_name, branch, worktree_path)
        worktree_managed = 0
    else:
        worktree_result = ensure_worktree(
            repo_root,
            detected_repo_name,
            pr_result["number"],
            branch,
            worktree_root,
            layout=worktree_layout,
        )
        worktree_managed = 1
    snapshot = pull_request_snapshot(repo_root, detected_repo_name, pr_result["number"])
    record = upsert_tracked_pr(
        {
            "key": key,
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
            "worktree_managed": worktree_managed,
            "thread_id": thread["id"],
            "thread_title": thread.get("title"),
            "active": 1,
            "run_state": None,
            "run_reason": None,
            "current_job_id": None,
            "lock_started_at": None,
            "lock_owner_pid": None,
            "last_handled_signature": None,
            "last_prompted_at": None,
            "last_run_started_at": None,
            "last_run_finished_at": now_ms(),
            "last_run_status": "registered",
            "last_run_summary": "PR handoff completed and tracking registered",
            "last_error": None,
            "provider": (provider or "codex").strip().lower(),
            **state_payload(snapshot),
        }
    )
    record_event("info", "handoff_complete", f"Handoff completed for PR #{record.pr_number}", tracked_pr_key=record.key)
    return {
        "status": "ready",
        "branch": branch_result,
        "commit": commit_result,
        "push": push_result,
        "pull_request": pr_result,
        "repo_reset": repo_reset,
        "worktree": worktree_result,
        "thread": thread,
        "tracked_pr": tracked_pr_to_dict(record),
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


def summarize_pr_comments(actionable_comments: list[dict[str, Any]]) -> str:
    if not actionable_comments:
        return "No actionable top-level PR comments remain."
    lines: list[str] = []
    for comment in actionable_comments[:12]:
        body = (comment.get("body") or "").replace("\r", " ").replace("\n", " ").strip()
        if len(body) > 240:
            body = body[:237] + "..."
        author = comment.get("author") or "unknown"
        lines.append(f"- {comment.get('id') or '<unknown>'} [{author}] {body}")
    if len(actionable_comments) > 12:
        lines.append(f"- ... {len(actionable_comments) - 12} more actionable PR comments")
    return "\n".join(lines)


def summarize_ci_failures(failing_checks: list[dict[str, Any]]) -> str:
    if not failing_checks:
        return "No completed failing CI checks remain."
    lines: list[str] = []
    for check in failing_checks[:12]:
        summary = check.get("summary") or check.get("name") or "Unknown failing check"
        description = check.get("description") or ""
        text = summary if not description else f"{summary}: {description}"
        if len(text) > 240:
            text = text[:237] + "..."
        lines.append(f"- {text}")
    if len(failing_checks) > 12:
        lines.append(f"- ... {len(failing_checks) - 12} more failing checks")
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

        Work only against the dedicated PR worktree for code changes. Do not use the main checkout for edits.
        Address GitHub review feedback, completed failing CI checks, or both, with minimal targeted fixes.
        Pull the latest PR branch state into that worktree before making changes.
        Run relevant validation for the touched files, including repo typecheck if available.
        Commit and push scoped follow-up changes when needed. You must commit and push any code changes before finishing; do not leave the worktree with uncommitted changes.
        Request reviewer `chatgpt-codex-connector` after every push when further review is needed (or `copilot-pull-request-reviewer` if the repository still uses that flow).
        Resolve review threads only after fixes are pushed, or leave a clear rationale when no code change is needed.
        If you address a top-level PR comment, reply on the PR after the push and include `<!-- pr-review-coordinator:handled-comment COMMENT_ID -->` for each handled comment ID so the coordinator can treat it as addressed.
        When review feedback is clear and CI is green, return to idle tracking for final testing.

        Current unresolved review threads:
        {summarize_threads(snapshot["unresolved_threads"])}

        Current actionable top-level PR comments:
        {summarize_pr_comments(snapshot.get("actionable_pr_comments", []))}

        Current completed failing CI checks/statuses:
        {summarize_ci_failures(snapshot["failing_checks"])}

        If no code changes are required after inspection, say so clearly in your final summary.
        """
    ).strip()


def run_codex_resume(record: TrackedPR, snapshot: dict[str, Any], dry_run: bool) -> dict[str, Any]:
    codex_bin = resolve_codex_executable()
    prompt = resume_prompt(record, snapshot)
    if dry_run:
        return {"status": "dry_run", "thread_id": record.thread_id, "prompt_preview": prompt}
    with tempfile.NamedTemporaryFile(prefix="codex-pr-followup-", suffix=".txt", delete=False) as output_file:
        output_path = output_file.name
    try:
        result = subprocess.run(
            [
                codex_bin,
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


def run_cursor_resume(record: TrackedPR, snapshot: dict[str, Any], dry_run: bool) -> dict[str, Any]:
    agent_bin = resolve_provider_executable("cursor")
    prompt = resume_prompt(record, snapshot)
    if dry_run:
        return {"status": "dry_run", "prompt_preview": prompt}
    result = subprocess.run(
        [agent_bin, "--trust", "--yolo", "-p", prompt, "--output-format", "text"],
        cwd=record.worktree_path,
        check=False,
        capture_output=True,
        text=True,
    )
    last_message = (result.stdout or result.stderr or "").strip()[:4000]
    return {
        "status": "ok" if result.returncode == 0 else "error",
        "exit_code": result.returncode,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
        "last_message": last_message,
    }


def run_agent_resume(record: TrackedPR, snapshot: dict[str, Any], dry_run: bool) -> dict[str, Any]:
    provider = (record.provider or "codex").strip().lower()
    if provider == "cursor":
        return run_cursor_resume(record, snapshot, dry_run)
    return run_codex_resume(record, snapshot, dry_run)


def priority_key(record: TrackedPR) -> tuple[int, str, int]:
    return (PRIORITY_ORDER.get(record.status, 99), record.repo_name.lower(), record.pr_number)


def should_trigger_follow_up(record: TrackedPR, snapshot: dict[str, Any], *, force_run: bool) -> tuple[bool, str]:
    if snapshot["status"] not in ACTIVE_STATUSES:
        return False, "PR is not currently actionable"
    if force_run:
        return True, "Manual run requested"
    if (
        record.last_handled_signature == snapshot["signature"]
        and record.last_prompted_at
        and record.last_run_status in {"ok", "dry_run"}
    ):
        return False, "No new actionable review or CI changes since the last follow-up run"
    return True, "Actionable review or CI state changed"


def maybe_cleanup_closed_pr(record: TrackedPR) -> dict[str, Any]:
    if not Path(record.worktree_path).exists():
        return {"status": "skipped", "removed": False, "reason": "worktree missing"}
    if not git_status_is_clean(record.worktree_path):
        return {"status": "skipped", "removed": False, "reason": "worktree dirty"}
    try:
        cleanup = remove_worktree(record.repo_root, record.worktree_path)
        return {"status": "ready", **cleanup}
    except ScriptError as exc:
        return {"status": "skipped", "removed": False, "reason": str(exc)}


def handle_untrack(record: TrackedPR, cleanup_worktree: bool) -> dict[str, Any]:
    cleanup: dict[str, Any] | None = None
    summary = "Tracking disabled manually"
    if cleanup_worktree:
        if record.worktree_managed:
            cleanup = maybe_cleanup_closed_pr(record) if record.pr_state != "OPEN" else maybe_cleanup_managed_active(record)
        else:
            cleanup = cleanup_external_worktree(record)
        if cleanup.get("removed"):
            summary = "Tracking disabled and worktree removed"
        else:
            summary = f"Tracking disabled; cleanup skipped: {cleanup.get('reason', 'not eligible')}"
    updated = update_tracked_pr(
        record.key,
        active=0,
        status="untracked",
        run_state=None,
        run_reason=None,
        current_job_id=None,
        lock_started_at=None,
        lock_owner_pid=None,
        last_run_finished_at=now_ms(),
        last_run_status="paused",
        last_run_summary=summary,
        last_error=None,
    )
    record_event("info", "untracked", summary, tracked_pr_key=record.key, details=cleanup)
    return {"status": "ready", "tracked_pr": tracked_pr_to_dict(updated), "cleanup": cleanup}


def maybe_cleanup_managed_active(record: TrackedPR) -> dict[str, Any]:
    if not Path(record.worktree_path).exists():
        return {"status": "skipped", "removed": False, "reason": "worktree missing"}
    if not git_status_is_clean(record.worktree_path):
        return {"status": "skipped", "removed": False, "reason": "worktree dirty"}
    try:
        cleanup = remove_worktree(record.repo_root, record.worktree_path)
        return {"status": "ready", **cleanup}
    except ScriptError as exc:
        return {"status": "skipped", "removed": False, "reason": str(exc)}


def cleanup_external_worktree(record: TrackedPR) -> dict[str, Any]:
    if record.pr_state == "OPEN":
        return {"status": "skipped", "removed": False, "reason": "external worktrees are only removed after the PR is merged or closed"}
    path = Path(record.worktree_path)
    if not path.exists():
        return {"status": "skipped", "removed": False, "reason": "worktree missing"}
    if not git_status_is_clean(path):
        return {"status": "skipped", "removed": False, "reason": "external worktree is dirty"}
    try:
        cleanup = remove_worktree(record.repo_root, path)
        return {"status": "ready", **cleanup}
    except ScriptError as exc:
        return {"status": "skipped", "removed": False, "reason": f"git refused removal: {exc}"}


def poll_record(record: TrackedPR, *, dry_run: bool, force_run: bool, job_id: int | None) -> dict[str, Any]:
    record_event("info", "poll_started", f"Polling PR #{record.pr_number}", tracked_pr_key=record.key, details={"job_id": job_id})
    update_tracked_pr(
        record.key,
        last_run_status="running",
        last_run_summary="Fetching latest GitHub PR state",
        last_error=None,
    )
    snapshot = pull_request_snapshot(record.repo_root, record.repo_name, record.pr_number)
    previous_status = record.status
    if previous_status != snapshot["status"]:
        record_event(
            "info",
            "state_transition",
            f"PR state changed from {previous_status} to {snapshot['status']}",
            tracked_pr_key=record.key,
            details={"from": previous_status, "to": snapshot["status"]},
        )

    if snapshot["pr"]["state"] != "OPEN":
        cleanup = maybe_cleanup_closed_pr(record) if record.worktree_managed else {"status": "skipped", "removed": False, "reason": "external worktree retained until explicit cleanup"}
        updated = update_tracked_pr(
            record.key,
            active=0,
            last_run_finished_at=now_ms(),
            last_run_status="closed",
            last_run_summary=f"PR is {snapshot['pr']['state']}; tracking archived",
            last_error=None,
            **state_payload(snapshot),
        )
        record_event("info", "pr_closed", f"Archived tracking for {snapshot['pr']['state']} PR", tracked_pr_key=record.key, details=cleanup)
        return {"status": "closed", "tracked_pr": tracked_pr_to_dict(updated), "review": snapshot, "cleanup": cleanup}

    should_run, reason = should_trigger_follow_up(record, snapshot, force_run=force_run)
    if not should_run:
        updated = update_tracked_pr(
            record.key,
            last_run_finished_at=now_ms(),
            last_run_status="idle",
            last_run_summary=reason,
            last_error=None,
            **state_payload(snapshot),
        )
        record_event("info", "poll_idle", reason, tracked_pr_key=record.key)
        return {"status": "idle", "tracked_pr": tracked_pr_to_dict(updated), "review": snapshot, "triggered": False}

    if dry_run:
        updated = update_tracked_pr(
            record.key,
            last_run_finished_at=now_ms(),
            last_run_status="dry_run",
            last_run_summary="Would queue follow-up execution",
            last_error=None,
            **state_payload(snapshot),
        )
        return {"status": "dry_run", "tracked_pr": tracked_pr_to_dict(updated), "review": snapshot, "triggered": True}

    enqueue_result = enqueue_job(
        "run-one",
        tracked_pr_key=record.key,
        requested_by="poller",
        payload={"force_run": force_run, "signature": snapshot["signature"]},
    )
    job = enqueue_result["job"]
    summary = "Follow-up run already queued" if enqueue_result["duplicate"] else f"Queued follow-up job #{job['id']}"
    updated = update_tracked_pr(
        record.key,
        last_run_finished_at=now_ms(),
        last_run_status="queued",
        last_run_summary=summary,
        last_error=None,
        **state_payload(snapshot),
    )
    record_event("info", "follow_up_queued", summary, tracked_pr_key=record.key, details={"job_id": job["id"], "duplicate": enqueue_result["duplicate"]})
    return {"status": "queued", "tracked_pr": tracked_pr_to_dict(updated), "review": snapshot, "triggered": True, "job": job}


def run_follow_up(record: TrackedPR, *, dry_run: bool, force_run: bool, job_id: int) -> dict[str, Any]:
    existing_lock = acquire_lock(record, job_id)
    if existing_lock:
        started_at = format_timestamp(existing_lock.get("started_at"))
        summary = "Another orchestrator run is active for this PR"
        if started_at:
            summary += f" since {started_at}"
        updated = update_tracked_pr(
            record.key,
            current_job_id=None,
            last_run_finished_at=now_ms(),
            last_run_status="busy",
            last_run_summary=summary,
            last_error=None,
        )
        record_event("info", "busy", summary, tracked_pr_key=record.key)
        return {"status": "busy", "tracked_pr": tracked_pr_to_dict(updated), "triggered": False}

    try:
        snapshot = pull_request_snapshot(record.repo_root, record.repo_name, record.pr_number)
        if snapshot["pr"]["state"] != "OPEN":
            cleanup = maybe_cleanup_closed_pr(record) if record.worktree_managed else {"status": "skipped", "removed": False, "reason": "external worktree retained until explicit cleanup"}
            updated = update_tracked_pr(
                record.key,
                active=0,
                run_state=None,
                run_reason=None,
                current_job_id=None,
                last_run_finished_at=now_ms(),
                last_run_status="closed",
                last_run_summary=f"PR is {snapshot['pr']['state']}; tracking archived",
                last_error=None,
                **state_payload(snapshot),
            )
            return {"status": "closed", "tracked_pr": tracked_pr_to_dict(updated), "review": snapshot, "cleanup": cleanup}

        should_run, reason = should_trigger_follow_up(record, snapshot, force_run=force_run)
        if not should_run:
            updated = refresh_record_state(
                record,
                snapshot,
                run_status="idle",
                run_summary=reason,
                error=None,
                run_reason=None,
                finished=True,
            )
            record_event("info", "run_skipped", reason, tracked_pr_key=record.key, details={"job_id": job_id})
            return {"status": "idle", "tracked_pr": tracked_pr_to_dict(updated), "review": snapshot, "triggered": False}

        worktree_path = Path(record.worktree_path)
        if worktree_path.exists() and not git_status_is_clean(worktree_path):
            summary = f"Worktree has local changes; treating this PR as busy: {record.worktree_path}"
            updated = refresh_record_state(
                record,
                snapshot,
                run_status="busy",
                run_summary=summary,
                error=None,
                run_reason=None,
                finished=True,
            )
            record_event("info", "worktree_busy", summary, tracked_pr_key=record.key)
            return {"status": "busy", "tracked_pr": tracked_pr_to_dict(updated), "review": snapshot, "triggered": False}

        if record.worktree_managed:
            refresh_record_state(record, snapshot, run_status="running", run_summary="Ensuring managed PR worktree is ready", run_reason="prepare", job_id=job_id)
            worktree = ensure_worktree(
                record.repo_root,
                record.repo_name,
                record.pr_number,
                record.branch,
                str(Path(record.worktree_path).parent.parent),
            )
        else:
            refresh_record_state(record, snapshot, run_status="running", run_summary="Validating tracked PR worktree", run_reason="prepare", job_id=job_id)
            worktree = ensure_existing_worktree(record.repo_root, record.repo_name, record.branch, record.worktree_path)

        refresh_record_state(record, snapshot, run_status="running", run_summary="Syncing worktree to latest remote branch state", run_reason="sync", job_id=job_id)
        sync_result = sync_worktree_to_remote(record.repo_root, record.branch, worktree["worktree"])
        provider = (record.provider or "codex").strip().lower()
        refresh_record_state(record, snapshot, run_status="running", run_summary=f"Resuming {provider} agent", run_reason=provider, job_id=job_id)
        record_event("info", "agent_resume", f"Launching {provider} follow-up", tracked_pr_key=record.key, details={"job_id": job_id, "dry_run": dry_run, "provider": provider})
        agent_result = run_agent_resume(record, snapshot, dry_run)
        updated = refresh_record_state(
            record,
            snapshot,
            run_status=agent_result["status"],
            run_summary=(agent_result.get("last_message") or agent_result.get("stderr") or agent_result.get("stdout") or reason)[:4000],
            error=None if agent_result["status"] in {"ok", "dry_run"} else (agent_result.get("stderr") or "agent resume failed"),
            run_reason=None,
            finished=True,
        )
        if agent_result["status"] in {"ok", "dry_run"}:
            updated = update_tracked_pr(record.key, **execution_payload(snapshot))
        level = "info" if agent_result["status"] in {"ok", "dry_run"} else "error"
        record_event(level, "agent_finished", f"Agent follow-up finished with status {agent_result['status']}", tracked_pr_key=record.key, details={"job_id": job_id, "provider": provider})
        return {
            "status": agent_result["status"],
            "tracked_pr": tracked_pr_to_dict(updated),
            "review": snapshot,
            "worktree": worktree,
            "sync": sync_result,
            "agent": agent_result,
            "triggered": True,
        }
    finally:
        release_lock(record.key)


def poll_all(active_only: bool, dry_run: bool) -> dict[str, Any]:
    records = list_tracked_prs(active_only=active_only)
    records.sort(key=priority_key)
    results = []
    for record in records:
        results.append(enqueue_job("poll-one", tracked_pr_key=record.key, requested_by="poll-all"))
    return {"status": "ready", "results": results, "count": len(results)}


def process_job(job: Job, *, dry_run: bool = False) -> dict[str, Any]:
    record_event("info", "job_started", f"Started job {job.action}", tracked_pr_key=job.tracked_pr_key, details={"job_id": job.id})
    try:
        if job.action == "poll-all":
            records = list_tracked_prs(active_only=True)
            records.sort(key=priority_key)
            results = []
            for record in records:
                results.append(enqueue_job("poll-one", tracked_pr_key=record.key, requested_by="poll-all"))
            summary = f"Queued {len(results)} PR poll job(s)"
            finish_job(job.id, "succeeded", summary)
            record_event("info", "job_finished", f"Finished job {job.action}", details={"job_id": job.id, "count": len(results)})
            return {"status": "ready", "results": results}

        record = get_tracked_pr(job.tracked_pr_key or "")
        payload = decode_job_payload(job)
        if job.action == "poll-one":
            result = poll_record(record, dry_run=dry_run, force_run=bool(payload.get("force_run")), job_id=job.id)
        elif job.action == "run-one":
            result = run_follow_up(record, dry_run=dry_run, force_run=bool(payload.get("force_run")), job_id=job.id)
        elif job.action == "untrack":
            result = handle_untrack(record, cleanup_worktree=False)
        elif job.action == "untrack-cleanup":
            result = handle_untrack(record, cleanup_worktree=True)
        else:
            raise ScriptError(f"unsupported job action: {job.action}")
        finish_job(job.id, "succeeded", result.get("status", "ready"))
        record_event("info", "job_finished", f"Finished job {job.action}", tracked_pr_key=job.tracked_pr_key, details={"job_id": job.id})
        return result
    except Exception as exc:  # noqa: BLE001
        finish_job(job.id, "failed", str(exc), error=str(exc))
        if job.tracked_pr_key:
            try:
                update_tracked_pr(
                    job.tracked_pr_key,
                    run_state=None,
                    run_reason=None,
                    current_job_id=None,
                    last_run_finished_at=now_ms(),
                    last_run_status="error",
                    last_run_summary=str(exc),
                    last_error=str(exc),
                )
            except ScriptError:
                pass
        record_event("error", "job_failed", str(exc), tracked_pr_key=job.tracked_pr_key, details={"job_id": job.id})
        raise


def run_daemon(host: str, port: int, poll_seconds: int) -> None:
    cleanup_stale_runtime_state()
    worker_count = max(2, int(os.environ.get("PR_REVIEW_COORDINATOR_WORKERS", str(DEFAULT_WORKER_COUNT))))
    record_event("info", "daemon_started", "Daemon started", details={"host": host, "port": port, "poll_seconds": poll_seconds, "worker_count": worker_count})
    stop_event = threading.Event()

    def worker_loop(worker_id: int) -> None:
        while not stop_event.is_set():
            job = claim_next_job()
            if job:
                try:
                    process_job(job)
                except Exception:
                    pass
                continue
            time.sleep(0.2)

    workers = [
        threading.Thread(target=worker_loop, name=f"pr-review-worker-{index + 1}", args=(index + 1,), daemon=True)
        for index in range(worker_count)
    ]
    for worker in workers:
        worker.start()

    next_poll_at = time.monotonic()
    try:
        while True:
            if time.monotonic() >= next_poll_at:
                enqueue_job("poll-all", requested_by="daemon")
                next_poll_at = time.monotonic() + poll_seconds
                continue
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()
        for worker in workers:
            worker.join(timeout=1)
        record_event("info", "daemon_stopped", "Daemon stopped")


def status_badge(status: str | None) -> str:
    value = html.escape(status or "unknown")
    cls = "pill"
    if status in {"needs_review", "needs_ci_fix", "pending_copilot_review", "running", "queued", "busy"}:
        cls = "pill warn"
    elif status in {"error", "closed"}:
        cls = "pill bad"
    return f'<span class="{cls}">{value}</span>'


def describe_pending_jobs(record: TrackedPR, jobs: list[Job]) -> str:
    if not jobs:
        return ""
    top = min(jobs, key=lambda job: (job_priority(job.action), job.requested_at, job.id))
    verb = "running" if top.status == "running" else "queued"
    label = top.action.replace("-", " ")
    suffix = "" if len(jobs) == 1 else f" (+{len(jobs) - 1} more)"
    return f"{label} {verb}{suffix}"


def render_record_row(record: TrackedPR, pending_jobs: list[Job] | None = None) -> str:
    pending_jobs = pending_jobs or []
    details = []
    if record.unresolved_thread_count:
        details.append(f"{record.unresolved_thread_count} review thread(s)")
    if record.actionable_comment_count:
        details.append(f"{record.actionable_comment_count} PR comment(s)")
    if record.failing_check_count:
        details.append(f"{record.failing_check_count} failing check(s)")
    if record.ci_summary:
        details.append(record.ci_summary)
    pending_text = describe_pending_jobs(record, pending_jobs)
    if pending_text:
        details.append(f"pending: {pending_text}")
    detail_text = " | ".join(details)
    actions_disabled = "disabled" if pending_jobs else ""
    return f"""
        <tr data-pr-key="{html.escape(record.key)}">
          <td>{status_badge(record.status)}</td>
          <td><a href="{html.escape(record.pr_url)}">{html.escape(record.repo_name)} #{record.pr_number}</a><div class="small">{html.escape(record.pr_title)}</div></td>
          <td><code>{html.escape(record.branch)}</code><div class="small">{html.escape(record.thread_id)}</div></td>
          <td><code>{html.escape(record.provider or "codex")}</code></td>
          <td><code>{html.escape(record.worktree_path)}</code><div class="small" data-role="detail-text">{html.escape(detail_text)}</div></td>
          <td>{status_badge(record.run_state or record.last_run_status)}<div class="small stack" data-role="run-summary">{html.escape(record.last_run_summary or "")}</div></td>
          <td>{html.escape(format_timestamp(record.last_polled_at))}</td>
          <td>
            <form method="post" action="/run-one?key={html.escape(record.key)}" onsubmit="return queueAction(this, 'run now queued')"><button {actions_disabled}>Run now</button></form>
            <form method="post" action="/poll-one?key={html.escape(record.key)}" onsubmit="return queueAction(this, 'poll queued')"><button {actions_disabled}>Poll</button></form>
            <form method="post" action="/untrack?key={html.escape(record.key)}" onsubmit="return queueAction(this, 'untrack queued')"><button {actions_disabled}>Untrack</button></form>
            <form method="post" action="/untrack-cleanup?key={html.escape(record.key)}" onsubmit="return queueAction(this, 'untrack cleanup queued')"><button {actions_disabled}>Untrack + Cleanup</button></form>
          </td>
        </tr>
        """


def sort_records(records: list[TrackedPR], sort_key: str) -> list[TrackedPR]:
    if sort_key == "status":
        return sorted(records, key=priority_key)
    if sort_key == "pr":
        return sorted(records, key=lambda record: (record.repo_name.lower(), record.pr_number))
    if sort_key == "last_poll":
        return sorted(records, key=lambda record: (record.last_polled_at or 0, record.repo_name.lower(), record.pr_number), reverse=True)
    return sorted(records, key=lambda record: record.updated_at, reverse=True)


class DashboardHandler(BaseHTTPRequestHandler):
    def _send_html(self, content: bytes, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        try:
            self.wfile.write(content)
        except (BrokenPipeError, ConnectionResetError):
            return

    def _redirect(self, location: str = "/") -> None:
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        try:
            self.end_headers()
        except (BrokenPipeError, ConnectionResetError):
            return

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/favicon.ico":
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        params = parse_qs(parsed.query)
        scope = params.get("scope", ["active"])[0]
        status_filter = params.get("status", ["all"])[0]
        sort_key = params.get("sort", ["updated"])[0]

        records = list_tracked_prs(active_only=False)
        if scope == "active":
            records = [record for record in records if record.active]
        elif scope == "archived":
            records = [record for record in records if not record.active]
        if status_filter != "all":
            records = [record for record in records if record.status == status_filter]
        pending_jobs = pending_jobs_by_pr()
        rows = [render_record_row(record, pending_jobs.get(record.key, [])) for record in sort_records(records, sort_key)]
        jobs = list_recent_jobs(15)
        events = list_recent_events(20)

        body = f"""
        <header>
          <h1>PR Review Coordinator</h1>
          <p>Web UI reads tracked state and enqueues daemon jobs. Refreshes every 5 seconds.</p>
        </header>
        <script>
          function queueAction(form, label) {{
            const row = form.closest('tr');
            if (!row) return true;
            for (const button of row.querySelectorAll('button')) {{
              button.disabled = true;
            }}
            const summary = row.querySelector('[data-role="run-summary"]');
            if (summary) {{
              summary.textContent = label;
            }}
            const detail = row.querySelector('[data-role="detail-text"]');
            if (detail && !detail.textContent.includes(label)) {{
              detail.textContent = detail.textContent ? `${{detail.textContent}} | pending: ${{label}}` : `pending: ${{label}}`;
            }}
            return true;
          }}
        </script>
        <main>
          <div class="actions">
            <form method="post" action="/poll"><button>Poll all now</button></form>
          </div>
          <form method="get" class="filters">
            <label>Scope
              <select name="scope">
                <option value="active" {"selected" if scope == "active" else ""}>Active</option>
                <option value="archived" {"selected" if scope == "archived" else ""}>Archived</option>
                <option value="all" {"selected" if scope == "all" else ""}>All</option>
              </select>
            </label>
            <label>Status
              <select name="status">
                <option value="all" {"selected" if status_filter == "all" else ""}>All</option>
                {"".join(f'<option value="{name}" {"selected" if status_filter == name else ""}>{name}</option>' for name in ["needs_review", "needs_ci_fix", "pending_copilot_review", "awaiting_final_test", "queued", "busy", "error", "untracked"])}
              </select>
            </label>
            <label>Sort
              <select name="sort">
                <option value="updated" {"selected" if sort_key == "updated" else ""}>Updated</option>
                <option value="status" {"selected" if sort_key == "status" else ""}>Status</option>
                <option value="pr" {"selected" if sort_key == "pr" else ""}>PR number/repo</option>
                <option value="last_poll" {"selected" if sort_key == "last_poll" else ""}>Last poll</option>
              </select>
            </label>
            <button type="submit">Apply</button>
          </form>
          <table>
            <thead>
              <tr>
                <th>Status</th>
                <th>PR</th>
                <th>Branch / Thread</th>
                <th>Provider</th>
                <th>Worktree / CI</th>
                <th>Run state</th>
                <th>Last poll</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {''.join(rows) or '<tr><td colspan="8">No matching tracked PRs</td></tr>'}
            </tbody>
          </table>
          <h2>Recent Jobs</h2>
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Action</th>
                <th>Status</th>
                <th>Requested</th>
                <th>Finished</th>
                <th>Summary</th>
              </tr>
            </thead>
            <tbody>
              {''.join(render_job_row(job) for job in jobs) or '<tr><td colspan="6">No jobs yet</td></tr>'}
            </tbody>
          </table>
          <h2>Recent Events</h2>
          <table>
            <thead>
              <tr>
                <th>When</th>
                <th>Level</th>
                <th>Event</th>
                <th>PR</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>
              {''.join(render_event_row(event) for event in events) or '<tr><td colspan="5">No events yet</td></tr>'}
            </tbody>
          </table>
        </main>
        """
        self._send_html(html_page("PR Review Coordinator", body))

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        if parsed.path == "/poll":
            enqueue_job("poll-all", requested_by="web")
            self._redirect("/")
            return
        if parsed.path == "/poll-one":
            key = params.get("key", [None])[0]
            if key:
                enqueue_job("poll-one", tracked_pr_key=key, requested_by="web")
            self._redirect("/")
            return
        if parsed.path == "/run-one":
            key = params.get("key", [None])[0]
            if key:
                enqueue_job("run-one", tracked_pr_key=key, requested_by="web", payload={"force_run": True})
            self._redirect("/")
            return
        if parsed.path == "/untrack":
            key = params.get("key", [None])[0]
            if key:
                enqueue_job("untrack", tracked_pr_key=key, requested_by="web")
            self._redirect("/")
            return
        if parsed.path == "/untrack-cleanup":
            key = params.get("key", [None])[0]
            if key:
                enqueue_job("untrack-cleanup", tracked_pr_key=key, requested_by="web")
            self._redirect("/")
            return
        self._send_html(html_page("Not found", "<main><p>Unknown action</p></main>"), status=404)


def render_job_row(job: dict[str, Any]) -> str:
    return f"""
        <tr>
          <td>{job['id']}</td>
          <td>{html.escape(job['action'])}</td>
          <td>{status_badge(job.get('status'))}</td>
          <td>{html.escape(format_timestamp(job.get('requested_at')))}</td>
          <td>{html.escape(format_timestamp(job.get('finished_at')))}</td>
          <td class="stack">{html.escape(job.get('result_summary') or job.get('error') or '')}</td>
        </tr>
        """


def render_event_row(event: dict[str, Any]) -> str:
    pr_key = event.get("tracked_pr_key") or ""
    return f"""
        <tr>
          <td>{html.escape(format_timestamp(event.get('created_at')))}</td>
          <td>{status_badge(event.get('level'))}</td>
          <td>{html.escape(event.get('event_type') or '')}</td>
          <td><code>{html.escape(pr_key)}</code></td>
          <td class="stack">{html.escape(event.get('message') or '')}</td>
        </tr>
        """


def html_page(title: str, body: str) -> bytes:
    markup = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <meta http-equiv="refresh" content="5">
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
    h1, h2 {{ margin: 0 0 12px; }}
    p {{ margin: 6px 0 0; color: var(--muted); }}
    main {{ padding: 0 28px 28px; }}
    .actions, .filters {{ display: flex; gap: 12px; margin: 16px 0 20px; align-items: end; flex-wrap: wrap; }}
    button, select {{ border: 1px solid var(--line); background: var(--card); padding: 8px 12px; border-radius: 8px; cursor: pointer; font: inherit; }}
    table {{ width: 100%; border-collapse: collapse; background: var(--card); border: 1px solid var(--line); border-radius: 12px; overflow: hidden; margin-bottom: 18px; }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid var(--line); vertical-align: top; text-align: left; }}
    th {{ background: #ebe3d4; font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; }}
    tr:last-child td {{ border-bottom: none; }}
    .pill {{ display: inline-block; padding: 2px 8px; border-radius: 999px; background: #d7edea; color: var(--accent); }}
    .warn {{ background: #fce7c3; color: var(--warn); }}
    .bad {{ background: #f7d8d5; color: var(--bad); }}
    .small {{ color: var(--muted); font-size: 12px; }}
    .stack {{ white-space: pre-wrap; overflow-wrap: anywhere; }}
    code {{ font: inherit; }}
    form {{ display: inline; }}
    label {{ display: flex; flex-direction: column; gap: 6px; }}
  </style>
</head>
<body>
{body}
</body>
</html>
"""
    return markup.encode("utf-8")


def run_web(host: str, port: int) -> None:
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(json.dumps({"status": "ready", "url": f"http://{host}:{port}"}), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def run_serve(host: str, port: int, poll_seconds: int) -> None:
    daemon_thread = threading.Thread(target=run_daemon, args=(host, port, poll_seconds), daemon=True, name="pr-review-daemon")
    daemon_thread.start()
    run_web(host, port)


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
    handoff.add_argument("--provider", choices=("codex", "cursor"), default="codex", help="Agent provider for follow-up (default: codex).")
    handoff.add_argument("--worktree-root", default=str(CODEX_HOME / "worktrees" / "pr-review"))
    handoff.add_argument("--worktree-layout", choices=("nested", "sibling"), default="nested")
    handoff.add_argument("--worktree-path", help="Use an existing registered git worktree instead of creating one.")
    handoff.add_argument("--format", choices=("json", "text"), default="json")

    track = subparsers.add_parser("track", help="Register an existing PR against the current agent thread (codex) or a synthetic thread (cursor).")
    track.add_argument("--repo-root", required=True)
    track.add_argument("--repo-name")
    track.add_argument("--pr", required=True, type=int)
    track.add_argument("--branch", required=True)
    track.add_argument("--thread-id")
    track.add_argument("--provider", choices=("codex", "cursor"), default="codex", help="Agent provider for follow-up (default: codex).")
    track.add_argument("--worktree-root", default=str(CODEX_HOME / "worktrees" / "pr-review"))
    track.add_argument("--worktree-layout", choices=("nested", "sibling"), default="nested")
    track.add_argument("--worktree-path", help="Use an existing registered git worktree instead of creating one.")
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

    daemon = subparsers.add_parser("daemon", help="Run the polling and execution daemon.")
    daemon.add_argument("--host", default="127.0.0.1")
    daemon.add_argument("--port", type=int, default=8765)
    daemon.add_argument("--poll-seconds", type=int, default=DEFAULT_POLL_SECONDS)

    web = subparsers.add_parser("web", help="Run the read-only dashboard and enqueue actions into SQLite.")
    web.add_argument("--host", default="127.0.0.1")
    web.add_argument("--port", type=int, default=8765)

    serve = subparsers.add_parser("serve", help="Run daemon and web together for compatibility.")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--poll-seconds", type=int, default=DEFAULT_POLL_SECONDS)

    return parser


def main() -> None:
    args = parse_args().parse_args()
    if args.command == "handoff":
        emit(
            handoff_pr(
                repo_root=args.repo_root,
                repo_name=args.repo_name,
                branch=args.branch,
                base_branch=args.base_branch,
                commit_message=args.commit_message,
                pr_title=args.pr_title,
                pr_body=args.pr_body,
                draft=args.draft,
                worktree_root=args.worktree_root,
                worktree_path=args.worktree_path,
                thread_id=args.thread_id,
                worktree_layout=args.worktree_layout,
                provider=getattr(args, "provider", "codex"),
            ),
            args.format,
        )
        return

    if args.command == "track":
        emit(
            register_tracking(
                repo_root=args.repo_root,
                repo_name=args.repo_name,
                pr_number=args.pr,
                branch=args.branch,
                worktree_root=args.worktree_root,
                worktree_path=args.worktree_path,
                thread_id=args.thread_id,
                worktree_layout=args.worktree_layout,
                provider=getattr(args, "provider", "codex"),
            ),
            args.format,
        )
        return

    if args.command == "poll-once":
        emit(poll_all(active_only=not args.all, dry_run=args.dry_run), args.format)
        return

    if args.command == "status":
        emit(
            {
                "status": "ready",
                "tracked_prs": [tracked_pr_to_dict(record) for record in list_tracked_prs(active_only=not args.all)],
                "jobs": list_recent_jobs(10),
                "events": list_recent_events(10),
            },
            args.format,
        )
        return

    if args.command == "untrack":
        emit(handle_untrack(get_tracked_pr(args.key), cleanup_worktree=args.cleanup_worktree), args.format)
        return

    if args.command == "daemon":
        run_daemon(args.host, args.port, args.poll_seconds)
        return

    if args.command == "web":
        run_web(args.host, args.port)
        return

    if args.command == "serve":
        run_serve(args.host, args.port, args.poll_seconds)
        return

    raise ScriptError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    try:
        main()
    except ScriptError as exc:
        emit({"status": "blocked", "error": str(exc)}, "json")
        raise SystemExit(1) from exc
