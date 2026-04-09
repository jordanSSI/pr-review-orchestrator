#!/usr/bin/env python3
"""Shared helpers for reusable PR review tooling."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any


CODEX_HOME = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))).expanduser()
AUTOMATIONS_DIR = CODEX_HOME / "automations"
AUTOMATIONS_DB = CODEX_HOME / "sqlite" / "codex-dev.db"
DEFAULT_WORKTREE_ROOT = CODEX_HOME / "worktrees" / "pr-review"
DEFAULT_WORKTREE_LAYOUT = "nested"
PR_REVIEW_COORDINATOR_CONFIG = CODEX_HOME / "pr-review-coordinator.json"
DEFAULT_AGENT_NICKNAME = "jordanBot"
COPILOT_LOGINS = {
    "copilot-pull-request-reviewer[bot]",
    "github-copilot[bot]",
    "copilot[bot]",
    "chatgpt-codex-connector[bot]",
}
COPILOT_REVIEW_REQUEST_LOGIN = "copilot-pull-request-reviewer"
HANDLED_PR_COMMENT_MARKER = "pr-review-coordinator:handled-comment"
COPILOT_RETRYABLE_ERROR_SNIPPETS = (
    "copilot encountered an error and was unable to review this pull request",
    "try again by re-requesting a review",
)


class ScriptError(RuntimeError):
    """Raised for expected script failures."""


def load_pr_review_coordinator_config() -> dict[str, Any]:
    if not PR_REVIEW_COORDINATOR_CONFIG.exists():
        return {}
    try:
        return json.loads(PR_REVIEW_COORDINATOR_CONFIG.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def normalize_agent_comment_prefix(value: str | None) -> str:
    normalized = (value or "").strip()
    if not normalized:
        return ""
    if normalized.startswith("[") and normalized.endswith("]"):
        return normalized
    return f"[{normalized}]"


def resolve_agent_comment_prefix() -> str:
    env_prefix = normalize_agent_comment_prefix(os.environ.get("PR_REVIEW_COORDINATOR_AGENT_COMMENT_PREFIX"))
    if env_prefix:
        return env_prefix

    env_nickname = normalize_agent_comment_prefix(os.environ.get("PR_REVIEW_COORDINATOR_AGENT_NICKNAME"))
    if env_nickname:
        return env_nickname

    config = load_pr_review_coordinator_config()
    config_prefix = normalize_agent_comment_prefix(str(config.get("agent_comment_prefix") or ""))
    if config_prefix:
        return config_prefix

    config_nickname = normalize_agent_comment_prefix(str(config.get("agent_nickname") or ""))
    if config_nickname:
        return config_nickname

    return normalize_agent_comment_prefix(DEFAULT_AGENT_NICKNAME)


def agent_github_comment_instruction() -> str:
    prefix = resolve_agent_comment_prefix()
    return (
        f"Any GitHub comment or review reply you post must begin with `{prefix}`. "
        "This includes handled-comment replies and rationale-only replies."
    )


AGENT_COMMENT_PREFIX = resolve_agent_comment_prefix()
AGENT_GITHUB_COMMENT_INSTRUCTION = agent_github_comment_instruction()


def project_dir() -> Path:
    return Path(__file__).resolve().parent


def codex_skills_dir() -> Path:
    return CODEX_HOME / "skills"


def pr_review_executor_skill_path() -> Path:
    return codex_skills_dir() / "pr-review-executor" / "SKILL.md"


def resolve_codex_executable() -> str:
    override = os.environ.get("CODEX_BIN", "").strip()
    if override:
        candidate = Path(override).expanduser()
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
        raise ScriptError(f"CODEX_BIN is set but not executable: {override}")

    from_path = shutil.which("codex")
    if from_path:
        return from_path

    fallback_candidates = [
        Path("/Applications/Codex.app/Contents/Resources/codex"),
        Path.home() / ".local/bin/codex",
        Path("/usr/local/bin/codex"),
        Path("/opt/homebrew/bin/codex"),
    ]
    for candidate in fallback_candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)

    raise ScriptError(
        "unable to find `codex` executable. Set CODEX_BIN to an absolute executable path "
        "or add codex to PATH."
    )


def resolve_cursor_executable() -> str:
    """Resolve the standalone `agent` CLI (Cursor agent). Used when provider is cursor."""
    override = os.environ.get("AGENT_BIN", "").strip()
    if override:
        candidate = Path(override).expanduser()
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
        raise ScriptError(f"AGENT_BIN is set but not executable: {override}")

    from_path = shutil.which("agent")
    if from_path:
        return from_path

    fallback_candidates = [
        Path.home() / ".local/bin/agent",
        Path("/usr/local/bin/agent"),
        Path("/opt/homebrew/bin/agent"),
    ]
    for candidate in fallback_candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)

    raise ScriptError(
        "unable to find `agent` executable. Set AGENT_BIN to an absolute executable path "
        "or add agent to PATH (e.g. install via https://cursor.com/install)."
    )


def resolve_provider_executable(provider: str) -> str:
    """Resolve the agent executable for the given provider (e.g. 'codex' or 'cursor')."""
    normalized = (provider or "codex").strip().lower()
    if normalized == "codex":
        return resolve_codex_executable()
    if normalized == "cursor":
        return resolve_cursor_executable()
    raise ScriptError(f"unknown provider: {provider!r}. Use 'codex' or 'cursor'.")


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
        "--worktree-layout",
        choices=("nested", "sibling"),
        default=DEFAULT_WORKTREE_LAYOUT,
        help="Layout used when creating managed PR worktrees.",
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
    return normalized in COPILOT_LOGINS or "copilot" in normalized or "codex-connector" in normalized


def is_retryable_copilot_review_error(author_login: str | None, body: str | None) -> bool:
    if not is_copilot_login(author_login):
        return False
    normalized_body = " ".join((body or "").lower().split())
    if not normalized_body:
        return False
    return all(snippet in normalized_body for snippet in COPILOT_RETRYABLE_ERROR_SNIPPETS)


def fetch_review_threads(repo_root: str | Path, repo_name: str, pr_number: int) -> dict[str, Any]:
    return fetch_pull_request_state(repo_root, repo_name, pr_number)


def fetch_pull_request_state(repo_root: str | Path, repo_name: str, pr_number: int) -> dict[str, Any]:
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
              reviewRequests(first: 20) {
                nodes {
                  requestedReviewer {
                    __typename
                    ... on User {
                      login
                    }
                    ... on Bot {
                      login
                    }
                  }
                }
              }
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
              comments(first: 100) {
                nodes {
                  id
                  author {
                    login
                  }
                  body
                  createdAt
                  updatedAt
                  url
                }
              }
              reviews(first: 100) {
                nodes {
                  id
                  author {
                    login
                  }
                  body
                  state
                  submittedAt
                  url
                }
              }
              commits(last: 1) {
                nodes {
                  commit {
                    statusCheckRollup {
                      contexts(first: 100) {
                        nodes {
                          __typename
                          ... on CheckRun {
                            name
                            status
                            conclusion
                            detailsUrl
                          }
                          ... on StatusContext {
                            context
                            state
                            description
                            targetUrl
                          }
                        }
                      }
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


def extract_handled_pr_comment_ids(body: str | None) -> set[str]:
    if not body:
        return set()
    if not body.lstrip().startswith(resolve_agent_comment_prefix()):
        return set()
    matches = re.findall(rf"{re.escape(HANDLED_PR_COMMENT_MARKER)}\s+([A-Za-z0-9_=:-]+)", body)
    return {match.strip() for match in matches if match.strip()}


def is_linear_linkback_comment(author: str | None, body: str | None) -> bool:
    if author != "linear" or not body:
        return False
    return body.lstrip().startswith("<!-- linear-linkback -->")


def serialize_actionable_pr_comments(pull_request: dict[str, Any]) -> list[dict[str, Any]]:
    comments = pull_request.get("comments", {}).get("nodes") or []
    handled_ids: set[str] = set()
    comment_summaries: list[dict[str, Any]] = []
    for comment in comments:
        body = (comment.get("body") or "").strip()
        author = (comment.get("author") or {}).get("login")
        handled_ids.update(extract_handled_pr_comment_ids(body))
        comment_summaries.append(
            {
                "id": comment.get("id"),
                "author": author,
                "body": body,
                "createdAt": comment.get("createdAt"),
                "updatedAt": comment.get("updatedAt"),
                "url": comment.get("url"),
                "is_handler_comment": HANDLED_PR_COMMENT_MARKER in body,
                "is_linear_linkback": is_linear_linkback_comment(author, body),
            }
        )

    actionable = [
        {
            "id": comment["id"],
            "author": comment["author"],
            "body": comment["body"],
            "createdAt": comment["createdAt"],
            "updatedAt": comment["updatedAt"],
            "url": comment["url"],
        }
        for comment in comment_summaries
        if comment["id"]
        and comment["body"]
        and not comment["is_handler_comment"]
        and not comment["is_linear_linkback"]
        and not is_retryable_copilot_review_error(comment["author"], comment["body"])
        and comment["id"] not in handled_ids
    ]
    actionable.sort(key=lambda item: ((item["updatedAt"] or item["createdAt"] or ""), item["id"]))
    return actionable


def serialize_latest_copilot_activity(pull_request: dict[str, Any]) -> dict[str, Any] | None:
    activities: list[dict[str, Any]] = []

    for review in pull_request.get("reviews", {}).get("nodes") or []:
        author = (review.get("author") or {}).get("login")
        if not is_copilot_login(author):
            continue
        activities.append(
            {
                "source": "review",
                "id": review.get("id"),
                "author": author,
                "body": (review.get("body") or "").strip(),
                "state": review.get("state"),
                "createdAt": review.get("submittedAt"),
                "url": review.get("url"),
            }
        )

    for comment in pull_request.get("comments", {}).get("nodes") or []:
        author = (comment.get("author") or {}).get("login")
        if not is_copilot_login(author):
            continue
        activities.append(
            {
                "source": "comment",
                "id": comment.get("id"),
                "author": author,
                "body": (comment.get("body") or "").strip(),
                "state": None,
                "createdAt": comment.get("updatedAt") or comment.get("createdAt"),
                "url": comment.get("url"),
            }
        )

    if not activities:
        return None
    activities.sort(key=lambda item: ((item.get("createdAt") or ""), item.get("id") or ""))
    return activities[-1]


def serialize_retryable_copilot_review_error(pull_request: dict[str, Any]) -> dict[str, Any] | None:
    latest = serialize_latest_copilot_activity(pull_request)
    if not latest:
        return None
    if not is_retryable_copilot_review_error(latest.get("author"), latest.get("body")):
        return None
    return latest


def serialize_failing_checks(pull_request: dict[str, Any]) -> list[dict[str, Any]]:
    commits = pull_request.get("commits", {}).get("nodes") or []
    if not commits:
        return []
    rollup = (commits[-1].get("commit") or {}).get("statusCheckRollup") or {}
    contexts = rollup.get("contexts", {}).get("nodes") or []
    failing: list[dict[str, Any]] = []
    for node in contexts:
        node_type = node.get("__typename")
        if node_type == "CheckRun":
            status = (node.get("status") or "").upper()
            conclusion = (node.get("conclusion") or "").upper()
            if status != "COMPLETED":
                continue
            if conclusion in {"SUCCESS", "NEUTRAL", "SKIPPED"}:
                continue
            failing.append(
                {
                    "type": "check_run",
                    "name": node.get("name") or "Unnamed check",
                    "status": status,
                    "conclusion": conclusion or "UNKNOWN",
                    "url": node.get("detailsUrl"),
                    "summary": f"{node.get('name') or 'Unnamed check'} ({conclusion or 'UNKNOWN'})",
                }
            )
            continue
        if node_type == "StatusContext":
            state = (node.get("state") or "").upper()
            if state in {"SUCCESS", "EXPECTED", "PENDING"}:
                continue
            failing.append(
                {
                    "type": "status_context",
                    "name": node.get("context") or "Unnamed status",
                    "status": state,
                    "conclusion": state,
                    "url": node.get("targetUrl"),
                    "summary": f"{node.get('context') or 'Unnamed status'} ({state})",
                    "description": node.get("description"),
                }
            )
    failing.sort(key=lambda item: (item["type"], item["name"]))
    return failing


def pull_request_snapshot(repo_root: str | Path, repo_name: str, pr_number: int) -> dict[str, Any]:
    pull_request = fetch_pull_request_state(repo_root, repo_name, pr_number)
    unresolved = serialize_unresolved_threads(pull_request)
    actionable_pr_comments = serialize_actionable_pr_comments(pull_request)
    failing_checks = serialize_failing_checks(pull_request)
    copilot_review_error = serialize_retryable_copilot_review_error(pull_request)
    latest_comment_at = None
    latest_comment_candidates = [item["latest_comment_at"] or "" for item in unresolved]
    latest_comment_candidates.extend(item["updatedAt"] or item["createdAt"] or "" for item in actionable_pr_comments)
    if copilot_review_error and copilot_review_error.get("createdAt"):
        latest_comment_candidates.append(copilot_review_error["createdAt"])
    if latest_comment_candidates:
        latest_comment_at = max(latest_comment_candidates) or None
    review_requests = pull_request.get("reviewRequests", {}).get("nodes") or []
    pending_copilot_review = any(
        is_copilot_login((node.get("requestedReviewer") or {}).get("login"))
        for node in review_requests
    )
    if unresolved or actionable_pr_comments:
        status = "needs_review"
    elif failing_checks:
        status = "needs_ci_fix"
    elif pending_copilot_review:
        status = "pending_copilot_review"
    elif copilot_review_error:
        status = "copilot_review_cooldown"
    else:
        status = "awaiting_final_test"
    signature_payload = {
        "status": status,
        "unresolved_threads": unresolved,
        "actionable_pr_comments": actionable_pr_comments,
        "failing_checks": failing_checks,
        "pending_copilot_review": pending_copilot_review,
        "copilot_review_error": copilot_review_error,
    }
    signature = json.dumps(signature_payload, sort_keys=True)
    return {
        "pr": {
            "number": pull_request["number"],
            "url": pull_request["url"],
            "title": pull_request["title"],
            "state": pull_request["state"],
        },
        "status": status,
        "signature": signature,
        "latest_comment_at": latest_comment_at,
        "pending_copilot_review": pending_copilot_review,
        "copilot_review_error": copilot_review_error,
        "unresolved_threads": unresolved,
        "actionable_pr_comments": actionable_pr_comments,
        "failing_checks": failing_checks,
    }


def classify_threads(pull_request: dict[str, Any]) -> dict[str, Any]:
    threads = pull_request["reviewThreads"]["nodes"] or []
    copilot_threads: list[dict[str, Any]] = []
    unresolved_threads: list[dict[str, Any]] = []
    unresolved_copilot_threads: list[dict[str, Any]] = []

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
            unresolved_threads.append(summary)
            if is_copilot_thread:
                unresolved_copilot_threads.append(summary)

    status = "clean" if not unresolved_threads else "needs_work"
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
            "unresolved_threads": len(unresolved_threads),
            "unresolved_copilot_threads": len(unresolved_copilot_threads),
        },
        "unresolved_threads": unresolved_threads,
        "unresolved_copilot_threads": unresolved_copilot_threads,
    }


def validate_managed_worktree_root(repo_root: str | Path, worktree_root: str | Path) -> Path:
    repo = Path(repo_root).expanduser().resolve()
    root = Path(worktree_root).expanduser().resolve()
    if root == repo or repo in root.parents:
        raise ScriptError(
            f"managed PR worktrees must live outside the repository checkout: repo={repo} worktree_root={root}"
        )
    return root


def validate_worktree_target(repo_root: str | Path, worktree: str | Path) -> Path:
    repo = Path(repo_root).expanduser().resolve()
    target = Path(worktree).expanduser().resolve()
    if target == repo or repo in target.parents:
        raise ScriptError(
            f"managed PR worktrees must live outside the repository checkout: repo={repo} worktree={target}"
        )
    return target


def worktree_path(
    repo_name: str,
    pr_number: int,
    branch: str,
    worktree_root: str | Path,
    *,
    layout: str = DEFAULT_WORKTREE_LAYOUT,
) -> Path:
    root = Path(worktree_root).expanduser().resolve()
    if layout == "sibling":
        return root / f"{slugify(repo_name)}-pr-{pr_number}"
    return root / slugify(repo_name) / f"pr-{pr_number}-{slugify(branch)}"


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
    worktree_path = Path(worktree).resolve()
    result = run(
        ["git", "-C", str(worktree_path), "status", "--porcelain", "--untracked-files=normal"],
    )
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        if line.startswith("?? "):
            status_path = line[3:]
            if " -> " in status_path:
                status_path = status_path.split(" -> ", 1)[1]
            status_path = status_path.rstrip("/")
            if status_path == "node_modules" and (worktree_path / status_path).is_symlink():
                continue
        return False
    return True


def ensure_worktree_node_modules_symlink(worktree_path: str | Path, repo_root: str | Path) -> bool:
    """If the worktree has package.json but no node_modules, symlink node_modules from repo_root.
    Returns True if a symlink was created, False otherwise.
    """
    worktree = Path(worktree_path).resolve()
    repo = Path(repo_root).resolve()
    package_json = worktree / "package.json"
    worktree_node_modules = worktree / "node_modules"
    repo_node_modules = repo / "node_modules"
    if not package_json.is_file():
        return False
    if worktree_node_modules.exists() or worktree_node_modules.is_symlink():
        return False
    if not repo_node_modules.is_dir():
        return False
    try:
        worktree_node_modules.symlink_to(repo_node_modules)
        return True
    except OSError:
        return False


def ensure_worktree(
    repo_root: str | Path,
    repo_name: str,
    pr_number: int,
    branch: str,
    worktree_root: str | Path,
    *,
    layout: str = DEFAULT_WORKTREE_LAYOUT,
) -> dict[str, Any]:
    verify_repo_name(repo_root, repo_name)
    validated_root = validate_managed_worktree_root(repo_root, worktree_root)
    target = validate_worktree_target(
        repo_root,
        worktree_path(repo_name, pr_number, branch, validated_root, layout=layout),
    )
    target.parent.mkdir(parents=True, exist_ok=True)

    run(["git", "-C", str(repo_root), "fetch", "origin", branch])

    tracked = tracked_worktrees(repo_root)
    target_key = str(target)
    existing = tracked.get(target_key)
    if existing:
        if not git_status_is_clean(target):
            raise ScriptError(f"existing worktree is dirty: {target}")
        ensure_worktree_node_modules_symlink(target, repo_root)
        return {"status": "ready", "worktree": str(target), "created": False}

    if target.exists() and not any(target.iterdir()):
        target.rmdir()
    elif target.exists():
        raise ScriptError(f"target worktree path exists but is not a registered git worktree: {target}")

    other_path = branch_checked_out_elsewhere(repo_root, branch)
    if other_path:
        raise ScriptError(
            f"branch {branch!r} is already checked out in another worktree: {other_path}. "
            f"Switch the canonical checkout off that branch or reuse the existing checkout with --worktree-path."
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
    ensure_worktree_node_modules_symlink(target, repo_root)
    return {"status": "ready", "worktree": str(target), "created": True}


def ensure_existing_worktree(repo_root: str | Path, repo_name: str, branch: str, worktree_path: str | Path) -> dict[str, Any]:
    verify_repo_name(repo_root, repo_name)
    target = validate_worktree_target(repo_root, worktree_path)
    tracked = tracked_worktrees(repo_root)
    existing = tracked.get(str(target))

    if not existing:
        try:
            checkout_root = Path(
                run(["git", "-C", str(target), "rev-parse", "--show-toplevel"]).stdout.strip()
            ).resolve()
        except ScriptError as exc:
            raise ScriptError(
                f"worktree is not registered in repo {repo_root}: {target}. "
                f"Pass a registered git worktree or a clean checkout/worktree root of the same repository."
            ) from exc
        if checkout_root != target:
            raise ScriptError(
                f"--worktree-path must point at the checkout root, got nested path {target} inside {checkout_root}"
            )
        owner, actual_repo_name = repo_owner_and_name(target)
        expected_owner, _ = verify_repo_name(repo_root, repo_name)
        if (owner, actual_repo_name) != (expected_owner, repo_name):
            raise ScriptError(
                f"--worktree-path points at {owner}/{actual_repo_name}, expected {expected_owner}/{repo_name}"
            )
        checked_out_branch = run(["git", "-C", str(target), "branch", "--show-current"]).stdout.strip()
        if checked_out_branch != branch:
            raise ScriptError(
                f"worktree {target} is on {checked_out_branch or 'unknown branch'}, expected {branch!r}"
            )
        if not git_status_is_clean(target):
            raise ScriptError(f"existing worktree is dirty: {target}")
        ensure_worktree_node_modules_symlink(target, repo_root)
        return {
            "status": "ready",
            "worktree": str(target),
            "created": False,
            "managed": False,
            "registered": False,
        }

    checked_out_branch = existing.get("branch", "")
    if not checked_out_branch.endswith(f"/{branch}"):
        raise ScriptError(
            f"worktree {target} is on {checked_out_branch or 'unknown branch'}, expected {branch!r}"
        )

    if not git_status_is_clean(target):
        raise ScriptError(f"existing worktree is dirty: {target}")

    ensure_worktree_node_modules_symlink(target, repo_root)
    return {"status": "ready", "worktree": str(target), "created": False, "managed": False}


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
    ensure_worktree_node_modules_symlink(worktree, repo_root)
    return {"status": "ready", "worktree": str(worktree), "head": remote_head, "changed": local_head != remote_head}


def codex_exec_review(worktree: str | Path, pr_number: int, branch: str) -> dict[str, Any]:
    codex_bin = resolve_codex_executable()
    comment_instruction = agent_github_comment_instruction()
    prompt = textwrap.dedent(
        f"""
        You are working in a dedicated PR review worktree for PR #{pr_number} on branch {branch}.
        Handle unresolved GitHub review feedback, actionable top-level PR comments, and completed failing CI checks on the current branch.
        Apply only targeted fixes, run repo typecheck and any targeted validation needed for touched files, commit scoped changes, push, explicitly request reviewer chatgpt-codex-connector (or copilot-pull-request-reviewer where required) when more review is needed, and resolve threads only after the fix is pushed.
        {comment_instruction}
        If you addressed a top-level PR comment, reply on the PR after pushing with a short note that includes `<!-- {HANDLED_PR_COMMENT_MARKER} COMMENT_ID -->` for each handled comment ID.

        If there is no actionable review or CI work when you inspect the PR, report that clearly and make no code changes.
        """
    ).strip()

    result = run(
        [
            codex_bin,
            "exec",
            "--cd",
            str(worktree),
            "--dangerously-bypass-approvals-and-sandbox",
            "--add-dir",
            str(project_dir()),
            "--add-dir",
            str(codex_skills_dir()),
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
            f"# PR Review Automation\n\nThis automation is managed by shared tooling in {project_dir()}.\n",
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
