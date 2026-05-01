#!/usr/bin/env python3
"""Shared helpers for reusable PR review tooling."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import textwrap
from pathlib import Path
from typing import Any


CODEX_HOME = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))).expanduser()
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
MERGE_CONFLICT_COMMENT_SNIPPETS = (
    "resolve merge conflicts",
    "resolve merge conflict",
    "merge conflicts with",
    "merge conflict with",
    "has merge conflicts",
    "has conflicts that must be resolved",
    "cannot be merged cleanly",
)
LOW_CONFIDENCE_REVIEW_SNIPPETS = (
    "low confidence",
    "suppressed due to low confidence",
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
    command = [
        "gh",
        "api",
        "graphql",
        "-f",
        f"query={query}",
    ]
    for key, value in variables.items():
        if value is None:
            continue
        command.extend(["-F", f"{key}={value}"])
    result = run(command)
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


def is_copilot_no_comments_review(activity: dict[str, Any] | None) -> bool:
    if not activity:
        return False
    if activity.get("source") != "review":
        return False
    if not is_copilot_login(activity.get("author")):
        return False
    normalized_body = " ".join((activity.get("body") or "").casefold().split()).rstrip(".!").strip()
    return (
        normalized_body == "no comments"
        or "generated no comments" in normalized_body
        or "generated no new comments" in normalized_body
        or "generated 0 comments" in normalized_body
    )


def is_merge_conflict_comment(body: str | None) -> bool:
    normalized_body = " ".join((body or "").lower().split())
    if not normalized_body:
        return False
    return any(snippet in normalized_body for snippet in MERGE_CONFLICT_COMMENT_SNIPPETS)


def fetch_pull_request_state(repo_root: str | Path, repo_name: str, pr_number: int) -> dict[str, Any]:
    owner, repo = verify_repo_name(repo_root, repo_name)
    initial_query = textwrap.dedent(
        """
        query($owner: String!, $repo: String!, $pr: Int!) {
          repository(owner: $owner, name: $repo) {
            pullRequest(number: $pr) {
              number
              url
              title
              state
              mergeable
              mergeStateStatus
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
                pageInfo {
                  endCursor
                  hasNextPage
                }
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
    thread_page_query = textwrap.dedent(
        """
        query($owner: String!, $repo: String!, $pr: Int!, $threadCursor: String!) {
          repository(owner: $owner, name: $repo) {
            pullRequest(number: $pr) {
              reviewThreads(first: 100, after: $threadCursor) {
                pageInfo {
                  endCursor
                  hasNextPage
                }
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
    payload = github_graphql(initial_query, {"owner": owner, "repo": repo, "pr": pr_number})
    pull_request = payload["data"]["repository"]["pullRequest"]
    if not pull_request:
        raise ScriptError(f"pull request #{pr_number} not found for {owner}/{repo}")

    review_threads = pull_request.get("reviewThreads") or {}
    page_info = review_threads.get("pageInfo") or {}
    while page_info.get("hasNextPage"):
        cursor = page_info.get("endCursor")
        if not cursor:
            raise ScriptError("GitHub reviewThreads pagination reported hasNextPage without an endCursor")
        page_payload = github_graphql(
            thread_page_query,
            {"owner": owner, "repo": repo, "pr": pr_number, "threadCursor": cursor},
        )
        page_pull_request = page_payload["data"]["repository"]["pullRequest"]
        if not page_pull_request:
            raise ScriptError(f"pull request #{pr_number} not found for {owner}/{repo}")
        page_review_threads = page_pull_request.get("reviewThreads") or {}
        review_threads.setdefault("nodes", []).extend(page_review_threads.get("nodes") or [])
        page_info = page_review_threads.get("pageInfo") or {}

    review_threads.pop("pageInfo", None)
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


def is_low_confidence_review(author: str | None, body: str | None) -> bool:
    if not is_copilot_login(author) or not body:
        return False
    normalized = body.casefold()
    return any(snippet in normalized for snippet in LOW_CONFIDENCE_REVIEW_SNIPPETS)


def serialize_actionable_pr_comments(pull_request: dict[str, Any]) -> list[dict[str, Any]]:
    comments = pull_request.get("comments", {}).get("nodes") or []
    reviews = pull_request.get("reviews", {}).get("nodes") or []
    handled_ids: set[str] = set()
    feedback_summaries: list[dict[str, Any]] = []
    for comment in comments:
        body = (comment.get("body") or "").strip()
        author = (comment.get("author") or {}).get("login")
        handled_ids.update(extract_handled_pr_comment_ids(body))
        feedback_summaries.append(
            {
                "source": "comment",
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

    for review in reviews:
        body = (review.get("body") or "").strip()
        author = (review.get("author") or {}).get("login")
        feedback_summaries.append(
            {
                "source": "review",
                "id": review.get("id"),
                "author": author,
                "body": body,
                "createdAt": review.get("submittedAt"),
                "updatedAt": review.get("submittedAt"),
                "url": review.get("url"),
                "state": review.get("state"),
                "is_handler_comment": False,
                "is_linear_linkback": False,
                "is_low_confidence_review": is_low_confidence_review(author, body),
            }
        )

    actionable = [
        {
            "source": comment["source"],
            "id": comment["id"],
            "author": comment["author"],
            "body": comment["body"],
            "createdAt": comment["createdAt"],
            "updatedAt": comment["updatedAt"],
            "url": comment["url"],
            "state": comment.get("state"),
        }
        for comment in feedback_summaries
        if comment["id"]
        and comment["body"]
        and not comment["is_handler_comment"]
        and not comment["is_linear_linkback"]
        and not is_retryable_copilot_review_error(comment["author"], comment["body"])
        and (comment["source"] == "comment" or comment.get("is_low_confidence_review"))
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


def serialize_merge_conflicts(
    pull_request: dict[str, Any],
    actionable_pr_comments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    mergeable = (pull_request.get("mergeable") or "").upper()
    merge_state_status = (pull_request.get("mergeStateStatus") or "").upper()
    if mergeable == "CONFLICTING" or merge_state_status == "DIRTY":
        conflicts.append(
            {
                "source": "github",
                "summary": "GitHub reports merge conflicts against the base branch.",
                "mergeable": mergeable or None,
                "mergeStateStatus": merge_state_status or None,
            }
        )

    for comment in actionable_pr_comments:
        if not is_merge_conflict_comment(comment.get("body")):
            continue
        body = (comment.get("body") or "").replace("\r", " ").replace("\n", " ").strip()
        if len(body) > 240:
            body = body[:237] + "..."
        conflicts.append(
            {
                "source": "comment",
                "summary": body,
                "id": comment.get("id"),
                "author": comment.get("author"),
                "createdAt": comment.get("createdAt"),
                "updatedAt": comment.get("updatedAt"),
                "url": comment.get("url"),
            }
        )

    conflicts.sort(
        key=lambda item: (
            0 if item.get("source") == "github" else 1,
            item.get("updatedAt") or item.get("createdAt") or "",
            item.get("id") or "",
        )
    )
    return conflicts


def pull_request_snapshot(repo_root: str | Path, repo_name: str, pr_number: int) -> dict[str, Any]:
    pull_request = fetch_pull_request_state(repo_root, repo_name, pr_number)
    unresolved = serialize_unresolved_threads(pull_request)
    actionable_pr_comments = serialize_actionable_pr_comments(pull_request)
    failing_checks = serialize_failing_checks(pull_request)
    merge_conflicts = serialize_merge_conflicts(pull_request, actionable_pr_comments)
    latest_copilot_activity = serialize_latest_copilot_activity(pull_request)
    final_copilot_review = is_copilot_no_comments_review(latest_copilot_activity)
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
    if merge_conflicts:
        status = "merge_conflicts"
    elif unresolved or actionable_pr_comments:
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
        "merge_conflicts": merge_conflicts,
        "unresolved_threads": unresolved,
        "actionable_pr_comments": actionable_pr_comments,
        "failing_checks": failing_checks,
        "pending_copilot_review": pending_copilot_review,
        "copilot_review_error": copilot_review_error,
        "latest_copilot_activity": latest_copilot_activity,
        "final_copilot_review": final_copilot_review,
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
        "latest_copilot_activity": latest_copilot_activity,
        "final_copilot_review": final_copilot_review,
        "merge_conflicts": merge_conflicts,
        "unresolved_threads": unresolved,
        "actionable_pr_comments": actionable_pr_comments,
        "failing_checks": failing_checks,
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


def ensure_existing_worktree(
    repo_root: str | Path,
    repo_name: str,
    branch: str,
    worktree_path: str | Path,
    *,
    allow_dirty: bool = False,
) -> dict[str, Any]:
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
        if not allow_dirty and not git_status_is_clean(target):
            raise ScriptError(f"existing worktree is dirty: {target}")
        ensure_worktree_node_modules_symlink(target, repo_root)
        return {
            "status": "ready",
            "worktree": str(target),
            "created": False,
            "managed": False,
            "registered": False,
            "dirty": not git_status_is_clean(target),
        }

    checked_out_branch = existing.get("branch", "")
    if not checked_out_branch.endswith(f"/{branch}"):
        raise ScriptError(
            f"worktree {target} is on {checked_out_branch or 'unknown branch'}, expected {branch!r}"
        )

    if not allow_dirty and not git_status_is_clean(target):
        raise ScriptError(f"existing worktree is dirty: {target}")

    ensure_worktree_node_modules_symlink(target, repo_root)
    return {
        "status": "ready",
        "worktree": str(target),
        "created": False,
        "managed": False,
        "dirty": not git_status_is_clean(target),
    }


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


def clear_worktree_to_remote(repo_root: str | Path, branch: str, worktree: str | Path) -> dict[str, Any]:
    run(["git", "-C", str(repo_root), "fetch", "origin", branch])
    remote_ref = f"origin/{branch}"
    remote_head = run(["git", "-C", str(repo_root), "rev-parse", remote_ref]).stdout.strip()
    run(["git", "-C", str(worktree), "reset", "--hard", remote_ref])
    run(["git", "-C", str(worktree), "clean", "-fd"])
    ensure_worktree_node_modules_symlink(worktree, repo_root)
    return {"status": "ready", "worktree": str(worktree), "head": remote_head, "cleared": True}


def remove_worktree(repo_root: str | Path, worktree: str | Path) -> dict[str, Any]:
    worktree_path_obj = Path(worktree)
    if not worktree_path_obj.exists():
        return {"status": "ready", "removed": False, "worktree": str(worktree_path_obj)}

    if not git_status_is_clean(worktree_path_obj):
        raise ScriptError(f"refusing to remove dirty worktree: {worktree_path_obj}")

    run(["git", "-C", str(repo_root), "worktree", "remove", str(worktree_path_obj)])
    run(["git", "-C", str(repo_root), "worktree", "prune"])
    return {"status": "ready", "removed": True, "worktree": str(worktree_path_obj)}
