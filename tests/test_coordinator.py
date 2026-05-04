import json
import os
import sqlite3
import tempfile
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from unittest import mock

import pr_review_common
import pr_review_coordinator


def make_pull_request(
    *,
    unresolved=False,
    pending_copilot=False,
    pending_reviewer_login="copilot-pull-request-reviewer[bot]",
    failing_check=False,
    mergeable=None,
    merge_state_status=None,
    review_author="github-copilot[bot]",
    pr_comments=None,
    reviews=None,
):
    review_requests = []
    if pending_copilot:
        review_requests.append({"requestedReviewer": {"login": pending_reviewer_login}})
    review_threads = []
    if unresolved:
        review_threads.append(
            {
                "id": "thread-1",
                "isResolved": False,
                "isOutdated": False,
                "path": "src/app.ts",
                "line": 10,
                "originalLine": 10,
                "comments": {
                    "nodes": [
                        {
                            "id": "comment-1",
                            "author": {"login": review_author},
                            "body": "Please handle this edge case.",
                            "createdAt": "2026-03-09T00:00:00Z",
                            "url": "https://example.com/comment-1",
                            "path": "src/app.ts",
                            "line": 10,
                        }
                    ]
                },
            }
        )
    contexts = []
    if failing_check:
        contexts.append(
            {
                "__typename": "CheckRun",
                "name": "test",
                "status": "COMPLETED",
                "conclusion": "FAILURE",
                "detailsUrl": "https://example.com/check",
            }
        )
    return {
        "number": 42,
        "url": "https://example.com/pr/42",
        "title": "Example PR",
        "state": "OPEN",
        "mergeable": mergeable,
        "mergeStateStatus": merge_state_status,
        "reviewRequests": {"nodes": review_requests},
        "reviewThreads": {"nodes": review_threads},
        "comments": {"nodes": pr_comments or []},
        "reviews": {"nodes": reviews or []},
        "commits": {"nodes": [{"commit": {"statusCheckRollup": {"contexts": {"nodes": contexts}}}}]},
    }


class PullRequestSnapshotTests(unittest.TestCase):
    def snapshot(self, pull_request):
        original = pr_review_common.fetch_pull_request_state
        pr_review_common.fetch_pull_request_state = lambda repo_root, repo_name, pr_number: pull_request
        try:
            return pr_review_common.pull_request_snapshot("/tmp/repo", "repo", 42)
        finally:
            pr_review_common.fetch_pull_request_state = original

    def test_unresolved_review_threads_only(self):
        snapshot = self.snapshot(make_pull_request(unresolved=True))
        self.assertEqual(snapshot["status"], "needs_review")

    def test_non_copilot_unresolved_review_threads_also_trigger_work(self):
        snapshot = self.snapshot(make_pull_request(unresolved=True, review_author="reviewer"))
        self.assertEqual(snapshot["status"], "needs_review")

    def test_pending_copilot_without_comments(self):
        snapshot = self.snapshot(make_pull_request(pending_copilot=True))
        self.assertEqual(snapshot["status"], "pending_copilot_review")

    def test_pending_codex_connector_without_comments(self):
        snapshot = self.snapshot(
            make_pull_request(
                pending_copilot=True,
                pending_reviewer_login="chatgpt-codex-connector[bot]",
            )
        )
        self.assertEqual(snapshot["status"], "pending_copilot_review")

    def test_completed_failing_ci_only(self):
        snapshot = self.snapshot(make_pull_request(failing_check=True))
        self.assertEqual(snapshot["status"], "needs_ci_fix")

    def test_review_comments_take_priority_over_ci_failures(self):
        snapshot = self.snapshot(make_pull_request(unresolved=True, failing_check=True))
        self.assertEqual(snapshot["status"], "needs_review")

    def test_github_mergeability_conflicts_get_distinct_status(self):
        snapshot = self.snapshot(make_pull_request(mergeable="CONFLICTING", merge_state_status="DIRTY"))
        self.assertEqual(snapshot["status"], "merge_conflicts")
        self.assertEqual(snapshot["merge_conflicts"][0]["source"], "github")

    def test_retryable_copilot_error_review_enters_cooldown(self):
        snapshot = self.snapshot(
            make_pull_request(
                reviews=[
                    {
                        "id": "review-1",
                        "author": {"login": "github-copilot[bot]"},
                        "body": "Copilot encountered an error and was unable to review this pull request. You can try again by re-requesting a review.",
                        "state": "COMMENTED",
                        "submittedAt": "2026-03-09T03:00:00Z",
                        "url": "https://example.com/review-1",
                    }
                ]
            )
        )
        self.assertEqual(snapshot["status"], "copilot_review_cooldown")

    def test_retryable_copilot_error_comment_is_not_actionable(self):
        snapshot = self.snapshot(
            make_pull_request(
                pr_comments=[
                    {
                        "id": "issue-comment-1",
                        "author": {"login": "github-copilot[bot]"},
                        "body": "Copilot encountered an error and was unable to review this pull request. You can try again by re-requesting a review.",
                        "createdAt": "2026-03-09T03:00:00Z",
                        "updatedAt": "2026-03-09T03:00:00Z",
                        "url": "https://example.com/comment-1",
                    }
                ]
            )
        )
        self.assertEqual(snapshot["status"], "copilot_review_cooldown")
        self.assertEqual(snapshot["actionable_pr_comments"], [])

    def test_low_confidence_review_body_is_actionable(self):
        snapshot = self.snapshot(
            make_pull_request(
                reviews=[
                    {
                        "id": "review-1",
                        "author": {"login": "chatgpt-codex-connector[bot]"},
                        "body": "Low confidence comment: this path may still need a nil guard.",
                        "state": "COMMENTED",
                        "submittedAt": "2026-03-09T03:00:00Z",
                        "url": "https://example.com/review-1",
                    }
                ]
            )
        )
        self.assertEqual(snapshot["status"], "needs_review")
        self.assertEqual(len(snapshot["actionable_pr_comments"]), 1)
        self.assertEqual(snapshot["actionable_pr_comments"][0]["source"], "review")
        self.assertEqual(snapshot["actionable_pr_comments"][0]["id"], "review-1")

    def test_handled_marker_suppresses_low_confidence_review_body(self):
        snapshot = self.snapshot(
            make_pull_request(
                pr_comments=[
                    {
                        "id": "issue-comment-1",
                        "author": {"login": "jordanSSI"},
                        "body": "[jordanBot] Inspected the low-confidence review and no code change was needed. <!-- pr-review-coordinator:handled-comment review-1 -->",
                        "createdAt": "2026-03-09T04:00:00Z",
                        "updatedAt": "2026-03-09T04:00:00Z",
                        "url": "https://example.com/comment-1",
                    }
                ],
                reviews=[
                    {
                        "id": "review-1",
                        "author": {"login": "chatgpt-codex-connector[bot]"},
                        "body": "Comments suppressed due to low confidence (1)",
                        "state": "COMMENTED",
                        "submittedAt": "2026-03-09T03:00:00Z",
                        "url": "https://example.com/review-1",
                    }
                ]
            )
        )
        self.assertEqual(snapshot["status"], "awaiting_final_test")
        self.assertEqual(snapshot["actionable_pr_comments"], [])

    def test_stale_copilot_error_is_ignored_after_newer_copilot_activity(self):
        snapshot = self.snapshot(
            make_pull_request(
                reviews=[
                    {
                        "id": "review-1",
                        "author": {"login": "github-copilot[bot]"},
                        "body": "Copilot encountered an error and was unable to review this pull request. You can try again by re-requesting a review.",
                        "state": "COMMENTED",
                        "submittedAt": "2026-03-09T03:00:00Z",
                        "url": "https://example.com/review-1",
                    },
                    {
                        "id": "review-2",
                        "author": {"login": "github-copilot[bot]"},
                        "body": "No issues found in the latest pass.",
                        "state": "COMMENTED",
                        "submittedAt": "2026-03-09T04:00:00Z",
                        "url": "https://example.com/review-2",
                    },
                ]
            )
        )
        self.assertEqual(snapshot["status"], "awaiting_final_test")
        self.assertIsNone(snapshot["copilot_review_error"])
        self.assertFalse(snapshot["final_copilot_review"])

    def test_no_comments_copilot_review_is_final_review_marker(self):
        snapshot = self.snapshot(
            make_pull_request(
                reviews=[
                    {
                        "id": "review-1",
                        "author": {"login": "github-copilot[bot]"},
                        "body": "No comments.",
                        "state": "COMMENTED",
                        "submittedAt": "2026-03-09T04:00:00Z",
                        "url": "https://example.com/review-1",
                    },
                ]
            )
        )
        self.assertEqual(snapshot["status"], "awaiting_final_test")
        self.assertTrue(snapshot["final_copilot_review"])
        self.assertEqual(snapshot["latest_copilot_activity"]["id"], "review-1")

    def test_copilot_overview_with_generated_no_comments_is_final_review_marker(self):
        snapshot = self.snapshot(
            make_pull_request(
                reviews=[
                    {
                        "id": "review-1",
                        "author": {"login": "copilot-pull-request-reviewer"},
                        "body": """
                        ## Pull request overview

                        Reviewed changes

                        Copilot reviewed 31 out of 31 changed files in this pull request and generated no comments.
                        """,
                        "state": "COMMENTED",
                        "submittedAt": "2026-03-09T04:00:00Z",
                        "url": "https://example.com/review-1",
                    },
                ]
            )
        )
        self.assertEqual(snapshot["status"], "awaiting_final_test")
        self.assertTrue(snapshot["final_copilot_review"])
        self.assertEqual(snapshot["latest_copilot_activity"]["id"], "review-1")

    def test_copilot_overview_with_generated_no_new_comments_is_final_review_marker(self):
        snapshot = self.snapshot(
            make_pull_request(
                reviews=[
                    {
                        "id": "review-1",
                        "author": {"login": "copilot-pull-request-reviewer"},
                        "body": """
                        ## Pull request overview

                        Copilot reviewed 9 out of 9 changed files in this pull request and generated no new comments.
                        """,
                        "state": "COMMENTED",
                        "submittedAt": "2026-03-09T04:00:00Z",
                        "url": "https://example.com/review-1",
                    },
                ]
            )
        )
        self.assertEqual(snapshot["status"], "awaiting_final_test")
        self.assertTrue(snapshot["final_copilot_review"])
        self.assertEqual(snapshot["latest_copilot_activity"]["id"], "review-1")

    def test_clean_green_pr(self):
        snapshot = self.snapshot(make_pull_request())
        self.assertEqual(snapshot["status"], "awaiting_final_test")


class PullRequestPaginationTests(unittest.TestCase):
    def snapshot(self, pull_request):
        original = pr_review_common.fetch_pull_request_state
        pr_review_common.fetch_pull_request_state = lambda repo_root, repo_name, pr_number: pull_request
        try:
            return pr_review_common.pull_request_snapshot("/tmp/repo", "repo", 42)
        finally:
            pr_review_common.fetch_pull_request_state = original

    def test_second_page_review_threads_are_included_in_snapshot(self):
        responses = [
            {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "number": 42,
                            "url": "https://example.com/pr/42",
                            "title": "Example PR",
                            "state": "OPEN",
                            "reviewRequests": {"nodes": []},
                            "reviewThreads": {
                                "pageInfo": {"endCursor": "cursor-1", "hasNextPage": True},
                                "nodes": [
                                    {
                                        "id": "thread-1",
                                        "isResolved": True,
                                        "isOutdated": False,
                                        "path": "src/app.ts",
                                        "line": 10,
                                        "originalLine": 10,
                                        "comments": {
                                            "nodes": [
                                                {
                                                    "id": "comment-1",
                                                    "author": {"login": "github-copilot[bot]"},
                                                    "body": "Already resolved.",
                                                    "createdAt": "2026-03-09T00:00:00Z",
                                                    "url": "https://example.com/comment-1",
                                                    "path": "src/app.ts",
                                                    "line": 10,
                                                }
                                            ]
                                        },
                                    }
                                ],
                            },
                            "comments": {"nodes": []},
                            "reviews": {"nodes": []},
                            "commits": {"nodes": [{"commit": {"statusCheckRollup": {"contexts": {"nodes": []}}}}]},
                        }
                    }
                }
            },
            {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "reviewThreads": {
                                "pageInfo": {"endCursor": None, "hasNextPage": False},
                                "nodes": [
                                    {
                                        "id": "thread-2",
                                        "isResolved": False,
                                        "isOutdated": False,
                                        "path": "src/other.ts",
                                        "line": 22,
                                        "originalLine": 22,
                                        "comments": {
                                            "nodes": [
                                                {
                                                    "id": "comment-2",
                                                    "author": {"login": "github-copilot[bot]"},
                                                    "body": "This still needs a fix.",
                                                    "createdAt": "2026-03-10T00:00:00Z",
                                                    "url": "https://example.com/comment-2",
                                                    "path": "src/other.ts",
                                                    "line": 22,
                                                }
                                            ]
                                        },
                                    }
                                ],
                            }
                        }
                    }
                }
            },
        ]
        graphql_calls: list[dict[str, object]] = []

        def fake_graphql(query, variables):
            graphql_calls.append({"query": query, "variables": dict(variables)})
            return responses.pop(0)

        with mock.patch("pr_review_common.verify_repo_name", return_value=("example", "repo")):
            with mock.patch("pr_review_common.github_graphql", side_effect=fake_graphql):
                snapshot = pr_review_common.pull_request_snapshot("/tmp/repo", "repo", 42)

        self.assertEqual(snapshot["status"], "needs_review")
        self.assertEqual([thread["id"] for thread in snapshot["unresolved_threads"]], ["thread-2"])
        self.assertEqual(len(graphql_calls), 2)
        self.assertEqual(graphql_calls[1]["variables"]["threadCursor"], "cursor-1")

    def test_top_level_merge_conflict_comment_gets_distinct_status(self):
        snapshot = self.snapshot(
            make_pull_request(
                pr_comments=[
                    {
                        "id": "issue-comment-1",
                        "author": {"login": "jordanSSI"},
                        "body": "Please resolve merge conflicts with master.",
                        "createdAt": "2026-03-09T01:00:00Z",
                        "updatedAt": "2026-03-09T01:00:00Z",
                        "url": "https://example.com/comment-1",
                    }
                ]
            )
        )
        self.assertEqual(snapshot["status"], "merge_conflicts")
        self.assertEqual(snapshot["merge_conflicts"][0]["source"], "comment")
        self.assertEqual(len(snapshot["actionable_pr_comments"]), 1)

    def test_linear_linkback_comment_is_not_actionable(self):
        snapshot = self.snapshot(
            make_pull_request(
                pr_comments=[
                    {
                        "id": "issue-comment-1",
                        "author": {"login": "linear"},
                        "body": "<!-- linear-linkback -->\n<details>\n<summary><a href=\"https://linear.app/ssi/issue/SSI-4070/example\">SSI-4070 Example</a></summary>\n<p>Context</p>\n</details>",
                        "createdAt": "2026-03-09T01:00:00Z",
                        "updatedAt": "2026-03-09T01:00:00Z",
                        "url": "https://example.com/comment-1",
                    }
                ]
            )
        )
        self.assertEqual(snapshot["status"], "awaiting_final_test")
        self.assertEqual(snapshot["actionable_pr_comments"], [])

    def test_non_linkback_linear_comment_still_triggers_needs_review(self):
        snapshot = self.snapshot(
            make_pull_request(
                pr_comments=[
                    {
                        "id": "issue-comment-1",
                        "author": {"login": "linear"},
                        "body": "Please review this follow-up before merging.",
                        "createdAt": "2026-03-09T01:00:00Z",
                        "updatedAt": "2026-03-09T01:00:00Z",
                        "url": "https://example.com/comment-1",
                    }
                ]
            )
        )
        self.assertEqual(snapshot["status"], "needs_review")
        self.assertEqual(len(snapshot["actionable_pr_comments"]), 1)

    def test_handled_marker_comment_suppresses_prior_pr_comment(self):
        snapshot = self.snapshot(
            make_pull_request(
                pr_comments=[
                    {
                        "id": "issue-comment-1",
                        "author": {"login": "jordanSSI"},
                        "body": "Please resolve merge conflicts with master.",
                        "createdAt": "2026-03-09T01:00:00Z",
                        "updatedAt": "2026-03-09T01:00:00Z",
                        "url": "https://example.com/comment-1",
                    },
                    {
                        "id": "issue-comment-2",
                        "author": {"login": "jordanSSI"},
                        "body": "[jordanBot] Merged master and resolved conflicts. <!-- pr-review-coordinator:handled-comment issue-comment-1 -->",
                        "createdAt": "2026-03-09T02:00:00Z",
                        "updatedAt": "2026-03-09T02:00:00Z",
                        "url": "https://example.com/comment-2",
                    },
                ]
            )
        )
        self.assertEqual(snapshot["status"], "awaiting_final_test")
        self.assertEqual(snapshot["actionable_pr_comments"], [])

    def test_handled_marker_without_jordanbot_prefix_does_not_suppress_prior_pr_comment(self):
        snapshot = self.snapshot(
            make_pull_request(
                pr_comments=[
                    {
                        "id": "issue-comment-1",
                        "author": {"login": "jordanSSI"},
                        "body": "Please resolve merge conflicts with master.",
                        "createdAt": "2026-03-09T01:00:00Z",
                        "updatedAt": "2026-03-09T01:00:00Z",
                        "url": "https://example.com/comment-1",
                    },
                    {
                        "id": "issue-comment-2",
                        "author": {"login": "jordanSSI"},
                        "body": "Merged master and resolved conflicts. <!-- pr-review-coordinator:handled-comment issue-comment-1 -->",
                        "createdAt": "2026-03-09T02:00:00Z",
                        "updatedAt": "2026-03-09T02:00:00Z",
                        "url": "https://example.com/comment-2",
                    },
                ]
            )
        )
        self.assertEqual(snapshot["status"], "merge_conflicts")
        self.assertEqual(len(snapshot["actionable_pr_comments"]), 1)

    def test_handled_marker_uses_configured_prefix(self):
        original_config = pr_review_common.PR_REVIEW_COORDINATOR_CONFIG
        try:
            with tempfile.TemporaryDirectory() as tmp:
                config_path = Path(tmp) / "pr-review-coordinator.json"
                config_path.write_text('{"agent_nickname":"reviewBot"}', encoding="utf-8")
                pr_review_common.PR_REVIEW_COORDINATOR_CONFIG = config_path

                snapshot = self.snapshot(
                    make_pull_request(
                        pr_comments=[
                            {
                                "id": "issue-comment-1",
                                "author": {"login": "jordanSSI"},
                                "body": "Please resolve merge conflicts with master.",
                                "createdAt": "2026-03-09T01:00:00Z",
                                "updatedAt": "2026-03-09T01:00:00Z",
                                "url": "https://example.com/comment-1",
                            },
                            {
                                "id": "issue-comment-2",
                                "author": {"login": "jordanSSI"},
                                "body": "[reviewBot] Merged master and resolved conflicts. <!-- pr-review-coordinator:handled-comment issue-comment-1 -->",
                                "createdAt": "2026-03-09T02:00:00Z",
                                "updatedAt": "2026-03-09T02:00:00Z",
                                "url": "https://example.com/comment-2",
                            },
                        ]
                    )
                )

                self.assertEqual(snapshot["status"], "awaiting_final_test")
                self.assertEqual(snapshot["actionable_pr_comments"], [])
        finally:
            pr_review_common.PR_REVIEW_COORDINATOR_CONFIG = original_config


class CodexBinaryResolutionTests(unittest.TestCase):
    def test_prefers_codex_bin_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_codex = Path(tmp) / "codex"
            fake_codex.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            fake_codex.chmod(0o755)
            with mock.patch.dict(os.environ, {"CODEX_BIN": str(fake_codex)}, clear=False):
                self.assertEqual(pr_review_common.resolve_codex_executable(), str(fake_codex))

    def test_falls_back_when_codex_not_on_path(self):
        with mock.patch.dict(os.environ, {"CODEX_BIN": ""}, clear=False):
            with mock.patch("pr_review_common.shutil.which", return_value=None):
                with mock.patch("pr_review_common.Path.is_file", return_value=True):
                    with mock.patch("pr_review_common.os.access", return_value=True):
                        resolved = pr_review_common.resolve_codex_executable()
        self.assertTrue(resolved.endswith("codex"))


class PromptInstructionTests(unittest.TestCase):
    def make_record(self, **overrides):
        payload = {
            "key": "repo-pr-42",
            "repo_root": "/tmp/repo",
            "repo_owner": "owner",
            "repo_name": "repo",
            "pr_number": 42,
            "pr_url": "https://example.com/pr/42",
            "pr_title": "Example PR",
            "pr_state": "OPEN",
            "branch": "feature/example",
            "base_branch": "main",
            "worktree_path": "/tmp/worktree",
            "worktree_managed": 1,
            "thread_id": "thread-42",
            "thread_title": "Original bug report and context for the thread",
            "status": "needs_review",
            "active": 1,
            "last_review_signature": None,
            "last_handled_signature": None,
            "last_review_status": "needs_review",
            "last_review_comment_at": None,
            "pending_copilot_review": 0,
            "unresolved_thread_count": 1,
            "actionable_comment_count": 1,
            "failing_check_count": 0,
            "unresolved_threads_json": "[]",
            "actionable_comments_json": "[]",
            "failing_checks_json": "[]",
            "ci_summary": None,
            "run_state": None,
            "run_reason": None,
            "current_job_id": None,
            "lock_started_at": None,
            "lock_owner_pid": None,
            "last_polled_at": None,
            "last_prompted_at": None,
            "last_run_started_at": None,
            "last_run_finished_at": None,
            "last_run_status": "ready",
            "last_run_summary": "Idle",
            "last_error": None,
            "provider": "codex",
            "created_at": 0,
            "updated_at": 0,
            "live_activity_json": None,
            "live_activity_updated_at": None,
        }
        payload.update(overrides)
        return pr_review_coordinator.TrackedPR(**payload)

    def test_resume_prompt_requires_jordanbot_prefix(self):
        record = self.make_record()
        snapshot = {
            "merge_conflicts": [],
            "unresolved_threads": [],
            "actionable_pr_comments": [],
            "failing_checks": [],
        }

        prompt = pr_review_coordinator.resume_prompt(record, snapshot)

        self.assertIn("must begin with `[jordanBot]`", prompt)
        self.assertIn("handled-comment COMMENT_ID", prompt)
        self.assertIn("low-confidence review body", prompt)
        self.assertIn("Request reviewer `copilot-pull-request-reviewer` after every push", prompt)
        self.assertNotIn("Request reviewer `chatgpt-codex-connector`", prompt)

    def test_resume_prompt_includes_merge_conflict_guidance(self):
        record = self.make_record(status="merge_conflicts", last_review_status="merge_conflicts")
        snapshot = {
            "merge_conflicts": [{"source": "github", "mergeable": "CONFLICTING", "mergeStateStatus": "DIRTY"}],
            "unresolved_threads": [],
            "actionable_pr_comments": [],
            "failing_checks": [],
        }

        prompt = pr_review_coordinator.resume_prompt(record, snapshot)

        self.assertIn("If merge conflicts are reported", prompt)
        self.assertIn("Current merge conflict signals", prompt)
        self.assertIn("mergeable=CONFLICTING", prompt)

    def test_resume_prompt_includes_dashboard_steering_message(self):
        record = self.make_record()
        snapshot = {
            "merge_conflicts": [],
            "unresolved_threads": [],
            "actionable_pr_comments": [],
            "failing_checks": [],
        }

        prompt = pr_review_coordinator.resume_prompt(record, snapshot, steering_message="Focus on the failing checkout flow.")

        self.assertIn("User steering message queued from the dashboard", prompt)
        self.assertIn("Focus on the failing checkout flow.", prompt)


class WorktreePathTests(unittest.TestCase):
    def test_resolve_agent_comment_prefix_uses_bootstrapped_config(self):
        original_config = pr_review_common.PR_REVIEW_COORDINATOR_CONFIG
        try:
            with tempfile.TemporaryDirectory() as tmp:
                config_path = Path(tmp) / "pr-review-coordinator.json"
                config_path.write_text('{"agent_nickname":"reviewBot"}', encoding="utf-8")
                pr_review_common.PR_REVIEW_COORDINATOR_CONFIG = config_path
                self.assertEqual(pr_review_common.resolve_agent_comment_prefix(), "[reviewBot]")
        finally:
            pr_review_common.PR_REVIEW_COORDINATOR_CONFIG = original_config

    def test_nested_layout_keeps_repo_subdirectory_shape(self):
        path = pr_review_common.worktree_path(
            "starshipit-wms",
            418,
            "feat/putaway-split-lines-and-serial-scan",
            "/Users/jordan/source/worktrees",
            layout="nested",
        )
        self.assertEqual(
            path,
            Path("/Users/jordan/source/worktrees/starshipit-wms/pr-418-feat-putaway-split-lines-and-serial-scan"),
        )

    def test_sibling_layout_uses_repo_and_pr_number_only(self):
        path = pr_review_common.worktree_path(
            "starshipit-wms",
            418,
            "feat/putaway-split-lines-and-serial-scan",
            "/Users/jordan/source",
            layout="sibling",
        )
        self.assertEqual(path, Path("/Users/jordan/source/starshipit-wms-pr-418"))

    def test_rejects_managed_worktree_root_inside_repo(self):
        with self.assertRaises(pr_review_common.ScriptError):
            pr_review_common.validate_managed_worktree_root(
                "/Users/jordan/source/starshipit-wms",
                "/Users/jordan/source/starshipit-wms",
            )

    def test_rejects_managed_worktree_target_inside_repo(self):
        with self.assertRaises(pr_review_common.ScriptError):
            pr_review_common.validate_worktree_target(
                "/Users/jordan/source/starshipit-wms",
                "/Users/jordan/source/starshipit-wms/pr-418-feat-putaway-split-lines-and-serial-scan",
            )

    def test_ensure_existing_worktree_accepts_clean_checkout_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp) / "repo"
            checkout = Path(tmp) / "checkout"
            repo_root.mkdir()
            checkout.mkdir()
            resolved_repo_root = repo_root.resolve()
            resolved_checkout = checkout.resolve()
            repo_root_variants = {str(repo_root), str(resolved_repo_root)}
            checkout_variants = {str(checkout), str(resolved_checkout)}

            def fake_run(cmd, *, cwd=None, check=True, capture_output=True):
                if cmd[:2] == ["git", "-C"] and cmd[2] in repo_root_variants and cmd[3:] == ["remote", "get-url", "origin"]:
                    return mock.Mock(stdout="git@github.com:Starshipit-Product/starshipit-wms.git\n")
                if cmd[:2] == ["git", "-C"] and cmd[2] in repo_root_variants and cmd[3:] == ["worktree", "list", "--porcelain"]:
                    return mock.Mock(stdout="")
                if cmd[:2] == ["git", "-C"] and cmd[2] in checkout_variants and cmd[3:] == ["rev-parse", "--show-toplevel"]:
                    return mock.Mock(stdout=f"{resolved_checkout}\n")
                if cmd[:2] == ["git", "-C"] and cmd[2] in checkout_variants and cmd[3:] == ["remote", "get-url", "origin"]:
                    return mock.Mock(stdout="git@github.com:Starshipit-Product/starshipit-wms.git\n")
                if cmd[:2] == ["git", "-C"] and cmd[2] in checkout_variants and cmd[3:] == ["branch", "--show-current"]:
                    return mock.Mock(stdout="feature/example\n")
                if cmd[:2] == ["git", "-C"] and cmd[2] in checkout_variants and cmd[3:] == ["status", "--porcelain", "--untracked-files=normal"]:
                    return mock.Mock(stdout="")
                raise AssertionError(f"unexpected command: {cmd}")

            with mock.patch.object(pr_review_common, "run", side_effect=fake_run):
                result = pr_review_common.ensure_existing_worktree(
                    repo_root,
                    "starshipit-wms",
                    "feature/example",
                    checkout,
                )

        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["worktree"], str(resolved_checkout))
        self.assertFalse(result["registered"])


class RepoNameAndCommitTests(unittest.TestCase):
    def test_ensure_repo_name_accepts_owner_qualified_input(self):
        with mock.patch.object(
            pr_review_coordinator,
            "repo_owner_and_name",
            return_value=("Starshipit-Product", "starshipit-wms"),
        ):
            owner, repo = pr_review_coordinator.ensure_repo_name(
                "/tmp/repo",
                "Starshipit-Product/starshipit-wms",
            )

        self.assertEqual(owner, "Starshipit-Product")
        self.assertEqual(repo, "starshipit-wms")

    def test_commit_all_changes_preserves_unstaged_when_index_is_prepared(self):
        commands: list[list[str]] = []

        def fake_run(cmd, *, cwd=None, check=True, capture_output=True):
            commands.append(cmd)
            if cmd[:4] == ["git", "-C", "/tmp/repo", "status"]:
                return mock.Mock(
                    stdout="M  lib/business/starshipit/update.ts\n M components/process-logs-view.tsx\n"
                )
            if cmd[:4] == ["git", "-C", "/tmp/repo", "commit"]:
                return mock.Mock(stdout="")
            if cmd[:4] == ["git", "-C", "/tmp/repo", "rev-parse"]:
                return mock.Mock(stdout="abc123\n")
            raise AssertionError(f"unexpected command: {cmd}")

        with mock.patch.object(pr_review_coordinator, "run", side_effect=fake_run):
            result = pr_review_coordinator.commit_all_changes("/tmp/repo", "Example")

        self.assertEqual(result["sha"], "abc123")
        self.assertEqual(result["committed_paths"], ["lib/business/starshipit/update.ts"])
        self.assertEqual(
            result["preserved_paths"],
            ["components/process-logs-view.tsx"],
        )
        self.assertNotIn(["git", "-C", "/tmp/repo", "add", "-A"], commands)


class OpenPullRequestListingTests(unittest.TestCase):
    def test_list_open_pull_requests_marks_existing_tracked_prs(self):
        original_ensure_repo_name = pr_review_coordinator.ensure_repo_name
        original_run = pr_review_coordinator.run
        original_list_tracked_prs = pr_review_coordinator.list_tracked_prs
        tracked = mock.Mock()
        tracked.key = "repo-pr-42"
        tracked.repo_name = "repo"
        tracked.status = "needs_review"
        tracked.active = 1
        try:
            pr_review_coordinator.ensure_repo_name = lambda repo_root, repo_name: ("owner", "repo")
            pr_review_coordinator.run = lambda *args, **kwargs: mock.Mock(
                stdout=json.dumps(
                    [
                        {
                            "number": 43,
                            "url": "https://example.com/pr/43",
                            "title": "PR 43",
                            "headRefName": "branch-43",
                            "baseRefName": "main",
                            "isDraft": False,
                            "state": "OPEN",
                        },
                        {
                            "number": 42,
                            "url": "https://example.com/pr/42",
                            "title": "PR 42",
                            "headRefName": "branch-42",
                            "baseRefName": "main",
                            "isDraft": True,
                            "state": "OPEN",
                        },
                    ]
                )
            )
            pr_review_coordinator.list_tracked_prs = lambda active_only=False: [tracked]

            result = pr_review_coordinator.list_open_pull_requests_for_repo("/tmp/repo")

            self.assertEqual(result["repo_name"], "repo")
            self.assertEqual([item["number"] for item in result["prs"]], [43, 42])
            self.assertFalse(result["prs"][0]["tracked"])
            self.assertTrue(result["prs"][1]["tracked"])
            self.assertEqual(result["prs"][1]["tracked_status"], "needs_review")
            self.assertTrue(result["prs"][1]["tracked_active"])
        finally:
            pr_review_coordinator.ensure_repo_name = original_ensure_repo_name
            pr_review_coordinator.run = original_run
            pr_review_coordinator.list_tracked_prs = original_list_tracked_prs


class HtmlPageTests(unittest.TestCase):
    def test_html_page_does_not_force_meta_refresh(self):
        page = pr_review_coordinator.html_page("Dashboard", "<main></main>").decode("utf-8")

        self.assertNotIn('http-equiv="refresh"', page)

    def test_html_page_constrains_tables_and_wraps_long_code_content(self):
        page = pr_review_coordinator.html_page("Dashboard", "<main></main>").decode("utf-8")

        self.assertIn(".table-shell { width: 100%; max-width: 100%; overflow-x: auto;", page)
        self.assertIn("table-layout: fixed;", page)
        self.assertIn("td code, .thread-disclosure code, .thread-panel code { white-space: pre-wrap;", page)


class DashboardHttpTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_var_dir = pr_review_coordinator.VAR_DIR
        self.original_locks_dir = pr_review_coordinator.LOCKS_DIR
        self.original_db = pr_review_coordinator.COORDINATOR_DB
        self.original_state_db = pr_review_coordinator.CODEX_STATE_DB
        self.original_ensure_repo_name = pr_review_coordinator.ensure_repo_name
        self.original_run = pr_review_coordinator.run
        self.original_list_recent_threads_for_repo = pr_review_coordinator.list_recent_threads_for_repo
        self.original_resolve_selected_thread = pr_review_coordinator.resolve_selected_thread
        self.original_assert_thread_available = pr_review_coordinator.assert_thread_available

        pr_review_coordinator.VAR_DIR = Path(self.temp_dir.name)
        pr_review_coordinator.LOCKS_DIR = pr_review_coordinator.VAR_DIR / "locks"
        pr_review_coordinator.COORDINATOR_DB = pr_review_coordinator.VAR_DIR / "test.db"
        pr_review_coordinator.CODEX_STATE_DB = Path(self.temp_dir.name) / "state.sqlite"

        self.server = pr_review_coordinator.ThreadingHTTPServer(("127.0.0.1", 0), pr_review_coordinator.DashboardHandler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        host, port = self.server.server_address
        self.base_url = f"http://{host}:{port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=2)

        pr_review_coordinator.VAR_DIR = self.original_var_dir
        pr_review_coordinator.LOCKS_DIR = self.original_locks_dir
        pr_review_coordinator.COORDINATOR_DB = self.original_db
        pr_review_coordinator.CODEX_STATE_DB = self.original_state_db
        pr_review_coordinator.ensure_repo_name = self.original_ensure_repo_name
        pr_review_coordinator.run = self.original_run
        pr_review_coordinator.list_recent_threads_for_repo = self.original_list_recent_threads_for_repo
        pr_review_coordinator.resolve_selected_thread = self.original_resolve_selected_thread
        pr_review_coordinator.assert_thread_available = self.original_assert_thread_available
        self.temp_dir.cleanup()

    def request(self, method, path, data=None):
        body = None
        headers = {}
        if data is not None:
            body = urllib.parse.urlencode(data, doseq=True).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded;charset=UTF-8"
        request = urllib.request.Request(self.base_url + path, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read().decode("utf-8")

    def add_record(self, **overrides):
        payload = {
            "key": "repo-pr-42",
            "repo_root": "/tmp/repo",
            "repo_owner": "owner",
            "repo_name": "repo",
            "pr_number": 42,
            "pr_url": "https://example.com/pr/42",
            "pr_title": "Example PR",
            "pr_state": "OPEN",
            "branch": "feature/example",
            "base_branch": "main",
            "worktree_path": "/tmp/worktree",
            "worktree_managed": 1,
            "thread_id": "thread-42",
            "thread_title": "Original bug report and context for the thread",
            "status": "needs_review",
            "active": 1,
            "last_review_signature": None,
            "last_handled_signature": None,
            "last_review_status": "needs_review",
            "last_review_comment_at": None,
            "pending_copilot_review": 0,
            "unresolved_thread_count": 1,
            "actionable_comment_count": 0,
            "failing_check_count": 0,
            "unresolved_threads_json": "[]",
            "actionable_comments_json": "[]",
            "failing_checks_json": "[]",
            "ci_summary": None,
            "run_state": None,
            "run_reason": None,
            "current_job_id": None,
            "lock_started_at": None,
            "lock_owner_pid": None,
            "last_polled_at": None,
            "last_prompted_at": None,
            "last_run_started_at": None,
            "last_run_finished_at": None,
            "last_run_status": "ready",
            "last_run_summary": "Idle",
            "last_error": None,
            "last_copilot_rerequested_at": None,
            "provider": "codex",
            "created_at": 0,
            "updated_at": 0,
        }
        payload.update(overrides)
        pr_review_coordinator.upsert_tracked_pr(payload)

    def test_get_root_renders_dashboard_shell_without_import_controls(self):
        status, body = self.request("GET", "/?scope=all&status=needs_review&sort=pr&project_root=/tmp/repo&provider=cursor&notice=test")

        self.assertEqual(status, 200)
        self.assertIn('id="dashboard-root"', body)
        self.assertIn('name="scope"', body)
        self.assertIn('value="all" selected', body)
        self.assertIn('value="needs_review" selected', body)
        self.assertIn('value="pr" selected', body)
        self.assertNotIn('id="import-form"', body)
        self.assertNotIn("Track Open PRs", body)

    def test_get_import_renders_import_shell(self):
        status, body = self.request("GET", "/import?project_root=/tmp/repo&provider=cursor&notice=test")

        self.assertEqual(status, 200)
        self.assertIn('id="import-form"', body)
        self.assertIn('id="import-browser"', body)
        self.assertNotIn('id="dashboard-filters"', body)

    def test_get_next_renders_new_dashboard_shell(self):
        status, body = self.request("GET", "/next?scope=all&status=needs_review&sort=pr")

        self.assertEqual(status, 200)
        self.assertIn('id="next-dashboard-root"', body)
        self.assertIn('id="pr-tabs"', body)
        self.assertIn('id="steer-form"', body)
        self.assertIn('href="/"', body)
        self.assertNotIn('id="tracked-pr-body"', body)

    def test_api_dashboard_returns_filtered_payload(self):
        self.add_record(key="repo-pr-1", pr_number=1, pr_title="Needs review", status="needs_review", active=1, updated_at=10)
        self.add_record(key="repo-pr-2", pr_number=2, pr_title="Archived", status="awaiting_final_test", active=0, updated_at=20)
        pr_review_coordinator.enqueue_job("poll-one", tracked_pr_key="repo-pr-1", requested_by="test")
        pr_review_coordinator.record_event("info", "test_event", "hello", tracked_pr_key="repo-pr-1")

        status, body = self.request("GET", "/api/dashboard?scope=all&status=needs_review&sort=pr")
        payload = json.loads(body)

        self.assertEqual(status, 200)
        self.assertEqual(payload["filters"], {"scope": "all", "status": "needs_review", "sort": "pr"})
        self.assertEqual(len(payload["records"]), 1)
        self.assertEqual(payload["records"][0]["key"], "repo-pr-1")
        self.assertTrue(payload["jobs"])
        self.assertTrue(payload["events"])

    def test_api_import_open_prs_returns_repo_data(self):
        pr_review_coordinator.ensure_repo_name = lambda repo_root, repo_name: ("owner", "repo")
        pr_review_coordinator.run = lambda *args, **kwargs: mock.Mock(
            stdout=json.dumps(
                [
                    {
                        "number": 42,
                        "url": "https://example.com/pr/42",
                        "title": "Example PR",
                        "headRefName": "feature/example",
                        "baseRefName": "main",
                        "isDraft": False,
                        "state": "OPEN",
                    }
                ]
            )
        )
        pr_review_coordinator.list_recent_threads_for_repo = lambda repo_root, limit=12, current_key=None: [
            {"id": "thread-1", "title": "Recent thread", "updated_at": 100, "in_use_by": None, "conflict": False}
        ]

        with tempfile.TemporaryDirectory() as repo_dir:
            status_result = mock.Mock(stdout=repo_dir)
            original_run = pr_review_coordinator.run

            def fake_run(cmd, cwd=None):
                if cmd[:4] == ["git", "-C", repo_dir, "rev-parse"]:
                    return status_result
                return original_run(cmd, cwd=cwd)

            pr_review_coordinator.run = fake_run
            status, body = self.request("GET", f"/api/import/open-prs?repo_root={urllib.parse.quote(repo_dir)}&provider=codex")

        payload = json.loads(body)
        self.assertEqual(status, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["repo_name"], "repo")
        self.assertEqual(payload["prs"][0]["number"], 42)
        self.assertEqual(payload["threads"][0]["id"], "thread-1")

    def test_api_track_open_returns_accepted_json(self):
        pr_review_coordinator.ensure_repo_name = lambda repo_root, repo_name: ("owner", "repo")
        pr_review_coordinator.resolve_selected_thread = lambda repo_root, provider, requested_thread_id, prefer_latest_when_empty=False: {
            "id": requested_thread_id or "thread-latest",
            "title": "Thread",
        }
        pr_review_coordinator.assert_thread_available = lambda thread_id, key: None

        status, body = self.request(
            "POST",
            "/api/actions/track-open",
            data={
                "project_root": "/tmp/repo",
                "repo_name": "repo",
                "provider": "codex",
                "selected_pr": ["42"],
                "branch_42": "feature/example",
                "thread_strategy_42": "specific_thread",
                "thread_id_42": "thread-42",
            },
        )
        payload = json.loads(body)

        self.assertEqual(status, 202)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["queued"], 1)
        job = pr_review_coordinator.get_job(payload["job_ids"][0])
        self.assertEqual(job.action, "track-existing")

    def test_api_steer_returns_accepted_json(self):
        self.add_record(key="repo-pr-1", pr_number=1)

        status, body = self.request(
            "POST",
            "/api/actions/steer",
            data={"key": "repo-pr-1", "message": "Focus on the failing checkout flow."},
        )
        payload = json.loads(body)
        job = pr_review_coordinator.get_job(payload["job_id"])

        self.assertEqual(status, 202)
        self.assertTrue(payload["ok"])
        self.assertEqual(job.action, "steer-message")
        self.assertEqual(pr_review_coordinator.decode_job_payload(job)["message"], "Focus on the failing checkout flow.")
        self.assertEqual(pr_review_coordinator.job_to_dict(job)["payload_message"], "Focus on the failing checkout flow.")

    def test_api_steer_rejects_empty_message(self):
        self.add_record(key="repo-pr-1", pr_number=1)

        status, body = self.request("POST", "/api/actions/steer", data={"key": "repo-pr-1", "message": "   "})
        payload = json.loads(body)

        self.assertEqual(status, 400)
        self.assertFalse(payload["ok"])
        self.assertIn("Enter a steering message", payload["message"])

    def test_api_track_open_rejects_missing_specific_thread(self):
        status, body = self.request(
            "POST",
            "/api/actions/track-open",
            data={
                "project_root": "/tmp/repo",
                "repo_name": "repo",
                "provider": "codex",
                "selected_pr": ["42"],
                "branch_42": "feature/example",
                "thread_strategy_42": "specific_thread",
                "thread_id_42": "",
            },
        )
        payload = json.loads(body)

        self.assertEqual(status, 400)
        self.assertFalse(payload["ok"])
        self.assertIn("enter an existing Codex thread ID", payload["message"])

    def test_api_track_open_rejects_duplicate_thread_selection(self):
        pr_review_coordinator.ensure_repo_name = lambda repo_root, repo_name: ("owner", "repo")
        pr_review_coordinator.resolve_selected_thread = lambda repo_root, provider, requested_thread_id, prefer_latest_when_empty=False: {
            "id": "thread-shared",
            "title": "Thread",
        }
        pr_review_coordinator.assert_thread_available = lambda thread_id, key: None

        status, body = self.request(
            "POST",
            "/api/actions/track-open",
            data={
                "project_root": "/tmp/repo",
                "repo_name": "repo",
                "provider": "codex",
                "selected_pr": ["42", "43"],
                "branch_42": "feature/one",
                "thread_strategy_42": "specific_thread",
                "thread_id_42": "thread-shared",
                "branch_43": "feature/two",
                "thread_strategy_43": "specific_thread",
                "thread_id_43": "thread-shared",
            },
        )
        payload = json.loads(body)

        self.assertEqual(status, 400)
        self.assertFalse(payload["ok"])
        self.assertIn("must be distinct", payload["message"])

    def test_api_retarget_thread_returns_accepted_json(self):
        self.add_record()
        pr_review_coordinator.resolve_selected_thread = lambda repo_root, provider, requested_thread_id, prefer_latest_when_empty=False: {
            "id": requested_thread_id or "thread-latest",
            "title": "Thread",
        }
        pr_review_coordinator.assert_thread_available = lambda thread_id, key: None

        status, body = self.request(
            "POST",
            "/api/actions/retarget-thread",
            data={"key": "repo-pr-42", "thread_id": "thread-new"},
        )
        payload = json.loads(body)

        self.assertEqual(status, 202)
        self.assertTrue(payload["ok"])
        self.assertIn("Queued thread update", payload["message"])
        job = pr_review_coordinator.get_job(payload["job_id"])
        self.assertEqual(job.action, "retarget-thread")


class ThreadSelectionTests(unittest.TestCase):
    def test_new_thread_sentinel_creates_fresh_codex_thread(self):
        original_create_codex_thread = pr_review_coordinator.create_codex_thread
        try:
            pr_review_coordinator.create_codex_thread = lambda repo_root: {"id": "thread-fresh", "title": "Fresh thread"}

            result = pr_review_coordinator.resolve_selected_thread(
                "/tmp/repo",
                "codex",
                pr_review_coordinator.NEW_CODEX_THREAD_SENTINEL,
            )

            self.assertEqual(result["id"], "thread-fresh")
            self.assertEqual(result["title"], "Fresh thread")
        finally:
            pr_review_coordinator.create_codex_thread = original_create_codex_thread

    def test_explicit_thread_id_does_not_fallback_to_latest_repo_thread(self):
        original_state_db = pr_review_coordinator.CODEX_STATE_DB
        try:
            with tempfile.TemporaryDirectory() as tmp:
                state_db = Path(tmp) / "state.sqlite"
                pr_review_coordinator.CODEX_STATE_DB = state_db
                connection = sqlite3.connect(state_db)
                try:
                    connection.execute(
                        """
                        CREATE TABLE threads (
                            id TEXT PRIMARY KEY,
                            rollout_path TEXT NOT NULL,
                            created_at INTEGER NOT NULL,
                            updated_at INTEGER NOT NULL,
                            source TEXT NOT NULL,
                            model_provider TEXT NOT NULL,
                            cwd TEXT NOT NULL,
                            title TEXT NOT NULL,
                            sandbox_policy TEXT NOT NULL,
                            approval_mode TEXT NOT NULL,
                            tokens_used INTEGER NOT NULL DEFAULT 0,
                            has_user_event INTEGER NOT NULL DEFAULT 0,
                            archived INTEGER NOT NULL DEFAULT 0,
                            archived_at INTEGER,
                            git_sha TEXT,
                            git_branch TEXT,
                            git_origin_url TEXT,
                            cli_version TEXT NOT NULL DEFAULT '',
                            first_user_message TEXT NOT NULL DEFAULT '',
                            agent_nickname TEXT,
                            agent_role TEXT,
                            memory_mode TEXT NOT NULL DEFAULT 'enabled'
                        )
                        """
                    )
                    connection.execute(
                        """
                        INSERT INTO threads (
                            id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
                            sandbox_policy, approval_mode, tokens_used, has_user_event, archived, cli_version,
                            first_user_message, memory_mode
                        ) VALUES (?, '', 0, 100, 'codex', 'openai', ?, ?, 'read-only', 'never', 0, 0, 0, '', '', 'enabled')
                        """,
                        ("thread-latest", "/tmp/repo", "Latest repo thread"),
                    )
                    connection.commit()
                finally:
                    connection.close()

                result = pr_review_coordinator.resolve_thread("/tmp/repo", "thread-explicit", provider="codex")

                self.assertEqual(result["id"], "thread-explicit")
                self.assertIsNone(result["title"])
        finally:
            pr_review_coordinator.CODEX_STATE_DB = original_state_db


class DashboardRenderingTests(unittest.TestCase):
    def make_record(self, **overrides):
        payload = {
            "key": "repo-42",
            "repo_root": "/tmp/repo",
            "repo_owner": "owner",
            "repo_name": "repo",
            "pr_number": 42,
            "pr_url": "https://example.com/pr/42",
            "pr_title": "Example PR",
            "pr_state": "OPEN",
            "branch": "feature/example",
            "base_branch": "main",
            "worktree_path": "/tmp/worktree",
            "worktree_managed": 1,
            "thread_id": "thread-42",
            "thread_title": "Original bug report and context for the thread",
            "status": "needs_review",
            "active": 1,
            "last_review_signature": None,
            "last_handled_signature": None,
            "last_review_status": "needs_review",
            "last_review_comment_at": None,
            "pending_copilot_review": 0,
            "unresolved_thread_count": 1,
            "actionable_comment_count": 0,
            "failing_check_count": 0,
            "unresolved_threads_json": "[]",
            "actionable_comments_json": "[]",
            "failing_checks_json": "[]",
            "ci_summary": None,
            "run_state": None,
            "run_reason": None,
            "current_job_id": None,
            "lock_started_at": None,
            "lock_owner_pid": None,
            "last_polled_at": None,
            "last_prompted_at": None,
            "last_run_started_at": None,
            "last_run_finished_at": None,
            "last_run_status": "ready",
            "last_run_summary": "Idle",
            "last_error": None,
            "provider": "codex",
            "created_at": 0,
            "updated_at": 0,
        }
        payload.update(overrides)
        return pr_review_coordinator.TrackedPR(**payload)

    def test_update_live_activity_from_codex_event_tracks_headline_and_files(self):
        activity = pr_review_coordinator.empty_live_activity()
        stream_state = {"message": "", "plan": "", "reasoning": ""}

        changed = pr_review_coordinator.update_live_activity_from_codex_event(
            activity,
            {"type": "agent_message_delta", "delta": "Investigating module resolution."},
            stream_state,
        )
        self.assertTrue(changed)
        self.assertEqual(activity["headline"], "Investigating module resolution.")

        changed = pr_review_coordinator.update_live_activity_from_codex_event(
            activity,
            {
                "type": "patch_apply_begin",
                "changes": {
                    "src/graph.ts": {"type": "add", "unified_diff": "@@ -0,0 +1,2 @@\n+one\n+two\n"},
                },
            },
            stream_state,
        )
        self.assertTrue(changed)
        self.assertEqual(activity["items"][0]["kind"], "file")
        self.assertIn("Created src/graph.ts +2 -0", activity["items"][0]["text"])

    def test_update_live_activity_from_current_codex_item_events(self):
        activity = pr_review_coordinator.empty_live_activity()
        stream_state = {"message": "", "plan": "", "reasoning": ""}

        changed = pr_review_coordinator.update_live_activity_from_codex_event(
            activity,
            {
                "type": "item.completed",
                "item": {
                    "id": "item_0",
                    "type": "agent_message",
                    "text": "I'm checking repo-local instructions before editing.",
                },
            },
            stream_state,
        )
        self.assertTrue(changed)
        self.assertEqual(activity["headline"], "I'm checking repo-local instructions before editing.")

        changed = pr_review_coordinator.update_live_activity_from_codex_event(
            activity,
            {
                "type": "item.started",
                "item": {
                    "id": "item_1",
                    "type": "command_execution",
                    "command": "/bin/zsh -lc pwd",
                    "status": "in_progress",
                },
            },
            stream_state,
        )
        self.assertTrue(changed)
        self.assertEqual(activity["items"][0]["kind"], "command")
        self.assertEqual(activity["items"][0]["text"], "Running /bin/zsh -lc pwd")

        changed = pr_review_coordinator.update_live_activity_from_codex_event(
            activity,
            {
                "type": "item.completed",
                "item": {
                    "id": "item_2",
                    "type": "file_change",
                    "changes": [{"path": "/tmp/hello.txt", "kind": "add"}],
                    "status": "completed",
                },
            },
            stream_state,
        )
        self.assertTrue(changed)
        self.assertEqual(activity["items"][1]["kind"], "file")
        self.assertEqual(activity["items"][1]["text"], "Created /tmp/hello.txt")

    def test_update_live_activity_from_app_server_item_events(self):
        activity = pr_review_coordinator.empty_live_activity()
        stream_state = {"message": "", "plan": "", "reasoning": ""}

        event = pr_review_coordinator.codex_app_server_notification_to_event(
            {
                "method": "item/completed",
                "params": {
                    "item": {
                        "id": "item_0",
                        "type": "agentMessage",
                        "text": "App-server turn is visible in the thread.",
                    }
                },
            }
        )

        self.assertIsNotNone(event)
        changed = pr_review_coordinator.update_live_activity_from_codex_event(activity, event, stream_state)

        self.assertTrue(changed)
        self.assertEqual(activity["headline"], "App-server turn is visible in the thread.")

    def test_serialize_dashboard_record_prefers_compact_codex_summary_line(self):
        record = self.make_record(
            run_state="running",
            last_run_status="running",
            last_run_summary="Launching codex follow-up",
            live_activity_json=json.dumps(
                {
                    "headline": "Tracing support-ticket parsing",
                    "items": [
                        {"kind": "command", "text": "Running rg -n supportTicket src"},
                        {"kind": "file", "text": "Updated src/routes/support_ticket.ts"},
                    ],
                }
            ),
            live_activity_updated_at=123456,
        )

        payload = pr_review_coordinator.serialize_dashboard_record(record)

        self.assertEqual(payload["run_summary_line"], "Codex running: Tracing support-ticket parsing")
        self.assertEqual(payload["live_activity_updated_label"], pr_review_coordinator.format_timestamp(123456))
        self.assertEqual(payload["run_detail_meta"], f"2 update(s) | latest activity {payload['live_activity_updated_label']}")
        self.assertTrue(payload["has_run_details"])

    def test_serialize_dashboard_record_falls_back_to_last_run_summary_without_live_activity(self):
        record = self.make_record(
            provider="cursor",
            run_state="busy",
            last_run_status="busy",
            last_run_summary="Worktree has local changes",
            live_activity_json=None,
            live_activity_updated_at=None,
        )

        payload = pr_review_coordinator.serialize_dashboard_record(record)

        self.assertEqual(payload["run_summary_line"], "Worktree has local changes")
        self.assertEqual(payload["run_detail_meta"], "")
        self.assertFalse(payload["has_run_details"])
        self.assertFalse(payload["dirty_worktree_busy"])

    def test_serialize_dashboard_record_marks_dirty_worktree_busy(self):
        record = self.make_record(
            run_state="busy",
            last_run_status="busy",
            last_run_summary="Worktree has local changes; treating this PR as busy: /tmp/worktree",
            live_activity_json=None,
            live_activity_updated_at=None,
        )

        payload = pr_review_coordinator.serialize_dashboard_record(record)

        self.assertTrue(payload["dirty_worktree_busy"])

    def test_serialize_dashboard_record_exposes_stop_for_live_agent_process(self):
        record = self.make_record(run_state="running", last_run_status="running")
        with tempfile.TemporaryDirectory() as tmp:
            original_locks_dir = pr_review_coordinator.LOCKS_DIR
            pr_review_coordinator.LOCKS_DIR = Path(tmp)
            try:
                pr_review_coordinator.LOCKS_DIR.mkdir(parents=True, exist_ok=True)
                pr_review_coordinator.lock_path(record.key).write_text(
                    json.dumps({"pid": 111, "agent_pid": 222, "agent_pgid": 222}),
                    encoding="utf-8",
                )
                with mock.patch("pr_review_coordinator.pid_is_alive", return_value=True):
                    payload = pr_review_coordinator.serialize_dashboard_record(record)
            finally:
                pr_review_coordinator.LOCKS_DIR = original_locks_dir

        self.assertTrue(payload["stop_available"])


class WorktreeCleanlinessTests(unittest.TestCase):
    def test_git_status_is_clean_ignores_root_node_modules_symlink(self):
        with tempfile.TemporaryDirectory() as tmp:
            worktree = Path(tmp) / "worktree"
            repo_root = Path(tmp) / "repo"
            worktree.mkdir()
            repo_root.mkdir()
            (repo_root / "node_modules").mkdir()
            (worktree / "node_modules").symlink_to(repo_root / "node_modules")

            with mock.patch.object(
                pr_review_common,
                "run",
                return_value=mock.Mock(stdout="?? node_modules\n"),
            ):
                self.assertTrue(pr_review_common.git_status_is_clean(worktree))

    def test_git_status_is_clean_keeps_real_node_modules_dirty(self):
        with tempfile.TemporaryDirectory() as tmp:
            worktree = Path(tmp) / "worktree"
            worktree.mkdir()
            (worktree / "node_modules").mkdir()

            with mock.patch.object(
                pr_review_common,
                "run",
                return_value=mock.Mock(stdout="?? node_modules\n"),
            ):
                self.assertFalse(pr_review_common.git_status_is_clean(worktree))

    def test_sync_worktree_to_remote_recreates_node_modules_symlink_after_clean(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp) / "repo"
            worktree = Path(tmp) / "worktree"
            repo_root.mkdir()
            worktree.mkdir()
            (repo_root / "node_modules").mkdir()
            (worktree / "package.json").write_text("{}", encoding="utf-8")
            node_modules_link = worktree / "node_modules"
            node_modules_link.symlink_to(repo_root / "node_modules")
            resolved_repo_root = repo_root.resolve()
            resolved_worktree = worktree.resolve()
            repo_root_variants = {str(repo_root), str(resolved_repo_root)}
            worktree_variants = {str(worktree), str(resolved_worktree)}

            def fake_run(cmd, *, cwd=None, check=True, capture_output=True):
                git_cwd = cmd[2]
                git_action = cmd[3:]

                if git_cwd in worktree_variants and git_action[:1] == ["status"]:
                    return mock.Mock(stdout="?? node_modules\n")
                if git_cwd in repo_root_variants and git_action[:1] == ["fetch"]:
                    return mock.Mock(stdout="")
                if git_cwd in repo_root_variants and git_action == ["rev-parse", "origin/feature/test"]:
                    return mock.Mock(stdout="remote-head\n")
                if git_cwd in worktree_variants and git_action == ["rev-parse", "HEAD"]:
                    return mock.Mock(stdout="local-head\n")
                if git_cwd in worktree_variants and git_action[:2] == ["reset", "--hard"]:
                    return mock.Mock(stdout="")
                if git_cwd in worktree_variants and git_action == ["clean", "-fd"]:
                    node_modules_link.unlink()
                    return mock.Mock(stdout="")
                raise AssertionError(f"unexpected command: {cmd}")

            with mock.patch.object(pr_review_common, "run", side_effect=fake_run):
                result = pr_review_common.sync_worktree_to_remote(repo_root, "feature/test", worktree)

            self.assertEqual(result["status"], "ready")
            self.assertTrue(node_modules_link.is_symlink())
            self.assertEqual(node_modules_link.resolve(), (repo_root / "node_modules").resolve())


class ThreadPolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_var_dir = pr_review_coordinator.VAR_DIR
        self.original_locks_dir = pr_review_coordinator.LOCKS_DIR
        self.original_db = pr_review_coordinator.COORDINATOR_DB
        self.original_state_db = pr_review_coordinator.CODEX_STATE_DB
        pr_review_coordinator.VAR_DIR = Path(self.temp_dir.name)
        pr_review_coordinator.LOCKS_DIR = pr_review_coordinator.VAR_DIR / "locks"
        pr_review_coordinator.COORDINATOR_DB = pr_review_coordinator.VAR_DIR / "test.db"
        pr_review_coordinator.CODEX_STATE_DB = Path(self.temp_dir.name) / "state.sqlite"

    def tearDown(self):
        pr_review_coordinator.VAR_DIR = self.original_var_dir
        pr_review_coordinator.LOCKS_DIR = self.original_locks_dir
        pr_review_coordinator.COORDINATOR_DB = self.original_db
        pr_review_coordinator.CODEX_STATE_DB = self.original_state_db
        self.temp_dir.cleanup()

    def add_record(self, *, key, thread_id, active):
        pr_review_coordinator.upsert_tracked_pr(
            {
                "key": key,
                "repo_root": "/tmp/repo",
                "repo_owner": "owner",
                "repo_name": "repo",
                "pr_number": 1 if key.endswith("1") else 2,
                "pr_url": f"https://example.com/{key}",
                "pr_title": key,
                "pr_state": "OPEN",
                "branch": key,
                "base_branch": "main",
                "worktree_path": f"/tmp/{key}",
                "worktree_managed": 1,
                "thread_id": thread_id,
                "thread_title": "Thread",
                "status": "awaiting_final_test",
                "active": 1 if active else 0,
                "last_review_signature": None,
                "last_handled_signature": None,
                "last_review_status": "awaiting_final_test",
                "last_review_comment_at": None,
                "pending_copilot_review": 0,
                "unresolved_thread_count": 0,
                "actionable_comment_count": 0,
                "failing_check_count": 0,
                "unresolved_threads_json": "[]",
                "actionable_comments_json": "[]",
                "failing_checks_json": "[]",
                "ci_summary": None,
                "run_state": None,
                "run_reason": None,
                "current_job_id": None,
                "lock_started_at": None,
                "lock_owner_pid": None,
                "last_polled_at": None,
                "last_prompted_at": None,
                "last_run_started_at": None,
                "last_run_finished_at": None,
                "last_run_status": "registered",
                "last_run_summary": "registered",
                "last_error": None,
                "provider": "codex",
            }
        )

    def test_rejects_reuse_for_another_active_pr(self):
        self.add_record(key="repo-pr-1", thread_id="thread-1", active=True)
        with self.assertRaises(pr_review_common.ScriptError):
            pr_review_coordinator.assert_thread_available("thread-1", "repo-pr-2")

    def test_allows_reuse_after_prior_pr_is_inactive(self):
        self.add_record(key="repo-pr-1", thread_id="thread-1", active=False)
        pr_review_coordinator.assert_thread_available("thread-1", "repo-pr-2")

    def test_recent_threads_include_in_use_annotations(self):
        self.add_record(key="repo-pr-1", thread_id="thread-1", active=True)
        connection = sqlite3.connect(pr_review_coordinator.CODEX_STATE_DB)
        try:
            connection.execute(
                """
                CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    cwd TEXT NOT NULL,
                    title TEXT NOT NULL,
                    archived INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            connection.execute(
                "INSERT INTO threads (id, cwd, title, archived, updated_at) VALUES (?, ?, ?, 0, ?)",
                ("thread-1", "/tmp/repo", "Thread One", 200),
            )
            connection.execute(
                "INSERT INTO threads (id, cwd, title, archived, updated_at) VALUES (?, ?, ?, 0, ?)",
                ("thread-2", "/tmp/repo", "Thread Two", 100),
            )
            connection.commit()
        finally:
            connection.close()

        result = pr_review_coordinator.list_recent_threads_for_repo("/tmp/repo")

        self.assertEqual([item["id"] for item in result], ["thread-1", "thread-2"])
        self.assertEqual(result[0]["in_use_by"], "repo #1")
        self.assertTrue(result[0]["conflict"])
        self.assertIsNone(result[1]["in_use_by"])


class RefreshRecordStateTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_var_dir = pr_review_coordinator.VAR_DIR
        self.original_locks_dir = pr_review_coordinator.LOCKS_DIR
        self.original_db = pr_review_coordinator.COORDINATOR_DB
        pr_review_coordinator.VAR_DIR = Path(self.temp_dir.name)
        pr_review_coordinator.LOCKS_DIR = pr_review_coordinator.VAR_DIR / "locks"
        pr_review_coordinator.COORDINATOR_DB = pr_review_coordinator.VAR_DIR / "test.db"

    def tearDown(self):
        pr_review_coordinator.VAR_DIR = self.original_var_dir
        pr_review_coordinator.LOCKS_DIR = self.original_locks_dir
        pr_review_coordinator.COORDINATOR_DB = self.original_db
        self.temp_dir.cleanup()

    def add_record(self, **overrides):
        payload = {
            "key": "repo-pr-42",
            "repo_root": "/tmp/repo",
            "repo_owner": "owner",
            "repo_name": "repo",
            "pr_number": 42,
            "pr_url": "https://example.com/pr/42",
            "pr_title": "Example PR",
            "pr_state": "OPEN",
            "branch": "feature/example",
            "base_branch": "main",
            "worktree_path": "/tmp/worktree",
            "worktree_managed": 1,
            "thread_id": "thread-42",
            "thread_title": "Thread",
            "status": "needs_review",
            "active": 1,
            "last_review_signature": "sig-1",
            "last_handled_signature": None,
            "last_review_status": "needs_review",
            "last_review_comment_at": None,
            "pending_copilot_review": 0,
            "unresolved_thread_count": 1,
            "actionable_comment_count": 0,
            "failing_check_count": 0,
            "unresolved_threads_json": "[]",
            "actionable_comments_json": "[]",
            "failing_checks_json": "[]",
            "ci_summary": None,
            "run_state": "running",
            "run_reason": "codex",
            "current_job_id": 100,
            "lock_started_at": 111,
            "lock_owner_pid": 222,
            "last_polled_at": None,
            "last_prompted_at": None,
            "last_run_started_at": 111,
            "last_run_finished_at": None,
            "last_run_status": "running",
            "last_run_summary": "Launching codex follow-up",
            "last_error": None,
            "provider": "codex",
            "live_activity_json": json.dumps({"headline": "Launching codex follow-up", "items": []}),
            "live_activity_updated_at": 111,
        }
        payload.update(overrides)
        return pr_review_coordinator.upsert_tracked_pr(payload)

    def snapshot(self):
        return {
            "status": "needs_review",
            "signature": "sig-2",
            "latest_comment_at": None,
            "pending_copilot_review": False,
            "unresolved_threads": [],
            "actionable_pr_comments": [],
            "failing_checks": [],
            "pr": {"state": "OPEN", "title": "Example PR", "url": "https://example.com/pr/42"},
        }

    def test_foreign_job_does_not_overwrite_active_run_state(self):
        stale_record = self.add_record(current_job_id=100)

        updated = pr_review_coordinator.refresh_record_state(
            stale_record,
            self.snapshot(),
            run_status="busy",
            run_summary="Worktree has local changes",
            finished=True,
            job_id=200,
        )

        self.assertEqual(updated.run_state, "running")
        self.assertEqual(updated.current_job_id, 100)
        self.assertEqual(updated.last_run_summary, "Launching codex follow-up")
        self.assertIsNotNone(updated.live_activity_json)

    def test_current_job_finish_clears_live_activity(self):
        record = self.add_record(current_job_id=100)

        updated = pr_review_coordinator.refresh_record_state(
            record,
            self.snapshot(),
            run_status="busy",
            run_summary="Worktree has local changes",
            finished=True,
            job_id=100,
        )

        self.assertIsNone(updated.run_state)
        self.assertIsNone(updated.current_job_id)
        self.assertEqual(updated.last_run_summary, "Worktree has local changes")
        self.assertIsNone(updated.live_activity_json)

    def test_run_codex_resume_uses_app_server_socket_when_available(self):
        record = self.add_record(current_job_id=100)
        pr_review_coordinator.LOCKS_DIR.mkdir(parents=True, exist_ok=True)
        pr_review_coordinator.lock_path(record.key).write_text(
            json.dumps({"pid": 111, "thread_id": record.thread_id}),
            encoding="utf-8",
        )
        instances = []

        class FakeClient:
            def __init__(self, codex_bin, socket_path):
                self.pid = 333
                self.stderr_lines = []
                self.requests = []
                self.messages = [
                    {
                        "method": "item/agentMessage/delta",
                        "params": {"threadId": "thread-42", "turnId": "turn-1", "itemId": "item-1", "delta": "Done."},
                    },
                    {"method": "turn/completed", "params": {"threadId": "thread-42", "turn": {"id": "turn-1", "status": "completed"}}},
                ]
                instances.append(self)

            def request(self, method, params):
                self.requests.append((method, params))
                return {"turn": {"id": "turn-1"}} if method == "turn/start" else {}

            def read_message(self):
                return self.messages.pop(0) if self.messages else None

            def close(self):
                pass

        with mock.patch("pr_review_coordinator.resolve_codex_app_server_socket", return_value=None):
            with mock.patch("pr_review_coordinator.resolve_codex_executable", return_value="codex"):
                with mock.patch("pr_review_coordinator.CodexAppServerClient", FakeClient):
                    result = pr_review_coordinator.run_codex_resume(record, self.snapshot(), dry_run=False)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["last_message"], "Done.")
        self.assertEqual([method for method, _ in instances[0].requests], ["initialize", "thread/resume", "turn/start"])
        lock = json.loads(pr_review_coordinator.lock_path(record.key).read_text(encoding="utf-8"))
        self.assertEqual(lock["agent_transport"], "app-server-stdio")
        self.assertEqual(lock["agent_turn_id"], "turn-1")

    def test_stop_active_run_targets_agent_process_group(self):
        record = self.add_record(current_job_id=100, lock_owner_pid=111)
        pr_review_coordinator.LOCKS_DIR.mkdir(parents=True, exist_ok=True)
        pr_review_coordinator.lock_path(record.key).write_text(
            json.dumps({"pid": 111, "agent_pid": 222, "agent_pgid": 222}),
            encoding="utf-8",
        )

        with mock.patch("pr_review_coordinator.pid_is_alive", return_value=True):
            with mock.patch("pr_review_coordinator.terminate_process_group") as terminate:
                result = pr_review_coordinator.stop_active_run(record)

        terminate.assert_called_once_with(222, 222)
        self.assertTrue(result["ok"])
        updated = pr_review_coordinator.get_tracked_pr(record.key)
        self.assertEqual(updated.last_run_status, "stopping")
        self.assertIn("Stop requested", updated.last_run_summary)

    def test_stop_active_run_interrupts_app_server_turn(self):
        record = self.add_record(current_job_id=100, lock_owner_pid=111)
        pr_review_coordinator.LOCKS_DIR.mkdir(parents=True, exist_ok=True)
        pr_review_coordinator.lock_path(record.key).write_text(
            json.dumps(
                {
                    "pid": 111,
                    "agent_pid": 222,
                    "agent_pgid": 222,
                    "agent_transport": "app-server",
                    "app_server_socket": "/tmp/codex.sock",
                    "agent_turn_id": "turn-1",
                }
            ),
            encoding="utf-8",
        )

        with mock.patch("pr_review_coordinator.pid_is_alive", return_value=True):
            with mock.patch("pr_review_coordinator.interrupt_codex_app_server_turn") as interrupt:
                with mock.patch("pr_review_coordinator.terminate_process_group") as terminate:
                    result = pr_review_coordinator.stop_active_run(record)

        interrupt.assert_called_once_with("/tmp/codex.sock", "thread-42", "turn-1")
        terminate.assert_called_once_with(222, 222)
        self.assertTrue(result["ok"])
        updated = pr_review_coordinator.get_tracked_pr(record.key)
        self.assertEqual(updated.last_run_status, "stopping")
        self.assertIn("Codex app turn turn-1", updated.last_run_summary)


class RegisterTrackingTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_var_dir = pr_review_coordinator.VAR_DIR
        self.original_locks_dir = pr_review_coordinator.LOCKS_DIR
        self.original_db = pr_review_coordinator.COORDINATOR_DB
        pr_review_coordinator.VAR_DIR = Path(self.temp_dir.name)
        pr_review_coordinator.LOCKS_DIR = pr_review_coordinator.VAR_DIR / "locks"
        pr_review_coordinator.COORDINATOR_DB = pr_review_coordinator.VAR_DIR / "test.db"

        self.original_verify_gh_auth = pr_review_coordinator.verify_gh_auth
        self.original_ensure_repo_name = pr_review_coordinator.ensure_repo_name
        self.original_run = pr_review_coordinator.run
        self.original_resolve_thread = pr_review_coordinator.resolve_thread
        self.original_assert_thread_available = pr_review_coordinator.assert_thread_available
        self.original_ensure_worktree = pr_review_coordinator.ensure_worktree
        self.original_pull_request_snapshot = pr_review_coordinator.pull_request_snapshot
        self.original_record_event = pr_review_coordinator.record_event
        self.original_switch_repo_to_base_branch = pr_review_coordinator.switch_repo_to_base_branch

    def tearDown(self):
        pr_review_coordinator.VAR_DIR = self.original_var_dir
        pr_review_coordinator.LOCKS_DIR = self.original_locks_dir
        pr_review_coordinator.COORDINATOR_DB = self.original_db
        pr_review_coordinator.verify_gh_auth = self.original_verify_gh_auth
        pr_review_coordinator.ensure_repo_name = self.original_ensure_repo_name
        pr_review_coordinator.run = self.original_run
        pr_review_coordinator.resolve_thread = self.original_resolve_thread
        pr_review_coordinator.assert_thread_available = self.original_assert_thread_available
        pr_review_coordinator.ensure_worktree = self.original_ensure_worktree
        pr_review_coordinator.pull_request_snapshot = self.original_pull_request_snapshot
        pr_review_coordinator.record_event = self.original_record_event
        pr_review_coordinator.switch_repo_to_base_branch = self.original_switch_repo_to_base_branch
        self.temp_dir.cleanup()

    def test_register_tracking_stores_sibling_managed_worktree(self):
        captured: dict[str, object] = {}

        pr_review_coordinator.verify_gh_auth = lambda: None
        pr_review_coordinator.ensure_repo_name = lambda repo_root, repo_name: ("Starshipit-Product", "starshipit-wms")
        pr_review_coordinator.run = lambda cmd, cwd=None: type(
            "Result",
            (),
            {
                "stdout": '{"number": 418, "url": "https://example.com/pr/418", "title": "PR 418", "headRefName": "feat/putaway-split-lines-and-serial-scan", "baseRefName": "master", "state": "OPEN"}'
            },
        )()
        pr_review_coordinator.resolve_thread = lambda repo_root, thread_id, provider=None: {"id": "thread-418", "title": "PR 418 thread"}
        pr_review_coordinator.assert_thread_available = lambda thread_id, key: None
        pr_review_coordinator.switch_repo_to_base_branch = lambda repo_root, base_branch, feature_branch: {
            "status": "ready",
            "switched": True,
            "branch": base_branch,
        }

        def fake_ensure_worktree(repo_root, repo_name, pr_number, branch, worktree_root, *, layout):
            captured["repo_root"] = repo_root
            captured["repo_name"] = repo_name
            captured["pr_number"] = pr_number
            captured["branch"] = branch
            captured["worktree_root"] = worktree_root
            captured["layout"] = layout
            return {
                "status": "ready",
                "worktree": "/Users/jordan/source/starshipit-wms-pr-418",
                "created": True,
            }

        pr_review_coordinator.ensure_worktree = fake_ensure_worktree
        pr_review_coordinator.pull_request_snapshot = lambda repo_root, repo_name, pr_number: {
            "status": "needs_review",
            "pr": {
                "number": 418,
                "url": "https://example.com/pr/418",
                "title": "PR 418",
                "state": "OPEN",
            },
            "signature": "sig",
            "latest_comment_at": None,
            "pending_copilot_review": True,
            "unresolved_threads": [],
            "actionable_pr_comments": [],
            "failing_checks": [],
            "ci_summary": None,
            "unresolved_thread_count": 0,
            "actionable_comment_count": 0,
            "failing_check_count": 0,
        }
        pr_review_coordinator.record_event = lambda *args, **kwargs: None

        result = pr_review_coordinator.register_tracking(
            repo_root="/Users/jordan/source/starshipit-wms",
            repo_name="starshipit-wms",
            pr_number=418,
            branch="feat/putaway-split-lines-and-serial-scan",
            worktree_root="/Users/jordan/source",
            worktree_path=None,
            thread_id="thread-418",
            worktree_layout="sibling",
        )

        self.assertEqual(captured["layout"], "sibling")
        self.assertEqual(result["tracked_pr"]["worktree_path"], "/Users/jordan/source/starshipit-wms-pr-418")
        self.assertEqual(result["tracked_pr"]["worktree_managed"], True)
        self.assertEqual(result["tracked_pr"]["worktree_root"], "/Users/jordan/source")
        self.assertEqual(result["tracked_pr"]["worktree_layout"], "sibling")


class QueueBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_var_dir = pr_review_coordinator.VAR_DIR
        self.original_locks_dir = pr_review_coordinator.LOCKS_DIR
        self.original_db = pr_review_coordinator.COORDINATOR_DB
        self.original_record_event = pr_review_coordinator.record_event
        self.original_pull_request_snapshot = pr_review_coordinator.pull_request_snapshot
        self.original_run_follow_up = pr_review_coordinator.run_follow_up
        self.original_request_copilot_review = pr_review_coordinator.request_copilot_review
        self.original_register_tracking = pr_review_coordinator.register_tracking
        self.original_resolve_selected_thread = pr_review_coordinator.resolve_selected_thread
        self.original_create_codex_thread = pr_review_coordinator.create_codex_thread
        pr_review_coordinator.VAR_DIR = Path(self.temp_dir.name)
        pr_review_coordinator.LOCKS_DIR = pr_review_coordinator.VAR_DIR / "locks"
        pr_review_coordinator.COORDINATOR_DB = pr_review_coordinator.VAR_DIR / "test.db"
        pr_review_coordinator.record_event = lambda *args, **kwargs: None

        for key, number in (("repo-pr-1", 1), ("repo-pr-2", 2)):
            pr_review_coordinator.upsert_tracked_pr(
                {
                    "key": key,
                    "repo_root": "/tmp/repo",
                    "repo_owner": "owner",
                    "repo_name": "repo",
                    "pr_number": number,
                    "pr_url": f"https://example.com/pr/{number}",
                    "pr_title": f"PR {number}",
                    "pr_state": "OPEN",
                    "branch": f"branch-{number}",
                    "base_branch": "main",
                    "worktree_path": f"/tmp/worktree-{number}",
                    "worktree_managed": 1,
                    "thread_id": f"thread-{number}",
                    "thread_title": f"Thread {number}",
                    "status": "awaiting_final_test",
                    "active": 1,
                    "last_review_signature": None,
                    "last_handled_signature": None,
                    "last_review_status": "awaiting_final_test",
                    "last_review_comment_at": None,
                    "pending_copilot_review": 0,
                    "unresolved_thread_count": 0,
                    "actionable_comment_count": 0,
                    "failing_check_count": 0,
                    "unresolved_threads_json": "[]",
                    "actionable_comments_json": "[]",
                    "failing_checks_json": "[]",
                    "ci_summary": None,
                    "run_state": None,
                    "run_reason": None,
                    "current_job_id": None,
                    "lock_started_at": None,
                    "lock_owner_pid": None,
                    "last_polled_at": None,
                    "last_prompted_at": None,
                    "last_run_started_at": None,
                    "last_run_finished_at": None,
                    "last_run_status": "registered",
                    "last_run_summary": "registered",
                    "last_error": None,
                    "provider": "codex",
                }
            )

    def tearDown(self):
        pr_review_coordinator.VAR_DIR = self.original_var_dir
        pr_review_coordinator.LOCKS_DIR = self.original_locks_dir
        pr_review_coordinator.COORDINATOR_DB = self.original_db
        pr_review_coordinator.record_event = self.original_record_event
        pr_review_coordinator.pull_request_snapshot = self.original_pull_request_snapshot
        pr_review_coordinator.run_follow_up = self.original_run_follow_up
        pr_review_coordinator.request_copilot_review = self.original_request_copilot_review
        pr_review_coordinator.register_tracking = self.original_register_tracking
        pr_review_coordinator.resolve_selected_thread = self.original_resolve_selected_thread
        pr_review_coordinator.create_codex_thread = self.original_create_codex_thread
        self.temp_dir.cleanup()

    def snapshot(
        self,
        status="needs_review",
        signature="sig-1",
        *,
        pending_copilot_review=False,
        copilot_review_error=None,
        final_copilot_review=False,
        latest_copilot_activity=None,
    ):
        return {
            "status": status,
            "pr": {
                "number": 1,
                "url": "https://example.com/pr/1",
                "title": "PR 1",
                "state": "OPEN",
            },
            "signature": signature,
            "latest_comment_at": "2026-03-09T00:00:00Z",
            "pending_copilot_review": pending_copilot_review,
            "copilot_review_error": copilot_review_error,
            "latest_copilot_activity": latest_copilot_activity,
            "final_copilot_review": final_copilot_review,
            "unresolved_threads": [{"id": "thread-1"}] if status == "needs_review" else [],
            "actionable_pr_comments": [],
            "failing_checks": [],
        }

    def test_poll_record_queues_follow_up_instead_of_running_it_inline(self):
        pr_review_coordinator.pull_request_snapshot = lambda *args, **kwargs: self.snapshot()
        record = pr_review_coordinator.get_tracked_pr("repo-pr-1")

        result = pr_review_coordinator.poll_record(record, dry_run=False, force_run=False, job_id=101)

        self.assertEqual(result["status"], "queued")
        pending = pr_review_coordinator.list_pending_jobs()
        self.assertEqual([job.action for job in pending], ["run-one"])
        self.assertEqual(json.loads(pending[0].payload_json)["signature"], "sig-1")

    def test_clean_pr_after_agent_run_becomes_awaiting_final_review(self):
        pr_review_coordinator.pull_request_snapshot = lambda *args, **kwargs: self.snapshot(
            status="awaiting_final_test",
            signature="sig-clean",
            final_copilot_review=True,
        )
        pr_review_coordinator.update_tracked_pr(
            "repo-pr-1",
            last_prompted_at=1,
            last_handled_signature="sig-needs-review",
            last_run_status="ok",
        )
        record = pr_review_coordinator.get_tracked_pr("repo-pr-1")

        result = pr_review_coordinator.poll_record(record, dry_run=False, force_run=False, job_id=102)

        self.assertEqual(result["status"], "idle")
        self.assertEqual(result["tracked_pr"]["status"], "awaiting_final_review")
        self.assertEqual(result["tracked_pr"]["last_run_summary"], "PR is awaiting final review")

    def test_clean_pr_after_agent_run_does_not_emit_fake_state_transition(self):
        events = []
        pr_review_coordinator.record_event = lambda level, event_type, message, **kwargs: events.append(
            {"level": level, "event_type": event_type, "message": message, "details": kwargs.get("details")}
        )
        pr_review_coordinator.pull_request_snapshot = lambda *args, **kwargs: self.snapshot(
            status="awaiting_final_test",
            signature="sig-clean",
            final_copilot_review=True,
        )
        pr_review_coordinator.update_tracked_pr(
            "repo-pr-1",
            status="awaiting_final_review",
            last_prompted_at=1,
            last_handled_signature="sig-needs-review",
            last_run_status="ok",
        )
        record = pr_review_coordinator.get_tracked_pr("repo-pr-1")

        result = pr_review_coordinator.poll_record(record, dry_run=False, force_run=False, job_id=102)

        self.assertEqual(result["status"], "idle")
        self.assertEqual(
            [event for event in events if event["event_type"] == "state_transition"],
            [],
        )

    def test_clean_pr_after_agent_run_requests_final_copilot_review_before_final_state(self):
        snapshots = iter(
            [
                self.snapshot(
                    status="awaiting_final_test",
                    signature="sig-clean",
                    final_copilot_review=False,
                ),
                self.snapshot(
                    status="pending_copilot_review",
                    signature="sig-pending",
                    pending_copilot_review=True,
                ),
            ]
        )
        pr_review_coordinator.pull_request_snapshot = lambda *args, **kwargs: next(snapshots)
        pr_review_coordinator.update_tracked_pr(
            "repo-pr-1",
            last_prompted_at=1,
            last_handled_signature="sig-needs-review",
            last_run_status="ok",
        )
        record = pr_review_coordinator.get_tracked_pr("repo-pr-1")
        captured: dict[str, object] = {}

        def fake_request(record):
            captured["pr_number"] = record.pr_number
            return {"status": "ready", "reviewer": "copilot-pull-request-reviewer"}

        pr_review_coordinator.request_copilot_review = fake_request

        result = pr_review_coordinator.poll_record(record, dry_run=False, force_run=False, job_id=102)

        self.assertEqual(result["status"], "idle")
        self.assertEqual(captured["pr_number"], 1)
        self.assertEqual(result["tracked_pr"]["status"], "pending_copilot_review")
        self.assertEqual(result["tracked_pr"]["last_run_summary"], "Requested final Copilot no-comments review")
        self.assertEqual(result["request"]["status"], "ready")

    def test_clean_pr_after_recent_final_review_request_waits_without_rerequesting(self):
        pr_review_coordinator.pull_request_snapshot = lambda *args, **kwargs: self.snapshot(
            status="awaiting_final_test",
            signature="sig-clean",
            final_copilot_review=False,
        )
        requested_at = pr_review_coordinator.parse_github_timestamp_ms("2026-03-09T00:00:00Z")
        pr_review_coordinator.update_tracked_pr(
            "repo-pr-1",
            last_prompted_at=1,
            last_handled_signature="sig-needs-review",
            last_run_status="ok",
            last_copilot_rerequested_at=requested_at,
        )
        record = pr_review_coordinator.get_tracked_pr("repo-pr-1")

        def fail_request(record):
            raise AssertionError("Copilot review should not be re-requested during cooldown")

        pr_review_coordinator.request_copilot_review = fail_request

        with mock.patch.object(
            pr_review_coordinator,
            "now_ms",
            return_value=pr_review_coordinator.parse_github_timestamp_ms("2026-03-09T00:10:00Z"),
        ):
            result = pr_review_coordinator.poll_record(record, dry_run=False, force_run=False, job_id=102)

        self.assertEqual(result["status"], "idle")
        self.assertEqual(result["tracked_pr"]["status"], "awaiting_final_test")
        self.assertIn("Waiting for final Copilot no-comments review", result["tracked_pr"]["last_run_summary"])

    def test_clean_pr_without_agent_run_stays_awaiting_final_test(self):
        pr_review_coordinator.pull_request_snapshot = lambda *args, **kwargs: self.snapshot(
            status="awaiting_final_test",
            signature="sig-clean",
        )
        record = pr_review_coordinator.get_tracked_pr("repo-pr-1")

        result = pr_review_coordinator.poll_record(record, dry_run=False, force_run=False, job_id=103)

        self.assertEqual(result["status"], "idle")
        self.assertEqual(result["tracked_pr"]["status"], "awaiting_final_test")
        self.assertEqual(result["tracked_pr"]["last_run_summary"], "PR is not currently actionable")

    def test_poll_record_treats_dirty_worktree_as_busy_without_queueing_follow_up(self):
        dirty_worktree = Path(self.temp_dir.name) / "dirty-worktree"
        dirty_worktree.mkdir()
        pr_review_coordinator.update_tracked_pr("repo-pr-1", worktree_path=str(dirty_worktree))
        pr_review_coordinator.pull_request_snapshot = lambda *args, **kwargs: self.snapshot()
        record = pr_review_coordinator.get_tracked_pr("repo-pr-1")

        with mock.patch.object(pr_review_coordinator, "git_status_is_clean", return_value=False):
            result = pr_review_coordinator.poll_record(record, dry_run=False, force_run=False, job_id=103)

        self.assertEqual(result["status"], "busy")
        self.assertEqual(result["tracked_pr"]["last_run_status"], "busy")
        self.assertEqual(pr_review_coordinator.list_pending_jobs(), [])

    def test_poll_record_skips_duplicate_follow_up_event_when_run_already_queued(self):
        events = []
        pr_review_coordinator.record_event = lambda level, event_type, message, **kwargs: events.append(
            {"level": level, "event_type": event_type, "message": message, "details": kwargs.get("details")}
        )
        pr_review_coordinator.pull_request_snapshot = lambda *args, **kwargs: self.snapshot(signature="sig-duplicate")
        pr_review_coordinator.enqueue_job(
            "run-one",
            tracked_pr_key="repo-pr-1",
            requested_by="test",
            payload={"force_run": False, "signature": "sig-duplicate"},
        )
        record = pr_review_coordinator.get_tracked_pr("repo-pr-1")

        result = pr_review_coordinator.poll_record(record, dry_run=False, force_run=False, job_id=104)

        self.assertEqual(result["status"], "queued")
        self.assertEqual(
            [event for event in events if event["event_type"] == "follow_up_queued"],
            [],
        )

    def test_poll_record_waits_during_copilot_review_cooldown(self):
        pr_review_coordinator.pull_request_snapshot = lambda *args, **kwargs: self.snapshot(
            status="copilot_review_cooldown",
            signature="sig-cooldown",
            copilot_review_error={
                "id": "review-1",
                "author": "github-copilot[bot]",
                "body": "Copilot encountered an error and was unable to review this pull request. You can try again by re-requesting a review.",
                "createdAt": "2026-03-09T00:00:00Z",
                "url": "https://example.com/review-1",
            },
        )
        record = pr_review_coordinator.get_tracked_pr("repo-pr-1")
        with mock.patch.object(
            pr_review_coordinator,
            "now_ms",
            return_value=pr_review_coordinator.parse_github_timestamp_ms("2026-03-09T00:10:00Z"),
        ):
            result = pr_review_coordinator.poll_record(record, dry_run=False, force_run=False, job_id=104)

        self.assertEqual(result["status"], "idle")
        self.assertEqual(result["tracked_pr"]["status"], "copilot_review_cooldown")
        self.assertIn("next re-request after", result["tracked_pr"]["last_run_summary"])
        self.assertEqual(pr_review_coordinator.list_pending_jobs(), [])

    def test_poll_record_rerequests_copilot_review_after_cooldown(self):
        snapshots = iter(
            [
                self.snapshot(
                    status="copilot_review_cooldown",
                    signature="sig-cooldown",
                    copilot_review_error={
                        "id": "review-1",
                        "author": "github-copilot[bot]",
                        "body": "Copilot encountered an error and was unable to review this pull request. You can try again by re-requesting a review.",
                        "createdAt": "2026-03-09T00:00:00Z",
                        "url": "https://example.com/review-1",
                    },
                ),
                self.snapshot(
                    status="pending_copilot_review",
                    signature="sig-pending",
                    pending_copilot_review=True,
                ),
            ]
        )
        pr_review_coordinator.pull_request_snapshot = lambda *args, **kwargs: next(snapshots)
        record = pr_review_coordinator.get_tracked_pr("repo-pr-1")
        captured: dict[str, object] = {}

        def fake_run(cmd, cwd=None):
            captured["cmd"] = cmd
            captured["cwd"] = cwd
            return mock.Mock(stdout="", stderr="")

        with mock.patch.object(pr_review_coordinator, "run", side_effect=fake_run):
            with mock.patch.object(
                pr_review_coordinator,
                "now_ms",
                return_value=pr_review_coordinator.parse_github_timestamp_ms("2026-03-09T00:16:00Z"),
            ):
                result = pr_review_coordinator.poll_record(record, dry_run=False, force_run=False, job_id=105)

        self.assertEqual(result["status"], "idle")
        self.assertEqual(result["tracked_pr"]["status"], "pending_copilot_review")
        self.assertEqual(
            captured["cmd"],
            ["gh", "pr", "edit", "1", "--add-reviewer", "copilot-pull-request-reviewer"],
        )
        self.assertEqual(captured["cwd"], "/tmp/repo")
        self.assertEqual(
            result["tracked_pr"]["last_copilot_rerequested_at"],
            pr_review_coordinator.parse_github_timestamp_ms("2026-03-09T00:16:00Z"),
        )
        self.assertEqual(pr_review_coordinator.list_pending_jobs(), [])

    def test_poll_all_job_fans_out_per_pr_poll_jobs(self):
        job_info = pr_review_coordinator.enqueue_job("poll-all", requested_by="test")
        result = pr_review_coordinator.process_job(pr_review_coordinator.get_job(job_info["job"]["id"]))

        self.assertEqual(result["status"], "ready")
        pending = pr_review_coordinator.list_pending_jobs()
        self.assertEqual([job.action for job in pending], ["poll-one", "poll-one"])

    def test_claim_next_job_prioritizes_untrack(self):
        pr_review_coordinator.enqueue_job("run-one", tracked_pr_key="repo-pr-1", requested_by="test")
        pr_review_coordinator.enqueue_job("untrack", tracked_pr_key="repo-pr-2", requested_by="test")

        claimed = pr_review_coordinator.claim_next_job()

        self.assertIsNotNone(claimed)
        self.assertEqual(claimed.action, "untrack")
        self.assertEqual(claimed.tracked_pr_key, "repo-pr-2")

    def test_process_run_one_uses_execution_path(self):
        pr_review_coordinator.enqueue_job("run-one", tracked_pr_key="repo-pr-1", requested_by="test", payload={"force_run": True})
        claimed = pr_review_coordinator.claim_next_job()
        captured = {}

        def fake_run_follow_up(record, *, dry_run, force_run, allow_dirty_worktree=False, job_id):
            captured["record"] = record.key
            captured["dry_run"] = dry_run
            captured["force_run"] = force_run
            captured["allow_dirty_worktree"] = allow_dirty_worktree
            captured["job_id"] = job_id
            return {"status": "ok"}

        pr_review_coordinator.run_follow_up = fake_run_follow_up

        pr_review_coordinator.process_job(claimed)

        self.assertEqual(
            captured,
            {"record": "repo-pr-1", "dry_run": False, "force_run": True, "allow_dirty_worktree": False, "job_id": claimed.id},
        )

    def test_process_steer_message_uses_execution_path_with_message(self):
        pr_review_coordinator.enqueue_job(
            "steer-message",
            tracked_pr_key="repo-pr-1",
            requested_by="test",
            payload={"message": "Focus on the failing checkout flow."},
        )
        claimed = pr_review_coordinator.claim_next_job()
        captured = {}

        def fake_run_follow_up(record, *, dry_run, force_run, allow_dirty_worktree=False, steering_message=None, job_id):
            captured["record"] = record.key
            captured["dry_run"] = dry_run
            captured["force_run"] = force_run
            captured["allow_dirty_worktree"] = allow_dirty_worktree
            captured["steering_message"] = steering_message
            captured["job_id"] = job_id
            return {"status": "ok"}

        pr_review_coordinator.run_follow_up = fake_run_follow_up

        pr_review_coordinator.process_job(claimed)

        self.assertEqual(
            captured,
            {
                "record": "repo-pr-1",
                "dry_run": False,
                "force_run": True,
                "allow_dirty_worktree": False,
                "steering_message": "Focus on the failing checkout flow.",
                "job_id": claimed.id,
            },
        )

    def test_steer_messages_are_not_deduplicated(self):
        first = pr_review_coordinator.enqueue_job(
            "steer-message",
            tracked_pr_key="repo-pr-1",
            requested_by="test",
            payload={"message": "First"},
        )
        second = pr_review_coordinator.enqueue_job(
            "steer-message",
            tracked_pr_key="repo-pr-1",
            requested_by="test",
            payload={"message": "Second"},
        )

        self.assertFalse(first["duplicate"])
        self.assertFalse(second["duplicate"])
        self.assertEqual([job.action for job in pr_review_coordinator.list_pending_jobs()], ["steer-message", "steer-message"])

    def test_process_use_worktree_anyway_uses_dirty_override_path(self):
        pr_review_coordinator.enqueue_job("use-worktree-anyway", tracked_pr_key="repo-pr-1", requested_by="test")
        claimed = pr_review_coordinator.claim_next_job()
        captured = {}

        def fake_run_follow_up(record, *, dry_run, force_run, allow_dirty_worktree=False, job_id):
            captured["record"] = record.key
            captured["dry_run"] = dry_run
            captured["force_run"] = force_run
            captured["allow_dirty_worktree"] = allow_dirty_worktree
            captured["job_id"] = job_id
            return {"status": "ok"}

        pr_review_coordinator.run_follow_up = fake_run_follow_up

        pr_review_coordinator.process_job(claimed)

        self.assertEqual(
            captured,
            {"record": "repo-pr-1", "dry_run": False, "force_run": True, "allow_dirty_worktree": True, "job_id": claimed.id},
        )

    def test_process_track_existing_job_uses_register_tracking(self):
        captured = {}

        def fake_register_tracking(**kwargs):
            captured.update(kwargs)
            return {"status": "ready", "tracked_pr": {"key": "repo-pr-9"}}

        pr_review_coordinator.register_tracking = fake_register_tracking
        pr_review_coordinator.enqueue_job(
            "track-existing",
            tracked_pr_key="repo-pr-9",
            requested_by="test",
            payload={
                "repo_root": "/tmp/repo",
                "repo_name": "repo",
                "pr_number": 9,
                "branch": "branch-9",
                "provider": "codex",
            },
        )
        claimed = pr_review_coordinator.claim_next_job()

        result = pr_review_coordinator.process_job(claimed)

        self.assertEqual(result["status"], "ready")
        self.assertEqual(
            captured,
            {
                "repo_root": "/tmp/repo",
                "repo_name": "repo",
                "pr_number": 9,
                "branch": "branch-9",
                "worktree_root": str(pr_review_coordinator.CODEX_HOME / "worktrees" / "pr-review"),
                "worktree_path": None,
                "thread_id": None,
                "worktree_layout": "nested",
                "provider": "codex",
            },
        )
        self.assertEqual(pr_review_coordinator.get_job(claimed.id).status, "succeeded")

    def test_process_track_existing_job_can_create_fresh_codex_thread(self):
        captured = {}

        def fake_register_tracking(**kwargs):
            captured.update(kwargs)
            return {"status": "ready", "tracked_pr": {"key": "repo-pr-9"}}

        pr_review_coordinator.register_tracking = fake_register_tracking
        pr_review_coordinator.create_codex_thread = lambda repo_root: {"id": "thread-fresh", "title": "Fresh thread"}
        pr_review_coordinator.enqueue_job(
            "track-existing",
            tracked_pr_key="repo-pr-9",
            requested_by="test",
            payload={
                "repo_root": "/tmp/repo",
                "repo_name": "repo",
                "pr_number": 9,
                "branch": "branch-9",
                "provider": "codex",
                "thread_id": pr_review_coordinator.NEW_CODEX_THREAD_SENTINEL,
            },
        )
        claimed = pr_review_coordinator.claim_next_job()

        result = pr_review_coordinator.process_job(claimed)

        self.assertEqual(result["status"], "ready")
        self.assertEqual(captured["thread_id"], "thread-fresh")
        self.assertEqual(pr_review_coordinator.get_job(claimed.id).status, "succeeded")

    def test_process_retarget_thread_job_updates_thread(self):
        pr_review_coordinator.resolve_selected_thread = lambda repo_root, provider, requested_thread_id, prefer_latest_when_empty=False: {
            "id": "thread-new",
            "title": "Fresh thread",
        }
        pr_review_coordinator.enqueue_job(
            "retarget-thread",
            tracked_pr_key="repo-pr-1",
            requested_by="test",
            payload={"provider": "codex", "thread_id": "thread-new"},
        )
        claimed = pr_review_coordinator.claim_next_job()

        result = pr_review_coordinator.process_job(claimed)

        self.assertEqual(result["status"], "ready")
        updated = pr_review_coordinator.get_tracked_pr("repo-pr-1")
        self.assertEqual(updated.thread_id, "thread-new")
        self.assertEqual(updated.thread_title, "Fresh thread")
        self.assertEqual(pr_review_coordinator.get_job(claimed.id).status, "succeeded")


class HandoffInputTests(unittest.TestCase):
    def test_validate_handoff_branch_accepts_work_type_prefix(self):
        self.assertEqual(
            pr_review_coordinator.validate_handoff_branch_name("feat/example-change"),
            "feat/example-change",
        )

    def test_validate_handoff_branch_rejects_agent_prefix(self):
        with self.assertRaises(pr_review_common.ScriptError):
            pr_review_coordinator.validate_handoff_branch_name("codex/example-change")

    def test_validate_handoff_branch_rejects_unsupported_prefix(self):
        with self.assertRaises(pr_review_common.ScriptError):
            pr_review_coordinator.validate_handoff_branch_name("feature/example-change")

    def test_render_pr_body_template_uses_canonical_sections(self):
        body = pr_review_coordinator.render_pr_body_template(
            summary=["add completion handoff"],
            validation=["python3 -m unittest"],
            notes=["uses Codex hooks"],
        )

        self.assertIn("## Summary\n- add completion handoff", body)
        self.assertIn("## Validation\n- python3 -m unittest", body)
        self.assertIn("## Notes\n- uses Codex hooks", body)

    def test_complete_defaults_infer_checkout_title_commit_and_body(self):
        with tempfile.TemporaryDirectory() as tmp:
            pr_review_common.run(["git", "init"], cwd=tmp)
            original_cwd = os.getcwd()
            try:
                os.chdir(tmp)
                defaults = pr_review_coordinator.resolve_complete_defaults(
                    repo_root=None,
                    worktree_path=None,
                    branch="feat/add-handoff-guard",
                    commit_message=None,
                    pr_title=None,
                    pr_body=None,
                    summary=[],
                    validation=["python3 -m unittest tests/test_coordinator.py"],
                    notes=[],
                )
            finally:
                os.chdir(original_cwd)

        self.assertEqual(defaults["repo_root"], str(Path(tmp).resolve()))
        self.assertIsNone(defaults["worktree_path"])
        self.assertEqual(defaults["branch"], "feat/add-handoff-guard")
        self.assertEqual(defaults["pr_title"], "Add handoff guard")
        self.assertEqual(defaults["commit_message"], "Add handoff guard")
        self.assertIn("## Summary\n- Add handoff guard", str(defaults["pr_body"]))
        self.assertIn("## Validation\n- python3 -m unittest tests/test_coordinator.py", str(defaults["pr_body"]))


class HandoffAdoptionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_var_dir = pr_review_coordinator.VAR_DIR
        self.original_locks_dir = pr_review_coordinator.LOCKS_DIR
        self.original_db = pr_review_coordinator.COORDINATOR_DB
        self.original_verify_gh_auth = pr_review_coordinator.verify_gh_auth
        self.original_ensure_repo_name = pr_review_coordinator.ensure_repo_name
        self.original_resolve_checkout_root = pr_review_coordinator.resolve_checkout_root
        self.original_ensure_branch = pr_review_coordinator.ensure_branch
        self.original_commit_all_changes = pr_review_coordinator.commit_all_changes
        self.original_push_branch = pr_review_coordinator.push_branch
        self.original_create_or_reuse_pr = pr_review_coordinator.create_or_reuse_pr
        self.original_current_branch = pr_review_coordinator.current_branch
        self.original_resolve_thread = pr_review_coordinator.resolve_thread
        self.original_assert_thread_available = pr_review_coordinator.assert_thread_available
        self.original_ensure_existing_worktree = pr_review_coordinator.ensure_existing_worktree
        self.original_pull_request_snapshot = pr_review_coordinator.pull_request_snapshot
        self.original_record_event = pr_review_coordinator.record_event
        pr_review_coordinator.VAR_DIR = Path(self.temp_dir.name)
        pr_review_coordinator.LOCKS_DIR = pr_review_coordinator.VAR_DIR / "locks"
        pr_review_coordinator.COORDINATOR_DB = pr_review_coordinator.VAR_DIR / "test.db"

    def tearDown(self):
        pr_review_coordinator.VAR_DIR = self.original_var_dir
        pr_review_coordinator.LOCKS_DIR = self.original_locks_dir
        pr_review_coordinator.COORDINATOR_DB = self.original_db
        pr_review_coordinator.verify_gh_auth = self.original_verify_gh_auth
        pr_review_coordinator.ensure_repo_name = self.original_ensure_repo_name
        pr_review_coordinator.resolve_checkout_root = self.original_resolve_checkout_root
        pr_review_coordinator.ensure_branch = self.original_ensure_branch
        pr_review_coordinator.commit_all_changes = self.original_commit_all_changes
        pr_review_coordinator.push_branch = self.original_push_branch
        pr_review_coordinator.create_or_reuse_pr = self.original_create_or_reuse_pr
        pr_review_coordinator.current_branch = self.original_current_branch
        pr_review_coordinator.resolve_thread = self.original_resolve_thread
        pr_review_coordinator.assert_thread_available = self.original_assert_thread_available
        pr_review_coordinator.ensure_existing_worktree = self.original_ensure_existing_worktree
        pr_review_coordinator.pull_request_snapshot = self.original_pull_request_snapshot
        pr_review_coordinator.record_event = self.original_record_event
        self.temp_dir.cleanup()

    def test_handoff_uses_adopted_checkout_for_git_operations(self):
        captured: dict[str, object] = {}
        pr_review_coordinator.verify_gh_auth = lambda: None
        pr_review_coordinator.ensure_repo_name = lambda repo_root, repo_name: ("owner", "repo")
        pr_review_coordinator.resolve_checkout_root = lambda repo_root, repo_name, worktree_path: "/tmp/adopted"
        def fake_ensure_branch(repo_root, branch):
            captured["ensure_branch_root"] = repo_root
            return {"status": "ready", "branch": branch, "created": False}

        def fake_commit_all_changes(repo_root, message):
            captured["commit_root"] = repo_root
            return {"status": "ready", "committed": True, "sha": "abc123"}

        def fake_push_branch(repo_root, branch):
            captured["push_root"] = repo_root
            return {"status": "ready", "branch": branch, "sha": "abc123"}

        def fake_create_or_reuse_pr(repo_root, branch, base_branch, title, body, draft):
            captured["pr_root"] = repo_root
            return {
                "status": "ready",
                "created": True,
                "number": 42,
                "url": "https://example.com/pr/42",
                "title": title,
                "state": "OPEN",
                "headRefName": branch,
                "baseRefName": base_branch,
            }

        pr_review_coordinator.ensure_branch = fake_ensure_branch
        pr_review_coordinator.commit_all_changes = fake_commit_all_changes
        pr_review_coordinator.push_branch = fake_push_branch
        pr_review_coordinator.create_or_reuse_pr = fake_create_or_reuse_pr
        pr_review_coordinator.current_branch = lambda repo_root: "master"
        pr_review_coordinator.resolve_thread = lambda repo_root, thread_id, provider=None: {"id": "thread-42", "title": "Thread"}
        pr_review_coordinator.assert_thread_available = lambda thread_id, key: None
        pr_review_coordinator.ensure_existing_worktree = lambda repo_root, repo_name, branch, worktree_path: {
            "status": "ready",
            "worktree": worktree_path,
            "created": False,
            "managed": False,
        }
        pr_review_coordinator.pull_request_snapshot = lambda repo_root, repo_name, pr_number: {
            "status": "pending_copilot_review",
            "pr": {
                "number": pr_number,
                "url": "https://example.com/pr/42",
                "title": "PR 42",
                "state": "OPEN",
            },
            "signature": "sig",
            "latest_comment_at": None,
            "pending_copilot_review": True,
            "unresolved_threads": [],
            "actionable_pr_comments": [],
            "failing_checks": [],
            "ci_summary": None,
            "unresolved_thread_count": 0,
            "actionable_comment_count": 0,
            "failing_check_count": 0,
        }
        pr_review_coordinator.record_event = lambda *args, **kwargs: None

        result = pr_review_coordinator.handoff_pr(
            repo_root="/tmp/repo",
            repo_name="repo",
            branch="feat/example",
            base_branch="master",
            commit_message="Example",
            pr_title="Example",
            pr_body="Body",
            draft=False,
            worktree_root="/tmp/worktrees",
            worktree_path="/tmp/adopted",
            thread_id="thread-42",
            worktree_layout="nested",
            provider="codex",
        )

        self.assertEqual(captured["ensure_branch_root"], "/tmp/adopted")
        self.assertEqual(captured["commit_root"], "/tmp/adopted")
        self.assertEqual(captured["push_root"], "/tmp/adopted")
        self.assertEqual(captured["pr_root"], "/tmp/adopted")
        self.assertEqual(result["tracked_pr"]["worktree_path"], "/tmp/adopted")
        self.assertFalse(result["tracked_pr"]["worktree_managed"])


class FollowUpWorktreeTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_var_dir = pr_review_coordinator.VAR_DIR
        self.original_locks_dir = pr_review_coordinator.LOCKS_DIR
        self.original_db = pr_review_coordinator.COORDINATOR_DB
        self.original_acquire_lock = pr_review_coordinator.acquire_lock
        self.original_release_lock = pr_review_coordinator.release_lock
        self.original_pull_request_snapshot = pr_review_coordinator.pull_request_snapshot
        self.original_should_trigger_follow_up = pr_review_coordinator.should_trigger_follow_up
        self.original_git_status_is_clean = pr_review_coordinator.git_status_is_clean
        self.original_refresh_record_state = pr_review_coordinator.refresh_record_state
        self.original_update_tracked_pr = pr_review_coordinator.update_tracked_pr
        self.original_record_event = pr_review_coordinator.record_event
        self.original_ensure_worktree = pr_review_coordinator.ensure_worktree
        self.original_ensure_existing_worktree = pr_review_coordinator.ensure_existing_worktree
        self.original_sync_worktree_to_remote = pr_review_coordinator.sync_worktree_to_remote
        self.original_clear_worktree_to_remote = pr_review_coordinator.clear_worktree_to_remote
        self.original_run_agent_resume = pr_review_coordinator.run_agent_resume
        self.original_remote_branch_sha = pr_review_coordinator.remote_branch_sha
        self.original_local_head_sha = pr_review_coordinator.local_head_sha
        self.original_request_copilot_review = pr_review_coordinator.request_copilot_review
        pr_review_coordinator.VAR_DIR = Path(self.temp_dir.name)
        pr_review_coordinator.LOCKS_DIR = pr_review_coordinator.VAR_DIR / "locks"
        pr_review_coordinator.COORDINATOR_DB = pr_review_coordinator.VAR_DIR / "test.db"

    def tearDown(self):
        pr_review_coordinator.VAR_DIR = self.original_var_dir
        pr_review_coordinator.LOCKS_DIR = self.original_locks_dir
        pr_review_coordinator.COORDINATOR_DB = self.original_db
        pr_review_coordinator.acquire_lock = self.original_acquire_lock
        pr_review_coordinator.release_lock = self.original_release_lock
        pr_review_coordinator.pull_request_snapshot = self.original_pull_request_snapshot
        pr_review_coordinator.should_trigger_follow_up = self.original_should_trigger_follow_up
        pr_review_coordinator.git_status_is_clean = self.original_git_status_is_clean
        pr_review_coordinator.refresh_record_state = self.original_refresh_record_state
        pr_review_coordinator.update_tracked_pr = self.original_update_tracked_pr
        pr_review_coordinator.record_event = self.original_record_event
        pr_review_coordinator.ensure_worktree = self.original_ensure_worktree
        pr_review_coordinator.ensure_existing_worktree = self.original_ensure_existing_worktree
        pr_review_coordinator.sync_worktree_to_remote = self.original_sync_worktree_to_remote
        pr_review_coordinator.clear_worktree_to_remote = self.original_clear_worktree_to_remote
        pr_review_coordinator.run_agent_resume = self.original_run_agent_resume
        pr_review_coordinator.remote_branch_sha = self.original_remote_branch_sha
        pr_review_coordinator.local_head_sha = self.original_local_head_sha
        pr_review_coordinator.request_copilot_review = self.original_request_copilot_review
        self.temp_dir.cleanup()

    def make_record(self, **overrides):
        payload = {
            "key": "repo-pr-42",
            "repo_root": "/tmp/repo",
            "repo_owner": "owner",
            "repo_name": "repo",
            "pr_number": 42,
            "pr_url": "https://example.com/pr/42",
            "pr_title": "Example PR",
            "pr_state": "OPEN",
            "branch": "feature/example",
            "base_branch": "main",
            "worktree_path": "/tmp/worktrees/repo-pr-42",
            "worktree_managed": 1,
            "worktree_root": "/tmp/worktrees",
            "worktree_layout": "sibling",
            "thread_id": "thread-42",
            "thread_title": "Thread",
            "status": "needs_review",
            "active": 1,
            "last_review_signature": "sig",
            "last_handled_signature": None,
            "last_review_status": "needs_review",
            "last_review_comment_at": None,
            "pending_copilot_review": 0,
            "unresolved_thread_count": 1,
            "actionable_comment_count": 0,
            "failing_check_count": 0,
            "unresolved_threads_json": "[]",
            "actionable_comments_json": "[]",
            "failing_checks_json": "[]",
            "ci_summary": None,
            "run_state": None,
            "run_reason": None,
            "current_job_id": None,
            "lock_started_at": None,
            "lock_owner_pid": None,
            "last_polled_at": None,
            "last_prompted_at": None,
            "last_run_started_at": None,
            "last_run_finished_at": None,
            "last_run_status": "ready",
            "last_run_summary": "Idle",
            "last_error": None,
            "provider": "codex",
            "created_at": 0,
            "updated_at": 0,
        }
        payload.update(overrides)
        return pr_review_coordinator.TrackedPR(**payload)

    def test_run_follow_up_uses_stored_managed_worktree_root_and_layout(self):
        captured: dict[str, object] = {}
        pr_review_coordinator.acquire_lock = lambda record, job_id: None
        pr_review_coordinator.release_lock = lambda record: None
        pr_review_coordinator.pull_request_snapshot = lambda repo_root, repo_name, pr_number: {
            "status": "needs_review",
            "pr": {
                "number": pr_number,
                "url": "https://example.com/pr/42",
                "title": "PR 42",
                "state": "OPEN",
            },
            "signature": "sig-next",
            "latest_comment_at": None,
            "pending_copilot_review": False,
            "unresolved_threads": [{"id": "thread-1"}],
            "actionable_pr_comments": [],
            "failing_checks": [],
            "ci_summary": None,
            "unresolved_thread_count": 1,
            "actionable_comment_count": 0,
            "failing_check_count": 0,
        }
        pr_review_coordinator.should_trigger_follow_up = lambda record, snapshot, force_run=False: (True, "needs review")
        pr_review_coordinator.git_status_is_clean = lambda path: True
        pr_review_coordinator.refresh_record_state = lambda *args, **kwargs: args[0]
        pr_review_coordinator.update_tracked_pr = lambda key, **changes: self.make_record()
        pr_review_coordinator.record_event = lambda *args, **kwargs: None

        def fake_ensure_worktree(repo_root, repo_name, pr_number, branch, worktree_root, *, layout):
            captured["worktree_root"] = worktree_root
            captured["layout"] = layout
            return {"status": "ready", "worktree": "/tmp/worktrees/repo-pr-42", "created": False}

        pr_review_coordinator.ensure_worktree = fake_ensure_worktree
        pr_review_coordinator.sync_worktree_to_remote = lambda repo_root, branch, worktree: {
            "status": "ready",
            "worktree": worktree,
            "head": "abc123",
            "changed": False,
        }
        pr_review_coordinator.run_agent_resume = lambda record, snapshot, dry_run: {
            "status": "ok",
            "last_message": "done",
        }

        result = pr_review_coordinator.run_follow_up(
            self.make_record(),
            dry_run=False,
            force_run=False,
            job_id=7,
        )

        self.assertEqual(captured["worktree_root"], "/tmp/worktrees")
        self.assertEqual(captured["layout"], "sibling")
        self.assertEqual(result["status"], "ok")

    def test_run_follow_up_rerequests_copilot_after_pushed_agent_changes(self):
        captured: dict[str, object] = {"events": [], "updates": []}
        snapshots = iter(
            [
                {
                    "status": "needs_review",
                    "pr": {
                        "number": 42,
                        "url": "https://example.com/pr/42",
                        "title": "PR 42",
                        "state": "OPEN",
                    },
                    "signature": "sig-next",
                    "latest_comment_at": None,
                    "pending_copilot_review": False,
                    "unresolved_threads": [{"id": "thread-1"}],
                    "actionable_pr_comments": [],
                    "failing_checks": [],
                    "ci_summary": None,
                    "unresolved_thread_count": 1,
                    "actionable_comment_count": 0,
                    "failing_check_count": 0,
                },
                {
                    "status": "pending_copilot_review",
                    "pr": {
                        "number": 42,
                        "url": "https://example.com/pr/42",
                        "title": "PR 42",
                        "state": "OPEN",
                    },
                    "signature": "sig-pending",
                    "latest_comment_at": None,
                    "pending_copilot_review": True,
                    "unresolved_threads": [],
                    "actionable_pr_comments": [],
                    "failing_checks": [],
                    "ci_summary": None,
                    "unresolved_thread_count": 0,
                    "actionable_comment_count": 0,
                    "failing_check_count": 0,
                },
            ]
        )
        remote_shas = iter(["before-sha", "after-sha"])
        pr_review_coordinator.acquire_lock = lambda record, job_id: None
        pr_review_coordinator.release_lock = lambda record: None
        pr_review_coordinator.pull_request_snapshot = lambda repo_root, repo_name, pr_number: next(snapshots)
        pr_review_coordinator.should_trigger_follow_up = lambda record, snapshot, force_run=False: (True, "needs review")
        pr_review_coordinator.git_status_is_clean = lambda path: True
        pr_review_coordinator.refresh_record_state = lambda *args, **kwargs: args[0]
        pr_review_coordinator.update_tracked_pr = lambda key, **changes: captured["updates"].append(changes) or self.make_record(
            last_prompted_at=changes.get("last_prompted_at", 1),
            last_copilot_rerequested_at=changes.get("last_copilot_rerequested_at"),
            pending_copilot_review=changes.get("pending_copilot_review", 0),
            last_review_signature=changes.get("last_review_signature", "sig-next"),
            last_review_status=changes.get("last_review_status", "needs_review"),
            status=changes.get("status", "needs_review"),
        )
        pr_review_coordinator.record_event = lambda level, event_type, message, **kwargs: captured["events"].append(
            {"level": level, "event_type": event_type, "message": message, "details": kwargs.get("details")}
        )
        pr_review_coordinator.ensure_worktree = lambda repo_root, repo_name, pr_number, branch, worktree_root, *, layout: {
            "status": "ready",
            "worktree": "/tmp/worktrees/repo-pr-42",
            "created": False,
        }
        pr_review_coordinator.sync_worktree_to_remote = lambda repo_root, branch, worktree: {
            "status": "ready",
            "worktree": worktree,
            "head": "before-sha",
            "changed": False,
        }
        pr_review_coordinator.run_agent_resume = lambda record, snapshot, dry_run: {
            "status": "ok",
            "last_message": "pushed fixes",
        }
        pr_review_coordinator.remote_branch_sha = lambda repo_root, branch: next(remote_shas)
        def fake_request_copilot_review(record):
            captured["requested_pr"] = record.pr_number
            return {
                "status": "ready",
                "reviewer": "copilot-pull-request-reviewer",
            }

        pr_review_coordinator.request_copilot_review = fake_request_copilot_review

        result = pr_review_coordinator.run_follow_up(
            self.make_record(),
            dry_run=False,
            force_run=False,
            job_id=7,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(captured["requested_pr"], 42)
        self.assertEqual(result["review_request"]["status"], "ready")
        self.assertEqual(result["review_request"]["before"], "before-sha")
        self.assertEqual(result["review_request"]["after"], "after-sha")
        self.assertEqual(result["review_request"]["reason"], "remote branch changed")
        self.assertEqual(result["review"]["status"], "pending_copilot_review")
        self.assertEqual(
            [event["event_type"] for event in captured["events"] if event["event_type"] == "copilot_review_rerequested"],
            ["copilot_review_rerequested"],
        )
        self.assertTrue(any("last_copilot_rerequested_at" in update for update in captured["updates"]))

    def test_run_follow_up_rerequests_copilot_when_local_change_reached_remote_after_missing_before_sha(self):
        captured: dict[str, object] = {"events": [], "updates": []}
        snapshots = iter(
            [
                {
                    "status": "needs_review",
                    "pr": {
                        "number": 42,
                        "url": "https://example.com/pr/42",
                        "title": "PR 42",
                        "state": "OPEN",
                    },
                    "signature": "sig-next",
                    "latest_comment_at": None,
                    "pending_copilot_review": False,
                    "unresolved_threads": [{"id": "thread-1"}],
                    "actionable_pr_comments": [],
                    "failing_checks": [],
                    "ci_summary": None,
                    "unresolved_thread_count": 1,
                    "actionable_comment_count": 0,
                    "failing_check_count": 0,
                },
                {
                    "status": "pending_copilot_review",
                    "pr": {
                        "number": 42,
                        "url": "https://example.com/pr/42",
                        "title": "PR 42",
                        "state": "OPEN",
                    },
                    "signature": "sig-pending",
                    "latest_comment_at": None,
                    "pending_copilot_review": True,
                    "unresolved_threads": [],
                    "actionable_pr_comments": [],
                    "failing_checks": [],
                    "ci_summary": None,
                    "unresolved_thread_count": 0,
                    "actionable_comment_count": 0,
                    "failing_check_count": 0,
                },
            ]
        )
        remote_shas = iter([None, "after-sha"])
        local_shas = iter(["before-sha", "after-sha"])
        pr_review_coordinator.acquire_lock = lambda record, job_id: None
        pr_review_coordinator.release_lock = lambda record: None
        pr_review_coordinator.pull_request_snapshot = lambda repo_root, repo_name, pr_number: next(snapshots)
        pr_review_coordinator.should_trigger_follow_up = lambda record, snapshot, force_run=False: (True, "needs review")
        pr_review_coordinator.git_status_is_clean = lambda path: True
        pr_review_coordinator.refresh_record_state = lambda *args, **kwargs: args[0]
        pr_review_coordinator.update_tracked_pr = lambda key, **changes: captured["updates"].append(changes) or self.make_record(
            last_prompted_at=changes.get("last_prompted_at", 1),
            last_copilot_rerequested_at=changes.get("last_copilot_rerequested_at"),
            pending_copilot_review=changes.get("pending_copilot_review", 0),
            last_review_signature=changes.get("last_review_signature", "sig-next"),
            last_review_status=changes.get("last_review_status", "needs_review"),
            status=changes.get("status", "needs_review"),
        )
        pr_review_coordinator.record_event = lambda level, event_type, message, **kwargs: captured["events"].append(
            {"level": level, "event_type": event_type, "message": message, "details": kwargs.get("details")}
        )
        pr_review_coordinator.ensure_worktree = lambda repo_root, repo_name, pr_number, branch, worktree_root, *, layout: {
            "status": "ready",
            "worktree": "/tmp/worktrees/repo-pr-42",
            "created": False,
        }
        pr_review_coordinator.sync_worktree_to_remote = lambda repo_root, branch, worktree: {
            "status": "ready",
            "worktree": worktree,
            "head": "before-sha",
            "changed": False,
        }
        pr_review_coordinator.run_agent_resume = lambda record, snapshot, dry_run: {
            "status": "ok",
            "last_message": "pushed fixes",
        }
        pr_review_coordinator.remote_branch_sha = lambda repo_root, branch: next(remote_shas)
        pr_review_coordinator.local_head_sha = lambda worktree: next(local_shas)
        pr_review_coordinator.request_copilot_review = lambda record: {
            "status": "ready",
            "reviewer": "copilot-pull-request-reviewer",
        }

        result = pr_review_coordinator.run_follow_up(
            self.make_record(),
            dry_run=False,
            force_run=False,
            job_id=7,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["review_request"]["status"], "ready")
        self.assertIsNone(result["review_request"]["before"])
        self.assertEqual(result["review_request"]["after"], "after-sha")
        self.assertEqual(result["review_request"]["local_before"], "before-sha")
        self.assertEqual(result["review_request"]["local_after"], "after-sha")
        self.assertEqual(result["review_request"]["reason"], "local head changed and remote matches local head")
        self.assertEqual(
            [event["event_type"] for event in captured["events"] if event["event_type"] == "copilot_review_rerequested"],
            ["copilot_review_rerequested"],
        )

    def test_run_follow_up_skips_copilot_request_when_remote_branch_unchanged(self):
        captured: dict[str, object] = {"events": []}
        remote_shas = iter(["same-sha", "same-sha"])
        local_shas = iter(["same-sha", "same-sha"])
        pr_review_coordinator.acquire_lock = lambda record, job_id: None
        pr_review_coordinator.release_lock = lambda record: None
        pr_review_coordinator.pull_request_snapshot = lambda repo_root, repo_name, pr_number: {
            "status": "needs_review",
            "pr": {
                "number": pr_number,
                "url": "https://example.com/pr/42",
                "title": "PR 42",
                "state": "OPEN",
            },
            "signature": "sig-next",
            "latest_comment_at": None,
            "pending_copilot_review": False,
            "unresolved_threads": [{"id": "thread-1"}],
            "actionable_pr_comments": [],
            "failing_checks": [],
            "ci_summary": None,
            "unresolved_thread_count": 1,
            "actionable_comment_count": 0,
            "failing_check_count": 0,
        }
        pr_review_coordinator.should_trigger_follow_up = lambda record, snapshot, force_run=False: (True, "needs review")
        pr_review_coordinator.git_status_is_clean = lambda path: True
        pr_review_coordinator.refresh_record_state = lambda *args, **kwargs: args[0]
        pr_review_coordinator.update_tracked_pr = lambda key, **changes: self.make_record()
        pr_review_coordinator.record_event = lambda level, event_type, message, **kwargs: captured["events"].append(
            {"level": level, "event_type": event_type, "message": message, "details": kwargs.get("details")}
        )
        pr_review_coordinator.ensure_worktree = lambda repo_root, repo_name, pr_number, branch, worktree_root, *, layout: {
            "status": "ready",
            "worktree": "/tmp/worktrees/repo-pr-42",
            "created": False,
        }
        pr_review_coordinator.sync_worktree_to_remote = lambda repo_root, branch, worktree: {
            "status": "ready",
            "worktree": worktree,
            "head": "same-sha",
            "changed": False,
        }
        pr_review_coordinator.run_agent_resume = lambda record, snapshot, dry_run: {
            "status": "ok",
            "last_message": "no changes needed",
        }
        pr_review_coordinator.remote_branch_sha = lambda repo_root, branch: next(remote_shas)
        pr_review_coordinator.local_head_sha = lambda worktree: next(local_shas)

        def fail_request(record):
            raise AssertionError("Copilot review should not be re-requested without a pushed commit")

        pr_review_coordinator.request_copilot_review = fail_request

        result = pr_review_coordinator.run_follow_up(
            self.make_record(),
            dry_run=False,
            force_run=False,
            job_id=7,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["review_request"]["status"], "skipped")
        self.assertEqual(result["review_request"]["reason"], "remote branch unchanged")
        self.assertEqual(result["review_request"]["before"], "same-sha")
        self.assertEqual(result["review_request"]["after"], "same-sha")
        self.assertEqual(
            [event["event_type"] for event in captured["events"] if event["event_type"] == "copilot_review_request_skipped"],
            ["copilot_review_request_skipped"],
        )
        skipped_event = next(event for event in captured["events"] if event["event_type"] == "copilot_review_request_skipped")
        self.assertEqual(skipped_event["details"]["before"], "same-sha")
        self.assertEqual(skipped_event["details"]["after"], "same-sha")

    def test_run_follow_up_logs_skipped_copilot_request_when_remote_sha_after_missing(self):
        captured: dict[str, object] = {"events": []}
        remote_shas = iter(["before-sha", None])
        local_shas = iter(["before-sha", "after-sha"])
        pr_review_coordinator.acquire_lock = lambda record, job_id: None
        pr_review_coordinator.release_lock = lambda record: None
        pr_review_coordinator.pull_request_snapshot = lambda repo_root, repo_name, pr_number: {
            "status": "needs_review",
            "pr": {
                "number": pr_number,
                "url": "https://example.com/pr/42",
                "title": "PR 42",
                "state": "OPEN",
            },
            "signature": "sig-next",
            "latest_comment_at": None,
            "pending_copilot_review": False,
            "unresolved_threads": [{"id": "thread-1"}],
            "actionable_pr_comments": [],
            "failing_checks": [],
            "ci_summary": None,
            "unresolved_thread_count": 1,
            "actionable_comment_count": 0,
            "failing_check_count": 0,
        }
        pr_review_coordinator.should_trigger_follow_up = lambda record, snapshot, force_run=False: (True, "needs review")
        pr_review_coordinator.git_status_is_clean = lambda path: True
        pr_review_coordinator.refresh_record_state = lambda *args, **kwargs: args[0]
        pr_review_coordinator.update_tracked_pr = lambda key, **changes: self.make_record()
        pr_review_coordinator.record_event = lambda level, event_type, message, **kwargs: captured["events"].append(
            {"level": level, "event_type": event_type, "message": message, "details": kwargs.get("details")}
        )
        pr_review_coordinator.ensure_worktree = lambda repo_root, repo_name, pr_number, branch, worktree_root, *, layout: {
            "status": "ready",
            "worktree": "/tmp/worktrees/repo-pr-42",
            "created": False,
        }
        pr_review_coordinator.sync_worktree_to_remote = lambda repo_root, branch, worktree: {
            "status": "ready",
            "worktree": worktree,
            "head": "before-sha",
            "changed": False,
        }
        pr_review_coordinator.run_agent_resume = lambda record, snapshot, dry_run: {
            "status": "ok",
            "last_message": "pushed fixes",
        }
        pr_review_coordinator.remote_branch_sha = lambda repo_root, branch: next(remote_shas)
        pr_review_coordinator.local_head_sha = lambda worktree: next(local_shas)

        def fail_request(record):
            raise AssertionError("Copilot review should not be re-requested when the post-run remote SHA is unavailable")

        pr_review_coordinator.request_copilot_review = fail_request

        result = pr_review_coordinator.run_follow_up(
            self.make_record(),
            dry_run=False,
            force_run=False,
            job_id=7,
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["review_request"]["status"], "skipped")
        self.assertEqual(result["review_request"]["reason"], "missing remote branch SHA after run")
        skipped_event = next(event for event in captured["events"] if event["event_type"] == "copilot_review_request_skipped")
        self.assertEqual(skipped_event["details"]["before"], "before-sha")
        self.assertIsNone(skipped_event["details"]["after"])
        self.assertEqual(skipped_event["details"]["local_before"], "before-sha")
        self.assertEqual(skipped_event["details"]["local_after"], "after-sha")

    def test_run_follow_up_can_use_dirty_worktree_anyway_without_sync(self):
        captured: dict[str, object] = {}
        pr_review_coordinator.acquire_lock = lambda record, job_id: None
        pr_review_coordinator.release_lock = lambda record: None
        pr_review_coordinator.pull_request_snapshot = lambda repo_root, repo_name, pr_number: {
            "status": "needs_review",
            "pr": {
                "number": pr_number,
                "url": "https://example.com/pr/42",
                "title": "PR 42",
                "state": "OPEN",
            },
            "signature": "sig-next",
            "latest_comment_at": None,
            "pending_copilot_review": False,
            "unresolved_threads": [{"id": "thread-1"}],
            "actionable_pr_comments": [],
            "failing_checks": [],
            "ci_summary": None,
            "unresolved_thread_count": 1,
            "actionable_comment_count": 0,
            "failing_check_count": 0,
        }
        pr_review_coordinator.should_trigger_follow_up = lambda record, snapshot, force_run=False: (True, "needs review")
        pr_review_coordinator.git_status_is_clean = lambda path: False
        pr_review_coordinator.refresh_record_state = lambda *args, **kwargs: args[0]
        pr_review_coordinator.update_tracked_pr = lambda key, **changes: self.make_record()
        pr_review_coordinator.record_event = lambda *args, **kwargs: None
        pr_review_coordinator.ensure_existing_worktree = lambda repo_root, repo_name, branch, worktree_path, allow_dirty=False: captured.update(
            {"repo_root": repo_root, "repo_name": repo_name, "branch": branch, "worktree_path": worktree_path, "allow_dirty": allow_dirty}
        ) or {"status": "ready", "worktree": worktree_path, "created": False, "managed": False, "dirty": True}

        def fail_sync(*args, **kwargs):
            raise AssertionError("sync_worktree_to_remote should not run when dirty worktree override is requested")

        pr_review_coordinator.sync_worktree_to_remote = fail_sync
        pr_review_coordinator.run_agent_resume = lambda record, snapshot, dry_run: {
            "status": "ok",
            "last_message": "done",
        }

        result = pr_review_coordinator.run_follow_up(
            self.make_record(worktree_managed=0),
            dry_run=False,
            force_run=True,
            allow_dirty_worktree=True,
            job_id=7,
        )

        self.assertTrue(captured["allow_dirty"])
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["sync"]["status"], "skipped")

    def test_clear_tracked_worktree_resets_and_updates_summary(self):
        dirty_worktree = Path(self.temp_dir.name) / "dirty-worktree"
        dirty_worktree.mkdir()
        captured: dict[str, object] = {}
        pr_review_coordinator.acquire_lock = lambda record, job_id: None
        pr_review_coordinator.release_lock = lambda key: captured.setdefault("released", key)
        pr_review_coordinator.pull_request_snapshot = lambda repo_root, repo_name, pr_number: {
            "status": "needs_review",
            "pr": {
                "number": pr_number,
                "url": "https://example.com/pr/42",
                "title": "PR 42",
                "state": "OPEN",
            },
            "signature": "sig-next",
            "latest_comment_at": None,
            "pending_copilot_review": False,
            "unresolved_threads": [{"id": "thread-1"}],
            "actionable_pr_comments": [],
            "failing_checks": [],
            "ci_summary": None,
            "unresolved_thread_count": 1,
            "actionable_comment_count": 0,
            "failing_check_count": 0,
        }
        pr_review_coordinator.refresh_record_state = lambda *args, **kwargs: self.make_record(worktree_path=str(dirty_worktree), last_run_summary=kwargs.get("run_summary"))
        pr_review_coordinator.record_event = lambda *args, **kwargs: None
        pr_review_coordinator.clear_worktree_to_remote = lambda repo_root, branch, worktree: captured.update(
            {"repo_root": repo_root, "branch": branch, "worktree": str(worktree)}
        ) or {"status": "ready", "worktree": str(worktree), "head": "abc123", "cleared": True}

        result = pr_review_coordinator.clear_tracked_worktree(
            self.make_record(worktree_path=str(dirty_worktree)),
            job_id=99,
        )

        self.assertEqual(captured["branch"], "feature/example")
        self.assertEqual(captured["worktree"], str(dirty_worktree))
        self.assertEqual(captured["released"], "repo-pr-42")
        self.assertEqual(result["status"], "ready")


class OrphanedWorktreeTests(unittest.TestCase):
    def test_find_orphaned_managed_worktrees_reports_untracked_managed_paths(self):
        record = pr_review_coordinator.TrackedPR(
            key="repo-pr-42",
            repo_root="/tmp/repo",
            repo_owner="owner",
            repo_name="repo",
            pr_number=42,
            pr_url="https://example.com/pr/42",
            pr_title="PR 42",
            pr_state="OPEN",
            branch="feature/example",
            base_branch="main",
            worktree_path="/tmp/worktrees/repo/pr-42-feature-example",
            worktree_managed=1,
            thread_id="thread-42",
            thread_title="Thread",
            status="needs_review",
            active=1,
            last_review_signature=None,
            last_handled_signature=None,
            last_review_status="needs_review",
            last_review_comment_at=None,
            pending_copilot_review=0,
            unresolved_thread_count=0,
            actionable_comment_count=0,
            failing_check_count=0,
            unresolved_threads_json="[]",
            actionable_comments_json="[]",
            failing_checks_json="[]",
            ci_summary=None,
            run_state=None,
            run_reason=None,
            current_job_id=None,
            lock_started_at=None,
            lock_owner_pid=None,
            last_polled_at=None,
            last_prompted_at=None,
            last_run_started_at=None,
            last_run_finished_at=None,
            last_run_status="registered",
            last_run_summary="registered",
            last_error=None,
            worktree_root="/tmp/worktrees",
            worktree_layout="nested",
        )

        with mock.patch.object(
            pr_review_coordinator,
            "tracked_worktrees",
            return_value={
                "/tmp/repo": {"path": "/tmp/repo"},
                "/tmp/worktrees/repo/pr-42-feature-example": {"path": "/tmp/worktrees/repo/pr-42-feature-example"},
                "/tmp/worktrees/repo/pr-99-old-branch": {
                    "path": "/tmp/worktrees/repo/pr-99-old-branch",
                    "branch": "refs/heads/old-branch",
                    "HEAD": "deadbeef",
                },
            },
        ):
            orphaned = pr_review_coordinator.find_orphaned_managed_worktrees([record])

        self.assertEqual(len(orphaned), 1)
        self.assertEqual(orphaned[0]["worktree_path"], str(Path("/tmp/worktrees/repo/pr-99-old-branch").resolve()))


class CliHelpTests(unittest.TestCase):
    def test_top_level_help_mentions_canonical_managed_worktree_root(self):
        parser = pr_review_coordinator.parse_args()
        help_text = parser.format_help()

        self.assertIn(str(pr_review_common.DEFAULT_WORKTREE_ROOT), help_text)
        self.assertIn("Managed worktrees should normally live under one canonical root", help_text)
        self.assertIn("Use --worktree-path only to attach an existing git worktree", help_text)


if __name__ == "__main__":
    unittest.main()
