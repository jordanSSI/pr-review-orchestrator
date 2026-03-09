import tempfile
import unittest
from pathlib import Path

import pr_review_common
import pr_review_coordinator


def make_pull_request(*, unresolved=False, pending_copilot=False, failing_check=False):
    review_requests = []
    if pending_copilot:
        review_requests.append({"requestedReviewer": {"login": "copilot-pull-request-reviewer[bot]"}})
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
                            "author": {"login": "github-copilot[bot]"},
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
        "reviewRequests": {"nodes": review_requests},
        "reviewThreads": {"nodes": review_threads},
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

    def test_pending_copilot_without_comments(self):
        snapshot = self.snapshot(make_pull_request(pending_copilot=True))
        self.assertEqual(snapshot["status"], "pending_copilot_review")

    def test_completed_failing_ci_only(self):
        snapshot = self.snapshot(make_pull_request(failing_check=True))
        self.assertEqual(snapshot["status"], "needs_ci_fix")

    def test_review_comments_take_priority_over_ci_failures(self):
        snapshot = self.snapshot(make_pull_request(unresolved=True, failing_check=True))
        self.assertEqual(snapshot["status"], "needs_review")

    def test_clean_green_pr(self):
        snapshot = self.snapshot(make_pull_request())
        self.assertEqual(snapshot["status"], "awaiting_final_test")


class ThreadPolicyTests(unittest.TestCase):
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
                "last_review_status": "awaiting_final_test",
                "last_review_comment_at": None,
                "pending_copilot_review": 0,
                "unresolved_thread_count": 0,
                "failing_check_count": 0,
                "unresolved_threads_json": "[]",
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
            }
        )

    def test_rejects_reuse_for_another_active_pr(self):
        self.add_record(key="repo-pr-1", thread_id="thread-1", active=True)
        with self.assertRaises(pr_review_common.ScriptError):
            pr_review_coordinator.assert_thread_available("thread-1", "repo-pr-2")

    def test_allows_reuse_after_prior_pr_is_inactive(self):
        self.add_record(key="repo-pr-1", thread_id="thread-1", active=False)
        pr_review_coordinator.assert_thread_available("thread-1", "repo-pr-2")


if __name__ == "__main__":
    unittest.main()
