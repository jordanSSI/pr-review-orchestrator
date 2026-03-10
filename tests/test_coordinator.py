import json
import tempfile
import unittest
from pathlib import Path

import pr_review_common
import pr_review_coordinator


def make_pull_request(
    *,
    unresolved=False,
    pending_copilot=False,
    failing_check=False,
    review_author="github-copilot[bot]",
    pr_comments=None,
):
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
        "reviewRequests": {"nodes": review_requests},
        "reviewThreads": {"nodes": review_threads},
        "comments": {"nodes": pr_comments or []},
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

    def test_completed_failing_ci_only(self):
        snapshot = self.snapshot(make_pull_request(failing_check=True))
        self.assertEqual(snapshot["status"], "needs_ci_fix")

    def test_review_comments_take_priority_over_ci_failures(self):
        snapshot = self.snapshot(make_pull_request(unresolved=True, failing_check=True))
        self.assertEqual(snapshot["status"], "needs_review")

    def test_clean_green_pr(self):
        snapshot = self.snapshot(make_pull_request())
        self.assertEqual(snapshot["status"], "awaiting_final_test")

    def test_top_level_pr_comment_triggers_needs_review(self):
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
                        "body": "Merged master and resolved conflicts. <!-- pr-review-coordinator:handled-comment issue-comment-1 -->",
                        "createdAt": "2026-03-09T02:00:00Z",
                        "updatedAt": "2026-03-09T02:00:00Z",
                        "url": "https://example.com/comment-2",
                    },
                ]
            )
        )
        self.assertEqual(snapshot["status"], "awaiting_final_test")
        self.assertEqual(snapshot["actionable_pr_comments"], [])


class WorktreePathTests(unittest.TestCase):
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
            }
        )

    def test_rejects_reuse_for_another_active_pr(self):
        self.add_record(key="repo-pr-1", thread_id="thread-1", active=True)
        with self.assertRaises(pr_review_common.ScriptError):
            pr_review_coordinator.assert_thread_available("thread-1", "repo-pr-2")

    def test_allows_reuse_after_prior_pr_is_inactive(self):
        self.add_record(key="repo-pr-1", thread_id="thread-1", active=False)
        pr_review_coordinator.assert_thread_available("thread-1", "repo-pr-2")


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
        pr_review_coordinator.resolve_thread = lambda repo_root, thread_id: {"id": "thread-418", "title": "PR 418 thread"}
        pr_review_coordinator.assert_thread_available = lambda thread_id, key: None

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


class QueueBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_var_dir = pr_review_coordinator.VAR_DIR
        self.original_locks_dir = pr_review_coordinator.LOCKS_DIR
        self.original_db = pr_review_coordinator.COORDINATOR_DB
        self.original_record_event = pr_review_coordinator.record_event
        self.original_pull_request_snapshot = pr_review_coordinator.pull_request_snapshot
        self.original_run_follow_up = pr_review_coordinator.run_follow_up
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
                }
            )

    def tearDown(self):
        pr_review_coordinator.VAR_DIR = self.original_var_dir
        pr_review_coordinator.LOCKS_DIR = self.original_locks_dir
        pr_review_coordinator.COORDINATOR_DB = self.original_db
        pr_review_coordinator.record_event = self.original_record_event
        pr_review_coordinator.pull_request_snapshot = self.original_pull_request_snapshot
        pr_review_coordinator.run_follow_up = self.original_run_follow_up
        self.temp_dir.cleanup()

    def snapshot(self, status="needs_review", signature="sig-1"):
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
            "pending_copilot_review": False,
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

        def fake_run_follow_up(record, *, dry_run, force_run, job_id):
            captured["record"] = record.key
            captured["dry_run"] = dry_run
            captured["force_run"] = force_run
            captured["job_id"] = job_id
            return {"status": "ok"}

        pr_review_coordinator.run_follow_up = fake_run_follow_up

        pr_review_coordinator.process_job(claimed)

        self.assertEqual(captured, {"record": "repo-pr-1", "dry_run": False, "force_run": True, "job_id": claimed.id})


if __name__ == "__main__":
    unittest.main()
