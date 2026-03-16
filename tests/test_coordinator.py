import json
import os
import sqlite3
import tempfile
import unittest
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
    review_author="github-copilot[bot]",
    pr_comments=None,
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


class ProjectImportRenderingTests(unittest.TestCase):
    def test_tracked_rows_are_selectable_and_offer_fresh_thread(self):
        markup = pr_review_coordinator.render_project_import_section(
            project_candidates=[],
            selected_project_root="/tmp/repo",
            selected_provider="codex",
            browse_result={
                "repo_root": "/tmp/repo",
                "repo_name": "repo",
                "prs": [
                    {
                        "number": 42,
                        "url": "https://example.com/pr/42",
                        "title": "Tracked PR",
                        "headRefName": "branch-42",
                        "baseRefName": "main",
                        "isDraft": False,
                        "tracked": True,
                        "tracked_status": "needs_review",
                        "tracked_active": True,
                        "tracked_thread_id": "thread-42",
                        "tracked_thread_title": "Thread 42",
                    }
                ],
            },
            recent_threads=[{"id": "thread-99", "title": "Latest", "in_use_by": None}],
            browse_error=None,
            notice=None,
        )

        self.assertIn('name="selected_pr" value="42"', markup)
        self.assertIn('name="thread_id_42"', markup)
        self.assertIn('value="thread-42"', markup)
        self.assertIn('name="new_thread_42"', markup)


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
        pr_review_coordinator.resolve_thread = lambda repo_root, thread_id, provider=None: {"id": "thread-418", "title": "PR 418 thread"}
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
        pr_review_coordinator.register_tracking = self.original_register_tracking
        pr_review_coordinator.resolve_selected_thread = self.original_resolve_selected_thread
        pr_review_coordinator.create_codex_thread = self.original_create_codex_thread
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

    def test_clean_pr_after_agent_run_becomes_awaiting_final_review(self):
        pr_review_coordinator.pull_request_snapshot = lambda *args, **kwargs: self.snapshot(
            status="awaiting_final_test",
            signature="sig-clean",
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


if __name__ == "__main__":
    unittest.main()
