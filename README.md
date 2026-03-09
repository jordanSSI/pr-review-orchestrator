# PR Review Coordinator

PR Review Coordinator is a local tool for managing PR handoff, dedicated review worktrees, GitHub review polling, and Codex-thread-aware review follow-up.

It is designed to:
- register a PR against the Codex thread that started the work
- keep review follow-up isolated in a dedicated PR worktree
- poll GitHub for new review activity
- resume the mapped Codex thread when follow-up is needed
- surface status, locks, and progress through a local dashboard

## What It Does

The coordinator maps:
- a repository checkout
- a PR number and branch
- a dedicated PR worktree
- the Codex thread that started the work

It then polls GitHub review activity and, when needed, resumes that same Codex thread with follow-up instructions targeted at the dedicated PR worktree.

This is useful when:
- you want to keep new feature work in your main checkout
- you want PR review follow-up isolated in a dedicated worktree
- you want repeated Copilot review loops to stay attached to the original Codex thread

## Requirements

You need:
- `git`
- `gh`
- `codex`
- GitHub auth configured for `gh`
- a Codex environment that records thread state locally

Expected runtime environment:
- the coordinator is run from its own repository
- tracked repos are local git clones
- Codex thread metadata is available in `~/.codex/state_5.sqlite`
- the coordinator can create or reuse git worktrees

## Install

From the `pr-review-orchestrator` repository:

```bash
./install.sh
```

By default, the installer will:
- use `/usr/local/bin` if it is writable
- otherwise fall back to `~/.local/bin`
- add `~/.local/bin` to `~/.zshrc` and `~/.zprofile` when needed

If you want a different target directory, set `PR_REVIEW_COORDINATOR_BIN_DIR` first:

```bash
PR_REVIEW_COORDINATOR_BIN_DIR="$HOME/bin" ./install.sh
```

Verify the install:

```bash
pr-review-coordinator --help
```

## Quickstart

### New PR handoff

Use `handoff` when your local coding pass is done and you want the coordinator to:
- create or switch to the branch
- commit local changes
- push the branch
- create or reuse the PR
- create or reuse the dedicated PR worktree
- register the PR against the current Codex thread

Example:

```bash
pr-review-coordinator handoff \
  --repo-root /absolute/path/to/your/repo \
  --branch feat/example-change \
  --commit-message "Add example change" \
  --pr-title "Add example change" \
  --pr-body "## Summary\n- add example change"
```

### Register an existing PR

Use `track` when the PR and worktree already exist and you only need to attach them to the current Codex thread:

```bash
pr-review-coordinator track \
  --repo-root /absolute/path/to/your/repo \
  --pr 123 \
  --branch feat/example-change
```

### Watch review follow-up

Run the dashboard and poller:

```bash
pr-review-coordinator serve --host 127.0.0.1 --port 8765 --poll-seconds 300
```

Then open [http://127.0.0.1:8765](http://127.0.0.1:8765).

## Operational Model

1. Do initial work in a normal Codex thread for a repo.
2. When the change is ready, run `pr-review-coordinator handoff ...` from that same thread.
3. The handoff command captures `CODEX_THREAD_ID`, creates or reuses the PR, creates the dedicated PR worktree, and registers `repo + PR + branch + worktree + thread_id`.
4. A separate poller or dashboard process checks GitHub review threads for each tracked PR.
5. When unresolved review feedback changes, it runs `codex exec resume <thread_id> "<follow-up prompt>"`, which sends the work back into the same Codex thread you started with.
6. The resumed thread is instructed to do code changes only in the dedicated PR worktree.
7. When review is clean, the PR stays tracked but idle so it is back with you for final testing. If new comments appear later, the same thread is resumed again.

## Agent Notes

Agents using this tool should follow these rules:
- Prefer `handoff` for the first transition from local implementation to PR lifecycle.
- Prefer `track` only when the PR already exists and should be attached to the current thread.
- Use the `pr-review-coordinator` PATH command rather than a machine-specific local path.
- Treat the tracked worktree as the only location for automated review follow-up changes.
- Do not manually edit the coordinator database or lock files.
- If the coordinator reports `busy`, assume another Codex run or manual work is in progress for that worktree and do not force a second run.
- If the coordinator reports `pending_copilot_review`, do not treat the PR as ready for final testing yet.

## Guidance For Other Repositories

If another repository wants to use this tool, its own `AGENTS.md` should reference the PATH command, not a machine-specific filesystem path.

Recommended wording:

```md
## Pull Request Lifecycle
- Use the `pr-review-coordinator` PATH command for orchestrator actions; do not hard-code a machine-specific local filesystem path in instructions, comments, or PR descriptions.
- Right after the first implementation pass is complete and the user approves PR handoff, put the work into the review cycle by running `pr-review-coordinator track` with the repo-specific `--repo-root`, `--repo-name`, `--pr`, `--branch`, `--thread-id`, and `--worktree-path` arguments.
```

Preferred conventions:
- refer to the command as `pr-review-coordinator`
- describe it as a PATH command
- avoid `/Users/...` examples
- avoid placeholders like `PATH_TO_PR_REVIEW_COORDINATOR` when the installer is expected to put the command on `PATH`

## Command Reference

### `handoff`

Creates or updates the branch/PR handoff and registers tracking.

```bash
pr-review-coordinator handoff \
  --repo-root /absolute/path/to/your/repo \
  --branch feat/example-change \
  --commit-message "Add example change" \
  --pr-title "Add example change" \
  --pr-body "## Summary\n- add example change"
```

Common options:
- `--repo-root`: absolute path to the main repo checkout
- `--repo-name`: optional explicit repo name; otherwise inferred from `origin`
- `--branch`: PR branch name
- `--base-branch`: optional PR base branch
- `--thread-id`: optional explicit Codex thread id
- `--worktree-root`: root directory for managed worktrees
- `--worktree-path`: use an existing registered git worktree instead of creating one

### `track`

Registers an already-open PR and worktree against the current Codex thread.

```bash
pr-review-coordinator track \
  --repo-root /absolute/path/to/your/repo \
  --pr 123 \
  --branch feat/example-change
```

### `poll-once`

Runs one review poll tick.

```bash
pr-review-coordinator poll-once --dry-run
```

Use `--dry-run` to inspect what would happen without resuming Codex.

### `status`

Lists tracked PRs and current stored state.

```bash
pr-review-coordinator status --all
```

### `serve`

Runs the lightweight dashboard and background poller.

```bash
pr-review-coordinator serve --host 127.0.0.1 --port 8765 --poll-seconds 300
```

### `untrack`

Stops tracking a PR record. Optional cleanup only removes managed worktrees.

## Status Meanings

- `needs_review`: unresolved review feedback exists and follow-up work may be needed
- `pending_copilot_review`: no unresolved threads, but Copilot review is still pending/in progress
- `awaiting_final_test`: no unresolved threads and no pending Copilot review request remain
- `busy`: the coordinator intentionally skipped this PR because another run or local work appears to be in progress
- `running`: a poll or follow-up job is currently active
- `idle`: the last poll completed and no immediate action was taken
- `error`: the last run failed and needs inspection

## Locks And Safety

The coordinator uses lock files to avoid interfering with active review-follow-up work.

Current behavior:
- one persisted lock file is created per tracked PR while a run is active
- if a lock is already active, the coordinator skips that PR as `busy`
- if the tracked worktree has local changes, the coordinator treats it as `busy` rather than assuming it is safe to reset
- dashboard actions are asynchronous from the browser point of view

## State Files

Operational state lives in:
- `./var/pr-review-coordinator.db`: tracked PR registry
- `./var/locks/`: active run locks

External state read by the coordinator:
- `~/.codex/state_5.sqlite`: Codex thread metadata
- git worktrees under `~/.codex/worktrees/pr-review/...` when managed worktrees are used

## Notes

- Thread identity is taken from `CODEX_THREAD_ID` when available.
- If `CODEX_THREAD_ID` is missing, tracking falls back to the most recent unarchived Codex thread for that repo path.
- Tracking state is stored in `./var/pr-review-coordinator.db` inside this repo.
- Codex thread metadata is read from `~/.codex/state_5.sqlite`.
- Worktrees are created under `~/.codex/worktrees/pr-review/<repo>/pr-<number>-<branch>`.
