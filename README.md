# PR Review Coordinator

PR Review Coordinator is a local tool for managing PR handoff, dedicated review worktrees, GitHub polling, and agent-thread-aware follow-up for review and CI work.

It is designed to:
- register a PR against the agent thread that started the work (Codex) or a synthetic thread (Cursor)
- keep review follow-up isolated in a dedicated PR worktree
- poll GitHub for new review activity and completed failing CI
- consider actionable top-level PR comments as steering input
- resume the mapped agent (Codex or Cursor) when follow-up is needed
- surface persisted status, jobs, locks, and telemetry through a local dashboard

**Agent provider:** Use `--provider codex` (default) or `--provider cursor` on `handoff` and `track`. When not provided, the provider defaults to **codex**.

## What It Does

The coordinator maps:
- a repository checkout
- a PR number and branch
- a dedicated PR worktree
- the Codex thread that started the work

It polls GitHub state, records the latest snapshot for each tracked PR, and queues follow-up work when needed. Execution stays targeted at the dedicated PR worktree.

This is useful when:
- you want to keep new feature work in your main checkout
- you want PR review follow-up isolated in a dedicated worktree
- you want repeated PR review loops to stay attached to the original Codex thread

## Requirements

You need:
- `git`
- `gh`
- GitHub auth configured for `gh`
- an agent provider:
  - **codex** (default): `codex` on PATH and Codex thread state in `~/.codex/state_5.sqlite`
  - **cursor**: standalone `agent` on PATH (install via https://cursor.com/install); use `--provider cursor` on `handoff` / `track`

Expected runtime environment:
- the coordinator is run from its own repository
- tracked repos are local git clones
- for Codex: thread metadata is read from `~/.codex/state_5.sqlite`
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
- register the PR against the current agent thread (Codex by default; use `--provider cursor` for Cursor)

Example (defaults to Codex):

```bash
pr-review-coordinator handoff \
  --repo-root /absolute/path/to/your/repo \
  --branch feat/example-change \
  --commit-message "Add example change" \
  --pr-title "Add example change" \
  --pr-body "## Summary\n- add example change"
```

### Register an existing PR

Use `track` when the PR and worktree already exist and you only need to attach them to the current agent thread (Codex by default; use `--provider cursor` for Cursor):

```bash
pr-review-coordinator track \
  --repo-root /absolute/path/to/your/repo \
  --pr 123 \
  --branch feat/example-change
```

### Run the daemon and dashboard

Run both processes together for local development:

```bash
pr-review-coordinator serve --host 127.0.0.1 --port 8765 --poll-seconds 300
```

Then open [http://127.0.0.1:8765](http://127.0.0.1:8765).

For independent restarts:

```bash
pr-review-coordinator daemon --host 127.0.0.1 --port 8765 --poll-seconds 300
pr-review-coordinator web --host 127.0.0.1 --port 8765
```

## Operational Model

1. Do initial work in a normal Codex thread for a repo.
2. When the change is ready, run `pr-review-coordinator handoff ...` from that same thread.
3. The handoff command captures `CODEX_THREAD_ID`, creates or reuses the PR, creates the dedicated PR worktree, and registers `repo + PR + branch + worktree + thread_id`.
4. The daemon queues lightweight per-PR poll jobs that fetch unresolved review feedback, Copilot review state, and completed failing CI.
5. Poll jobs update tracked state and queue follow-up execution only when actionable state changes.
6. Follow-up execution runs the configured agent (default: `codex exec resume <thread_id> "<follow-up prompt>"`; with `--provider cursor` it runs the standalone `agent -p "..." --output-format text` in the PR worktree), sending the work back into the same context you started with.
7. The resumed thread is instructed to do code changes only in the dedicated PR worktree.
8. When review is clean and completed CI failures are gone, the PR stays tracked but idle so it is back with you for final testing. If the orchestrator has already run follow-up on that PR, it reports `awaiting_final_review` to make that handoff explicit. If new comments or completed failures appear later, the same thread is resumed again.

## Agent Notes

Agents using this tool should follow these rules:
- Prefer `handoff` for the first transition from local implementation to PR lifecycle.
- Prefer `track` only when the PR already exists and should be attached to the current thread.
- Use the `pr-review-coordinator` PATH command rather than a machine-specific local path.
- Active PRs must not share the same Codex thread.
- Treat the tracked worktree as the only location for automated review follow-up changes.
- Do not manually edit the coordinator database or lock files.
- If the coordinator reports `busy`, assume another Codex run or manual work is in progress for that worktree and do not force a second run.
- If the coordinator reports `pending_copilot_review`, do not treat the PR as ready for final testing yet.
- If the coordinator reports `copilot_review_cooldown`, Copilot returned a transient review error; the coordinator will wait about 15 minutes and then re-request `copilot-pull-request-reviewer` automatically.
- If the coordinator reports `awaiting_final_review`, the PR is clean and the orchestrator has already completed its follow-up pass; use that as the human handoff point instead of inspecting PR body text.
- Any unresolved GitHub review thread is treated as actionable follow-up, not only Copilot-authored comments.
- Top-level PR conversation comments are also actionable follow-up. When the agent addresses one, it should reply on the PR with a marker comment so the coordinator can stop treating that comment as pending.
- `Untrack + Cleanup` may remove an externally created tracked worktree only after the PR is merged or closed, the worktree is clean, and Git accepts the removal.

## Guidance For Installed Agents

When this tool is installed for personal agent workflows, put shared usage guidance in `~/.codex/AGENTS.md` so it applies across repositories and does not pollute project-specific `AGENTS.md` files.

If a specific repository still needs custom overrides, keep those minimal and repo-specific only.

Recommended wording:

```md
## Pull Request Lifecycle
- Use the `pr-review-coordinator` PATH command for orchestrator actions; do not hard-code a machine-specific local filesystem path in instructions, comments, or PR descriptions.
- Use `pr-review-coordinator handoff ...` as the preferred single-step path for branch, commit, PR, dedicated worktree, and tracking.
- If handoff partially succeeds, switch the main checkout back to the base branch, create or confirm the PR worktree, then run `pr-review-coordinator track ...`.
- The coordinator follows up on both GitHub review feedback and completed failing CI failures.
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
- `--thread-id`: optional explicit Codex thread id (Codex only; ignored when `--provider cursor`)
- `--provider`: agent for follow-up: `codex` (default) or `cursor`; when not provided, defaults to **codex**
- `--worktree-root`: root directory for managed worktrees
- `--worktree-path`: use an existing registered git worktree instead of creating one

### `track`

Registers an already-open PR and worktree against the current agent thread (Codex by default). Use `--provider cursor` to use Cursor for follow-up; when `--provider` is not provided, it defaults to **codex**.

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

Use `--dry-run` to inspect what would happen without queuing or resuming the agent (Codex or Cursor).

### `status`

Lists tracked PRs and current stored state.

```bash
pr-review-coordinator status --all
```

### `serve`

Runs the daemon and web UI together for compatibility.

```bash
pr-review-coordinator serve --host 127.0.0.1 --port 8765 --poll-seconds 300
```

### `daemon`

Runs the job workers, periodic polling scheduler, agent follow-up execution (Codex or Cursor per tracked PR’s provider), and telemetry logging.

### `web`

Serves the dashboard and enqueues requested actions into SQLite.

- The dashboard can browse open PRs for a local repo checkout and queue selected PRs into tracking without using the CLI directly.
- Project suggestions are sourced from tracked repos and recent Codex threads; selecting PRs from the browser queues `track`-style work that creates managed PR worktrees as needed.

### `untrack`

Stops tracking a PR record.

- Managed worktrees can be removed when clean.
- External tracked worktrees are removed only after the PR is merged or closed, the worktree is clean, and Git confirms the removal is safe.
- Otherwise cleanup is downgraded to untrack-only and the reason is recorded.

## Status Meanings

- `needs_review`: unresolved GitHub review feedback or actionable top-level PR comments exist and follow-up work may be needed
- `needs_ci_fix`: completed failing CI checks or statuses exist and follow-up work may be needed
- `pending_copilot_review`: no unresolved threads, but Copilot review is still pending/in progress
- `copilot_review_cooldown`: Copilot returned its transient "unable to review" error; the coordinator is cooling down before re-requesting review automatically
- `awaiting_final_review`: no unresolved review activity, no pending Copilot review request, and no actionable completed CI failure remain after the orchestrator has already run follow-up for this PR
- `awaiting_final_test`: no unresolved review activity, no pending Copilot review request, and no actionable completed CI failure remain, but the orchestrator has not yet run follow-up for this PR
- `busy`: the coordinator intentionally skipped this PR because another run or local work appears to be in progress
- `running`: a follow-up job is currently active for this PR
- `queued`: the latest poll queued follow-up work, or a user action is waiting in the queue
- `idle`: the last poll completed and no immediate execution was needed
- `error`: the last run failed and needs inspection

## Locks And Safety

The coordinator uses persisted per-PR lock files and SQLite queue state to avoid interfering with active review-follow-up work.

Current behavior:
- one persisted lock file is created per tracked PR while follow-up execution is active
- if a lock is already active, the coordinator skips that PR as `busy`
- if the tracked worktree has local changes, the coordinator treats it as `busy` rather than assuming it is safe to reset
- dashboard actions are asynchronous from the browser point of view and flow through a prioritized SQLite job queue
- polling is decoupled from Codex execution: poll jobs update state and enqueue `run-one` instead of running Codex inline
- control actions like `untrack` are higher priority than execution jobs
- jobs and terse telemetry are persisted so the web UI can survive restarts cleanly

## State Files

Operational state lives in:
- `./var/pr-review-coordinator.db`: tracked PR registry
- `./var/locks/`: active run locks

External state read by the coordinator:
- `~/.codex/state_5.sqlite`: Codex thread metadata
- git worktrees under `~/.codex/worktrees/pr-review/...` when managed worktrees are used

## Notes

- **Provider default:** If `--provider` is not provided on `handoff` or `track`, it defaults to **codex**.
- For Codex: thread identity is taken from `CODEX_THREAD_ID` when available; if missing, tracking falls back to the most recent unarchived Codex thread for that repo path.
- For Cursor: the coordinator uses a synthetic thread id; no Codex state is read.
- Tracking state (including per-PR provider) is stored in `./var/pr-review-coordinator.db` inside this repo.
- Codex thread metadata is read from `~/.codex/state_5.sqlite` only when the tracked PR uses provider `codex`.
- Worktrees are created under `~/.codex/worktrees/pr-review/<repo>/pr-<number>-<branch>` by default.
- For Node.js repos: when creating or ensuring a worktree, if the worktree has `package.json` but no `node_modules`, the coordinator symlinks `node_modules` from the main repo so tests and installs can run without a full `npm install` in each worktree. Ensure the main repo has `node_modules` (e.g. run `npm install` there) before handoff or track.
