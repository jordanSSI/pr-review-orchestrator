# PR Review Coordinator

This workflow keeps PR review follow-up attached to the original Codex thread instead of using Codex automations.

## Install

Install the launcher onto your shell `PATH`:

```bash
/Users/jordan/source/tools/pr-review-orchestrator/install.sh
```

By default this installs a symlink at `/usr/local/bin/pr-review-coordinator`.

If you want a different target directory, set `PR_REVIEW_COORDINATOR_BIN_DIR` first:

```bash
PR_REVIEW_COORDINATOR_BIN_DIR="$HOME/bin" /Users/jordan/source/tools/pr-review-orchestrator/install.sh
```

## Model

1. Do initial work in a normal Codex thread for a repo.
2. When the change is ready, run `pr-review-coordinator handoff ...` from that same thread.
3. The handoff command captures `CODEX_THREAD_ID`, creates or reuses the PR, creates the dedicated PR worktree, and registers `repo + PR + branch + worktree + thread_id`.
4. A separate poller or dashboard process checks GitHub review threads for each tracked PR.
5. When unresolved review feedback changes, it runs `codex exec resume <thread_id> "<follow-up prompt>"`, which sends the work back into the same Codex thread you started with.
6. The resumed thread is instructed to do code changes only in the dedicated PR worktree.
7. When review is clean, the PR stays tracked but idle so it is back with you for final testing. If new comments appear later, the same thread is resumed again.

## Commands

Create branch, commit, PR, worktree, and tracking:

```bash
pr-review-coordinator handoff \
  --repo-root /Users/jordan/source/starshipit-wms \
  --branch feat/example-change \
  --commit-message "Add example change" \
  --pr-title "Add example change" \
  --pr-body "## Summary\n- add example change"
```

Register an already-open PR to the current thread:

```bash
pr-review-coordinator track \
  --repo-root /Users/jordan/source/starshipit-wms \
  --pr 123 \
  --branch feat/example-change
```

Run one non-destructive review poll:

```bash
pr-review-coordinator poll-once --dry-run
```

See tracked PRs:

```bash
pr-review-coordinator status --all
```

Run the lightweight dashboard and background poller:

```bash
pr-review-coordinator serve --host 127.0.0.1 --port 8765 --poll-seconds 300
```

Then open [http://127.0.0.1:8765](http://127.0.0.1:8765).

## Notes

- Thread identity is taken from `CODEX_THREAD_ID` when available.
- If `CODEX_THREAD_ID` is missing, tracking falls back to the most recent unarchived Codex thread for that repo path.
- Tracking state is stored in `./var/pr-review-coordinator.db` inside this repo.
- Codex thread metadata is read from `~/.codex/state_5.sqlite`.
- Worktrees are created under `~/.codex/worktrees/pr-review/<repo>/pr-<number>-<branch>`.
