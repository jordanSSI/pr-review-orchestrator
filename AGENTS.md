# Agent Instructions

## Pull Request Lifecycle

- Use `prc` as the preferred PATH command for this tool. `pr-review-coordinator` remains supported and is equivalent.
- Prefer `prc handoff ...` for the first transition from local implementation to PR lifecycle.
- Prefer `prc track ...` only when the PR already exists and should be attached to the current thread.
- Agent-authored PR comments should begin with the configured prefix from `~/.codex/pr-review-coordinator.json`, which the installer bootstraps to `[jordanBot]` by default.
- Use a stable canonical checkout as `--repo-root`; do not register a scratch checkout or ephemeral worktree as the repo root.
- Keep automated PR follow-up in the dedicated tracked PR worktree, not the main checkout.
