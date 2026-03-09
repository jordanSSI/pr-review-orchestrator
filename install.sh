#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_BIN="$SCRIPT_DIR/pr-review-coordinator"

if [[ ! -x "$SOURCE_BIN" ]]; then
  echo "error: missing executable launcher at $SOURCE_BIN" >&2
  exit 1
fi

DEFAULT_TARGET_DIR="/usr/local/bin"
if [[ ! -w "$DEFAULT_TARGET_DIR" ]]; then
  DEFAULT_TARGET_DIR="$HOME/.local/bin"
fi

TARGET_DIR="${PR_REVIEW_COORDINATOR_BIN_DIR:-$DEFAULT_TARGET_DIR}"
TARGET_BIN="$TARGET_DIR/pr-review-coordinator"
SHELL_RC="${PR_REVIEW_COORDINATOR_SHELL_RC:-$HOME/.zshrc}"
LOGIN_SHELL_RC="${PR_REVIEW_COORDINATOR_LOGIN_SHELL_RC:-$HOME/.zprofile}"

mkdir -p "$TARGET_DIR"
ln -sfn "$SOURCE_BIN" "$TARGET_BIN"

if [[ "$TARGET_DIR" == "$HOME/.local/bin" ]]; then
  for rc_file in "$SHELL_RC" "$LOGIN_SHELL_RC"; do
    mkdir -p "$(dirname "$rc_file")"
    touch "$rc_file"
    if ! grep -Fq 'export PATH="$HOME/.local/bin:$PATH"' "$rc_file"; then
      {
        echo
        echo '# Added by pr-review-coordinator installer'
        echo 'export PATH="$HOME/.local/bin:$PATH"'
      } >> "$rc_file"
    fi
  done
fi

echo "Installed pr-review-coordinator to $TARGET_BIN"
if [[ "$TARGET_DIR" == "$HOME/.local/bin" ]]; then
  echo "Ensured $HOME/.local/bin is exported from $SHELL_RC and $LOGIN_SHELL_RC"
fi
echo
echo "Verify:"
echo "  pr-review-coordinator --help"
