#!/usr/bin/env bash
# Shared helpers for bash entrypoints.

repo_root() {
  local script_dir
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  printf '%s\n' "$(cd "$script_dir/.." && pwd)"
}

resolve_python() {
  local root="${1:-$(repo_root)}"
  # Prefer the repo virtualenv python when present
  if [ -x "$root/.venv/bin/python" ]; then
    printf '%s\n' "$root/.venv/bin/python"
    return 0
  fi

  # Prefer python 3.10 on systems where it's installed
  if command -v python3.10 >/dev/null 2>&1; then
    command -v python3.10
    return 0
  fi

  # Fall back to python3 then python
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return 0
  fi

  if command -v python >/dev/null 2>&1; then
    command -v python
    return 0
  fi

  echo "No Python interpreter found. Install Python 3.10 or run scripts/setup.sh first." >&2
  return 1
}