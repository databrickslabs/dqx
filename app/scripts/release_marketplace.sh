#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: app/scripts/release_marketplace.sh studio-vX.Y.Z" >&2
  exit 2
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd -- "$script_dir/../.." && pwd -P)"

cd "$repo_root"

# Use the configured package proxy without exposing its URL. build_marketplace.py
# copies the committed, self-contained lock (marketplace_templates/uv.lock) — it
# resolves nothing — so this only lets the task-runner wheel build fetch its build
# backend from the proxy when public PyPI isn't reachable.
if configured_index="$(python3 -m pip config get global.index-url 2>/dev/null)" && [[ -n "$configured_index" ]]; then
  export UV_DEFAULT_INDEX="$configured_index"
fi

exec uv run --frozen python app/scripts/release_marketplace.py --tag "$1"
