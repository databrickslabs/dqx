#!/usr/bin/env bash
# Prints the CI boundaries affected by a change as a JSON array, e.g. ["studio","docs"].
#
# Usage: changed-boundaries.sh [BASE]   paths changed in BASE...HEAD (default: origin/main);
#                                       a rename counts its old and new path
#        changed-boundaries.sh -        paths read from stdin, one per line
#        changed-boundaries.sh --all    every boundary
#
# With GITHUB_OUTPUT set, also writes "boundaries=<json>" and "<boundary>=true|false" to it.
set -euo pipefail

BOUNDARIES="core studio anomaly mcp docs"

# First match wins. "all" marks files shared by every boundary; paths matching nothing
# (LICENSE, CHANGELOG.md, AGENTS.md, ...) affect no boundary.
boundary_of() {
  case "$1" in
    pyproject.toml | uv.lock | Makefile | .build-constraints.txt | .github/* | tests/conftest.py) echo all ;;
    app/* | make.ps1) echo studio ;;
    mcp-server/* | tests/integration_mcp/*) echo mcp ;;
    src/databricks/labs/dqx/anomaly/* | tests/integration_anomaly/*) echo anomaly ;;
    docs/* | demos/* | skills/* | .claude-plugin/*) echo docs ;;
    src/* | tests/* | labs.yml | README.md | .codegen.json | .coveragerc | .python-version) echo core ;;
  esac
}

base="${1:-origin/main}"
case "$base" in
  --all) paths="" affected="all" ;;
  -) paths="$(cat)" affected="" ;;
  *) paths="$(git diff --name-only --no-renames "$base...HEAD")" affected="" ;;
esac

while IFS= read -r path; do
  if [ -n "$path" ]; then
    affected="$affected $(boundary_of "$path")"
  fi
done <<< "$paths"

# The other boundaries all build on the core library, so a core change runs everything.
case " $affected " in
  *" all "* | *" core "*) affected="$BOUNDARIES" ;;
esac

json=""
for boundary in $BOUNDARIES; do
  hit=false
  case " $affected " in
    *" $boundary "*) hit=true json="$json${json:+,}\"$boundary\"" ;;
  esac
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "$boundary=$hit" >> "$GITHUB_OUTPUT"
  fi
done

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "boundaries=[$json]" >> "$GITHUB_OUTPUT"
fi
echo "[$json]"
