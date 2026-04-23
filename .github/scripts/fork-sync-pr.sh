#!/usr/bin/env bash
#
# Sync a fork PR to a branch in databrickslabs/dqx and open a test PR.
# Acceptance, anomaly, and performance workflows are skipped for PRs from forks.
# This script pushes the fork's code to a branch in the main repo so those
# tests can run on the new PR.
#
# Usage:
#   .github/scripts/fork-sync-pr.sh <PR_NUMBER>
#
# Prerequisites:
#   - gh CLI installed and authenticated (gh auth login)
#   - Run from a clone of databrickslabs/dqx (or your fork with upstream configured)
#
# First run: Creates fork-test/pr-<N> branch and opens a test PR.
# Subsequent runs: Syncs latest changes from the fork PR to the same branch (force push).
#
set -e

UPSTREAM_REPO="databrickslabs/dqx"
BASE_BRANCH="main"

if [ -z "$1" ]; then
  echo "Usage: $0 <PR_NUMBER>"
  echo ""
  echo "Example: $0 123"
  echo ""
  echo "Syncs fork PR #123 to branch fork-test/pr-123 in $UPSTREAM_REPO,"
  echo "creating a test PR so acceptance/anomaly/perf tests can run."
  exit 1
fi

PR_NUMBER="$1"
SYNC_BRANCH="fork-test/pr-${PR_NUMBER}"

# Verify we're in a git repo
if ! git rev-parse --git-dir >/dev/null 2>&1; then
  echo "Error: Not in a git repository. Run from a clone of $UPSTREAM_REPO or your fork."
  exit 1
fi

# Capture the initial branch so we can restore it on exit (success or failure).
# `--show-current` is empty in detached-HEAD state; fall back to the commit SHA in that case.
ORIGINAL_REF=$(git branch --show-current)
if [ -z "$ORIGINAL_REF" ]; then
  ORIGINAL_REF=$(git rev-parse HEAD)
fi
trap 'git checkout --quiet "$ORIGINAL_REF" 2>/dev/null || true' EXIT

# Verify gh is installed
if ! command -v gh >/dev/null 2>&1; then
  echo "Error: gh CLI is required. Install from https://cli.github.com/"
  exit 1
fi

# Ensure upstream remote points to main repo
if ! git remote get-url upstream >/dev/null 2>&1; then
  echo "Adding upstream remote: $UPSTREAM_REPO"
  git remote add upstream "https://github.com/${UPSTREAM_REPO}.git"
fi

UPSTREAM_URL=$(git remote get-url upstream)
if [[ "$UPSTREAM_URL" != *"${UPSTREAM_REPO}"* ]]; then
  echo "Error: upstream remote does not point to $UPSTREAM_REPO"
  echo "  current: $UPSTREAM_URL"
  exit 1
fi

# Fetch and checkout the fork PR branch (PR lives in upstream repo)
echo "Fetching PR #${PR_NUMBER} from fork..."
gh pr checkout "$PR_NUMBER" --repo "$UPSTREAM_REPO"

# Create or update sync branch
git checkout -B "$SYNC_BRANCH"

# Push to upstream (force to overwrite with latest from fork)
echo "Pushing to upstream/${SYNC_BRANCH}..."
git push --force upstream "HEAD:${SYNC_BRANCH}"

# Create test PR if it does not exist
EXISTING=$(gh pr list --repo "$UPSTREAM_REPO" --head "$SYNC_BRANCH" --state open --json number -q '.[0].number' 2>/dev/null || true)
if [ -z "$EXISTING" ]; then
  PR_URL=$(gh pr view "$PR_NUMBER" --repo "$UPSTREAM_REPO" --json url -q '.url')
  PR_TITLE=$(gh pr view "$PR_NUMBER" --repo "$UPSTREAM_REPO" --json title -q '.title')
  TEST_PR_TITLE="Fork test: PR #${PR_NUMBER} - ${PR_TITLE}"
  echo "Creating test PR..."
  gh pr create --repo "$UPSTREAM_REPO" \
    --base "$BASE_BRANCH" \
    --head "$SYNC_BRANCH" \
    --title "$TEST_PR_TITLE" \
    --label "do-not-merge" \
    --label "fork-test" \
    --body "Automated sync from fork PR for CI testing.

Original PR: ${PR_URL}

All tests, including unit and integration tests run on this PR (they are skipped for fork PRs)."
  echo "Test PR created."
else
  echo "Test PR already exists: #${EXISTING}"
  echo "Branch ${SYNC_BRANCH} has been updated with latest changes from fork PR #${PR_NUMBER}."
fi
