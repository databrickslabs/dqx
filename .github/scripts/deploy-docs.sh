#!/usr/bin/env bash
#
# Build the Docusaurus site from the latest `main` and deploy it to the
# orphan `gh-pages` branch. GitHub Pages serves the branch root, so the site
# lives at https://databrickslabs.github.io/dqx/.
#
# `gh-pages` is an orphan branch (no shared history with `main`): main carries
# the source `.mdx`, gh-pages carries only the rendered static site. They are
# linked only by the source-SHA tag in each deploy commit message.
#
# A sibling git worktree at ../dqx-gh-pages keeps the gh-pages working tree
# checked out at all times, so deploys never have to switch branches in the
# main checkout. The worktree is created automatically on first run.
#
# Run this from anywhere inside the repo:
#   .github/scripts/deploy-docs.sh
#
# What this script does, in order:
#   1. Stashes any uncommitted local changes (restored at the end).
#   2. Switches to `main` and pulls the latest with --ff-only.
#   3. Runs `make dev` + `make docs-install` so build deps are present
#      (idempotent — fast no-ops when lockfiles are unchanged).
#   4. Builds the site with `make docs-build` (output at docs/dqx/build/).
#   5. Bootstraps `gh-pages` + worktree on first run:
#        - if origin/gh-pages exists, checks it out into the worktree;
#        - otherwise creates a fresh orphan branch and pushes it to origin.
#      All destructive ops are confined to the worktree — main is never touched.
#   6. Replaces the worktree contents with the new build, writes `.nojekyll`
#      (so GitHub Pages does NOT run Jekyll over the pre-rendered HTML),
#      commits with `Deploy site from main@<sha> on <UTC>`, and pushes.
#      Pushes are append-only (no force) — `git log gh-pages` is the deploy log.
#      If the build is identical to the previous deploy, the commit is skipped.
#   7. Restores your original branch and pops the stash via an EXIT trap, so
#      cleanup runs even if the script errors out.
#
# Prerequisites:
#   - GitHub Pages settings (configure once, after first successful run):
#     Source = Deploy from a branch, Branch = gh-pages, Folder = / (root).

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
WORKTREE_DIR="${REPO_ROOT}/../dqx-gh-pages"
BUILD_DIR="${REPO_ROOT}/docs/dqx/build"

cd "${REPO_ROOT}"

# --- Save state so we can restore it at the end ------------------------------

ORIGINAL_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
STASHED=0

cleanup() {
  set +e
  cd "${REPO_ROOT}"
  # symbolic-ref is safe on unborn branches; rev-parse is not.
  current_branch="$(git symbolic-ref --short -q HEAD || true)"
  if [[ -n "${current_branch}" && "${current_branch}" != "${ORIGINAL_BRANCH}" ]]; then
    echo "==> Switching back to ${ORIGINAL_BRANCH}"
    git checkout "${ORIGINAL_BRANCH}"
  fi
  if [[ "${STASHED}" -eq 1 ]]; then
    echo "==> Restoring stashed changes"
    git stash pop
  fi
}
trap cleanup EXIT

# Stash uncommitted work (tracked + untracked) so checkout/pull are safe.
if ! git diff --quiet || ! git diff --cached --quiet || [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
  echo "==> Stashing uncommitted changes"
  git stash push --include-untracked --message "deploy-docs.sh auto-stash $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  STASHED=1
fi

# --- Update main -------------------------------------------------------------

echo "==> Switching to main and pulling latest"
git checkout main
git pull --ff-only origin main

# --- Build -------------------------------------------------------------------

echo "==> Ensuring dependencies are installed"
make dev
make docs-install

echo "==> Building Docusaurus site"
make docs-build

if [[ ! -f "${BUILD_DIR}/index.html" ]]; then
  echo "Error: build did not produce ${BUILD_DIR}/index.html" >&2
  exit 1
fi

# --- Bootstrap gh-pages branch + worktree if missing -------------------------

git fetch origin gh-pages 2>/dev/null || true

if [[ ! -d "${WORKTREE_DIR}" ]]; then
  if git rev-parse --verify --quiet origin/gh-pages > /dev/null; then
    echo "==> Setting up gh-pages worktree from origin/gh-pages"
    git worktree add "${WORKTREE_DIR}" gh-pages
  else
    echo "==> Bootstrapping new orphan gh-pages branch + worktree"
    # Drop any leftover half-created local gh-pages from a previous failed run.
    git branch -D gh-pages 2>/dev/null || true

    # All destructive ops below run inside the new worktree only — the main
    # checkout is never touched.
    if git worktree add --orphan -b gh-pages "${WORKTREE_DIR}" 2>/dev/null; then
      # git 2.42+: --orphan creates an unborn branch directly in the worktree.
      :
    else
      # Older git fallback: create a worktree from main, then turn its branch
      # into an orphan inside the worktree.
      git worktree add -B gh-pages-bootstrap "${WORKTREE_DIR}" main
      (
        cd "${WORKTREE_DIR}"
        git checkout --orphan gh-pages
        git rm -rf . > /dev/null
      )
      git branch -D gh-pages-bootstrap 2>/dev/null || true
    fi

    # Empty the worktree and create the initial commit so the branch becomes a real ref.
    (
      cd "${WORKTREE_DIR}"
      find . -mindepth 1 -maxdepth 1 ! -name '.git' -exec rm -rf {} +
      git commit --allow-empty -m "Initialize gh-pages"
    )
  fi
fi

# --- Sync build into the gh-pages worktree -----------------------------------

echo "==> Replacing gh-pages worktree contents"
# Wipe everything in the worktree except git's own metadata
find "${WORKTREE_DIR}" -mindepth 1 -maxdepth 1 ! -name '.git' -exec rm -rf {} +

cp -R "${BUILD_DIR}/." "${WORKTREE_DIR}/"
touch "${WORKTREE_DIR}/.nojekyll"

# --- Commit and push ---------------------------------------------------------

echo "==> Committing and pushing gh-pages"
cd "${WORKTREE_DIR}"
git add -A

if git diff --cached --quiet; then
  echo "No changes to deploy. gh-pages is already up to date."
else
  SOURCE_SHA="$(git -C "${REPO_ROOT}" rev-parse --short HEAD)"
  git commit -m "Deploy site from main@${SOURCE_SHA} on $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  # -u sets upstream tracking on first push; harmless on subsequent pushes.
  git push -u origin gh-pages
  echo "==> Site will be live at https://databrickslabs.github.io/dqx/ in ~1 minute."
fi

# cleanup() runs via trap and switches back to the original branch + pops stash
