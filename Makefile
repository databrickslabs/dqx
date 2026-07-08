# Set ``make help`` as the default so a bare ``make`` is safe and shows
# usage instead of running the destructive ``all`` pipeline (which deletes
# .venv and re-runs the full test suite). Override with explicit targets.
.DEFAULT_GOAL := help

# Prevent uv from modifying the lock file. UV_FROZEN skips resolution entirely,
# which is required because the lock file uses public PyPI URLs while the actual
# index may be an internal proxy. Use `make lock-dependencies` to update the lock file.
export UV_FROZEN := 1
# Ensure that hatchling is pinned when builds are needed.
export UV_BUILD_CONSTRAINT := .build-constraints.txt

UV_RUN := uv run --exact --all-extras
UV_TEST := $(UV_RUN) pytest -n 10 --timeout 60 --durations 20

# ``make help`` parses ``##`` annotations next to each target and ``##@``
# section headers so the listing stays in sync with the Makefile
# automatically. Adding a new target without a ``## …`` description
# silently omits it from the help output — that's intentional, it's a
# nudge to write one before contributing the target.
help: ## Show this help with one-line descriptions for each target
	@awk 'BEGIN {FS = ":[^#]*## "} /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5); next } /^[a-zA-Z_][a-zA-Z0-9_-]*:[^#]*## / { printf "  \033[36m%-26s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

##@ Default pipeline

all: clean lint fmt test coverage ## Run clean → lint → fmt → test → coverage

##@ Cleanup

clean: docs-clean ## Remove .venv, caches, coverage data, and docs build artifacts
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml ./mlruns
	find . -name '__pycache__' -print0 | xargs -0 rm -fr

##@ Setup

dev: ## Install Python deps via uv sync (creates .venv with all extras)
	uv sync --all-extras

##@ Format & lint

lint: ## Check Python without modifying (black --check, ruff, mypy, pylint)
	$(UV_RUN) black --check .
	$(UV_RUN) ruff check .
	$(UV_RUN) mypy .
	$(UV_RUN) pylint --output-format=colorized -j 0 src tests

fmt: ## Format and auto-fix Python (black, ruff --fix, mypy, pylint, docs URL refresh)
	$(UV_RUN) black .
	$(UV_RUN) ruff check . --fix
	$(UV_RUN) mypy .
	$(UV_RUN) pylint --output-format=colorized -j 0 src tests
	$(UV_RUN) python docs/dqx/update_github_urls.py

##@ Tests (DQX library)

test: ## Run unit tests (writes coverage-unit.xml)
	$(UV_TEST) --cov --cov-report=xml:coverage-unit.xml tests/unit/

integration: ## Run integration tests (long timeout, requires Databricks workspace auth)
	$(UV_TEST) --timeout 1200 --cov --cov-report=xml tests/integration/

e2e: ## Run end-to-end tests (long timeout, requires Databricks workspace auth)
	$(UV_TEST) --timeout 1200 --cov --cov-report=xml tests/e2e/

perf: ## Run performance benchmarks (long timeout)
	$(UV_TEST) --timeout 600 --cov --cov-report=xml tests/perf/

anomaly: ## Run anomaly integration tests (long timeout, with reruns)
	$(UV_RUN) pytest tests/integration_anomaly/ -v -n 10 --timeout 1200 --durations 20 --reruns 2 --reruns-delay 5

coverage: ## Run all tests (excl. e2e/perf) and open HTML coverage report
	$(UV_TEST) --ignore=tests/e2e --ignore=tests/perf --cov --cov-report=html tests/
	open htmlcov/index.html

combine-coverage: ## Combine xdist worker coverage data into coverage-combined.xml
	# pytest-cov combines xdist worker files at session end, so there may be
	# no parallel .coverage.* files left for `coverage combine` to merge.
	# Tolerate that — the single .coverage file is still usable for the XML
	# conversion. Leading `-` makes `make` ignore combine's non-zero exit.
	-$(UV_RUN) coverage combine
	$(UV_RUN) coverage xml -o coverage-combined.xml
	$(UV_RUN) coverage erase

##@ Documentation

docs-install: ## Install docs site dependencies (yarn --frozen-lockfile)
	yarn --cwd docs/dqx install --frozen-lockfile

docs-build: ## Build the documentation site (pydoc-markdown + docusaurus build)
	$(UV_RUN) --group docs pydoc-markdown
	yarn --cwd docs/dqx build

docs-serve-dev: ## Run docusaurus dev server with hot reload
	$(UV_RUN) --group docs pydoc-markdown
	yarn --cwd docs/dqx start

docs-serve: docs-build ## Build and serve the docs site (production preview)
	$(UV_RUN) --group docs pydoc-markdown
	yarn --cwd docs/dqx serve

docs-clean: ## Remove docs build, .docusaurus, and generated API reference
	rm -rf docs/dqx/build docs/dqx/.docusaurus docs/dqx/.cache
	find docs/dqx/docs/reference/api -mindepth 1 -not -name 'index.mdx' -exec rm -rf {} +

##@ App development (DQX Studio)

app-install: ## Install app frontend dependencies (yarn --frozen-lockfile)
	yarn --cwd app install --frozen-lockfile

app-build: ## Build app: openapi → orval → vite (CI parity)
	cd app && $(UV_RUN) python scripts/build_app.py

# Start the local dev loop (foreground). ``scripts/dev.py`` spawns
# uvicorn (FastAPI, port 9002) and vite (port 9001) and wires vite's
# built-in proxy so the documented http://localhost:9001 URL works
# end-to-end. Ctrl+C cleanly terminates both children.
#
# We depend on ``app-build`` to ensure ``_metadata.py``, the typed
# UI client (``api.ts``), and the UI bundle exist before the dev
# servers come up — otherwise vite would fail to import them.
app-start-dev: app-build ## Start uvicorn + vite locally on http://localhost:9001 (foreground)
	cd app && $(UV_RUN) python $(CURDIR)/app/scripts/dev.py

# Stop any dev servers started in another shell. The pkill scopes are
# narrow enough that they only match THIS project's processes.
app-stop-dev: ## Stop dev servers started by app-start-dev (any shell)
	-pkill -9 -f "$(CURDIR)/app/scripts/dev.py"
	-pkill -f "$(CURDIR)/app/node_modules/.bin/vite"
	-pkill -f "uvicorn databricks_labs_dqx_app.backend.app:app"

# Regenerate ``src/databricks_labs_dqx_app/ui/lib/api.ts`` from the
# current FastAPI app's OpenAPI schema. Run this after editing pydantic
# models or adding new routes so the typed React Query client picks up
# the changes without a full ``make app-build``. ``uvicorn --reload``
# inside ``app-start-dev`` already handles backend hot reload; this
# target closes the loop for the UI's typed-client layer.
app-regen-api: ## Regenerate UI typed client (api.ts) from current OpenAPI schema
	cd app && $(UV_RUN) python -c "import json; \
from databricks_labs_dqx_app.backend.app import app; \
open('.build/openapi.json', 'w').write(json.dumps(app.openapi(), indent=2))"
	cd app && ./node_modules/.bin/orval

# Type-check both languages of the app — TypeScript front-end and
# Python backend — by invoking ``tsc -b`` and ``basedpyright`` directly,
# so a future Vite / basedpyright upgrade doesn't require coordinating
# with any wrapper tool. The TypeScript pass uses incremental mode
# (``-b``) so subsequent runs only re-check changed files; the Python
# pass runs basedpyright at ``error`` level only (per the existing
# pyproject configuration excluding tests, see [tool.basedpyright]).
app-check: ## Type-check app: tsc -b (TypeScript) + basedpyright (Python) + bun UI unit tests
	@echo "🔍 Checking TypeScript..."
	cd app && bun run tsc -b --incremental
	@echo "🔍 Checking Python..."
	cd app && $(UV_RUN) basedpyright --level error
	@$(MAKE) app-test-ui

# Front-end unit tests (bun's built-in test runner — runs *.test.ts natively,
# no extra config). Kept fast + dependency-free so it can run inside app-check.
app-test-ui: ## Run app UI unit tests (bun test)
	@echo "🧪 Testing UI (bun test)..."
	cd app && bun test src/databricks_labs_dqx_app/ui

# Run the app's backend unit-test suite (pytest, no Databricks dependencies).
# Usage:  make app-test            # run everything
#         make app-test K=expr     # forward -k filter to pytest
#         make app-test COV=1      # also produce coverage XML report
#
# The sync line cd's once and groups the fallback in a subshell. Without
# the parentheses, ``A && B || cd app && C`` parses left-to-right as
# ``((A && B) || cd app) && C`` (POSIX: && and || share precedence, left-
# associative), so a failure in B re-runs ``cd app`` from inside ``dqx/app``,
# fails, and aborts. The parens isolate the OR fallback to just the sync
# step. ``--extra all`` is best-effort: it currently doesn't exist on this
# pyproject, so the fallback always wins; the structure is preserved so
# adding an ``all`` extra later "just works".
app-test: ## Run app backend pytest suite (K=<expr> filter, COV=1 for coverage)
	cd app && (uv sync --group test --extra all 2>/dev/null || uv sync --group test)
	cd app && $(UV_RUN) --group test pytest tests/ \
	  $(if $(K),-k "$(K)") \
	  $(if $(COV),--cov=src/databricks_labs_dqx_app/backend --cov-report=term-missing --cov-report=xml:coverage-app.xml)

##@ App deploy (require PROFILE=<databricks-profile>; most also need TARGET=<bundle-target>)

# Minimum Databricks CLI version required to deploy. The ``postgres_roles``
# resource in ``app/databricks.yml`` (one-button Lakebase provisioning) was
# added in CLI v1.4.0 (https://github.com/databricks/cli/pull/5467); older
# CLIs reject the unknown field at ``bundle validate`` with a confusing
# error deep inside the deploy. Fail fast here with an actionable message.
DATABRICKS_MIN_VERSION := 1.4.0

# Preflight: assert the CLI exists and is new enough. Used as a prerequisite
# of app-deploy / app-bind so the version gate runs before any build or
# network call. The sort -V trick: the smallest of {MIN, installed} equals
# MIN exactly when installed >= MIN (handles 1.10 > 1.4 unlike a string sort).
app-check-cli: ## Verify the Databricks CLI meets the minimum version for deploy
	@command -v databricks >/dev/null 2>&1 || \
	  { echo "ERROR: 'databricks' CLI not found on PATH. Install: https://docs.databricks.com/dev-tools/cli/install.html"; exit 1; }
	@ver=$$(databricks --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1); \
	  if [ -z "$$ver" ]; then \
	    echo "ERROR: could not parse a version from 'databricks --version'."; exit 1; \
	  fi; \
	  if [ "$$(printf '%s\n%s\n' "$(DATABRICKS_MIN_VERSION)" "$$ver" | sort -V | head -1)" != "$(DATABRICKS_MIN_VERSION)" ]; then \
	    echo "ERROR: Databricks CLI v$$ver is too old; v$(DATABRICKS_MIN_VERSION)+ is required to deploy DQX Studio."; \
	    echo "       app/databricks.yml uses the 'postgres_roles' resource (one-button Lakebase),"; \
	    echo "       which older CLIs reject at 'bundle validate' with an unknown-field error."; \
	    echo "       Upgrade:  brew upgrade databricks   (or https://docs.databricks.com/dev-tools/cli/install.html)"; \
	    exit 1; \
	  fi; \
	  echo "✓ Databricks CLI v$$ver (>= $(DATABRICKS_MIN_VERSION))"

# Full deploy: build, bundle deploy, and start the app. ``bundle deploy``
# creates the SQL warehouse, schemas, volume, Lakebase project + endpoint +
# app-SP Postgres role, and applies all Unity Catalog grants natively — there
# is no post-deploy grant script and no one-time bind step.
#
# ONE-TIME prerequisite per catalog (the bundle does not manage the pre-existing
# catalog, so it cannot grant catalog-level access): grant USE CATALOG on the
# chosen catalog to the app SP, the task-runner SP, and ``account users``.
# See app/DEPLOYMENT.md.
#
# Usage: make app-deploy PROFILE=my-profile TARGET=dev
#        make app-deploy PROFILE=my-profile TARGET=dev \
#                        BUNDLE_VARS='--var=catalog_name=foo'
#
# BUNDLE_VARS forwards arbitrary ``--var key=value`` arguments to ``bundle
# deploy`` and ``bundle run``.
#
# FORCE=1 appends ``--force`` to ``bundle deploy``. Use it when the deploy
# aborts because a resource was modified in the workspace UI since the last
# deploy (e.g. "dashboard ... has been modified remotely") — ``--force``
# overwrites the remote copy with the bundle's local definition. This DROPS
# any in-UI edits, so only set it once you've confirmed the local version is
# the one you want to ship.
app-deploy: app-check-cli app-build ## Build, deploy bundle, and start app (FORCE=1 to overwrite remote edits)
	@test -n "$(PROFILE)" || (echo "Usage: make app-deploy PROFILE=<databricks-profile> TARGET=<bundle-target>"; exit 1)
	@test -n "$(TARGET)" || (echo "Usage: make app-deploy PROFILE=<databricks-profile> TARGET=<bundle-target>"; exit 1)
	cd app && databricks bundle deploy -p $(PROFILE) -t $(TARGET) $(if $(FORCE),--force) $(BUNDLE_VARS)
	cd app && databricks bundle run $(APP_NAME) -p $(PROFILE) -t $(TARGET) $(BUNDLE_VARS)

APP_NAME ?= dqx-studio

##@ Build & lockfiles

build: ## Build sdist + wheel (with --require-hashes against build-constraints)
	uv build --require-hashes --build-constraints=.build-constraints.txt

# Regenerate app lockfiles (uv.lock, .build-constraints.txt) and
# scrub private-proxy URLs so the committed files resolve against whatever
# registry the install environment is configured for (JFrog in CI, public in fork PRs).
lock-app-dependencies: export UV_FROZEN := 0
lock-app-dependencies: ## Regenerate app/uv.lock, app/yarn.lock, app/.build-constraints.txt
	rm -f app/bun.lock app/package-lock.json
	yarn --cwd app install
	perl -ni -e 'print unless /^  resolved /' app/yarn.lock
	cd app && uv lock --exclude-newer "7 days"
	perl -pi -e 's|registry = "https://[^"]*"|registry = "https://pypi.org/simple"|g' app/uv.lock
	$(UV_RUN) --group yq tomlq -r '.["build-system"].requires[]' app/pyproject.toml | \
	  uv pip compile --generate-hashes --universal --no-header - > app/build-constraints-new.txt
	mv app/build-constraints-new.txt app/.build-constraints.txt

lock-dependencies: export UV_FROZEN := 0
lock-dependencies: ## Regenerate top-level uv.lock and .build-constraints.txt
	uv lock --exclude-newer "7 days"
	$(UV_RUN) --group yq tomlq -r '.["build-system"].requires[]' pyproject.toml | \
	  uv pip compile --generate-hashes --universal --no-header - > build-constraints-new.txt
	mv build-constraints-new.txt .build-constraints.txt
	# Normalize the lock so contributors inside Databricks (private proxy) and outside (public PyPI)
	# produce an identical file. A proxy mirrors PyPI with identical paths, so rewrite the registry
	# index and every per-package "/packages/..." download URL to the public hosts. Also drop the
	# "size" field: the private proxy never reports it, so it is the only form both can reproduce.
	perl -pi -e 's|registry = "https://[^"]*"|registry = "https://pypi.org/simple"|g; s|url = "https://[^/"]+/packages/|url = "https://files.pythonhosted.org/packages/|g; s|, size = \d+||g' uv.lock

##@ Misc

# Sync fork PR to branch in main repo for CI testing (acceptance/anomaly/perf run on non-fork PRs).
# Usage: make fork-sync PR=123
fork-sync: ## Mirror a fork PR to a branch in the main repo for full CI (PR=<number>)
	@test -n "$(PR)" || (echo "Usage: make fork-sync PR=<number>"; exit 1)
	./.github/scripts/fork-sync-pr.sh $(PR)

.DEFAULT: all
.PHONY: help all clean dev lint fmt test integration e2e perf anomaly coverage combine-coverage docs-build docs-serve-dev docs-install docs-serve docs-clean app-install app-build app-start-dev app-stop-dev app-regen-api app-check app-test app-test-ui app-check-cli app-deploy fork-sync build lock-dependencies lock-app-dependencies
