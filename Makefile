all: clean lint fmt test coverage

# Prevent uv from modifying the lock file. UV_FROZEN skips resolution entirely,
# which is required because the lock file uses public PyPI URLs while the actual
# index may be an internal proxy. Use `make lock-dependencies` to update the lock file.
export UV_FROZEN := 1
# Ensure that hatchling is pinned when builds are needed.
export UV_BUILD_CONSTRAINT := .build-constraints.txt

UV_RUN := uv run --exact --all-extras
UV_TEST := $(UV_RUN) pytest -n 10 --timeout 60 --durations 20

clean: docs-clean
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml ./mlruns
	find . -name '__pycache__' -print0 | xargs -0 rm -fr

dev:
	uv sync --all-extras

lint:
	$(UV_RUN) black --check .
	$(UV_RUN) ruff check .
	$(UV_RUN) mypy .
	$(UV_RUN) pylint --output-format=colorized -j 0 src tests

fmt:
	$(UV_RUN) black .
	$(UV_RUN) ruff check . --fix
	$(UV_RUN) mypy .
	$(UV_RUN) pylint --output-format=colorized -j 0 src tests
	$(UV_RUN) python docs/dqx/update_github_urls.py

test:
	$(UV_TEST) --cov --cov-report=xml:coverage-unit.xml tests/unit/

integration:
	$(UV_TEST) --timeout 1200 --cov --cov-report=xml tests/integration/

e2e:
	$(UV_TEST) --timeout 1200 --cov --cov-report=xml tests/e2e/

perf:
	$(UV_TEST) --timeout 600 --cov --cov-report=xml tests/perf/

anomaly:
	$(UV_RUN) pytest tests/integration_anomaly/ -v -n 10 --timeout 1200 --durations 20 --reruns 2 --reruns-delay 5

coverage:
	$(UV_TEST) --ignore=tests/e2e --ignore=tests/perf --cov --cov-report=html tests/
	open htmlcov/index.html

combine-coverage:
	# pytest-cov combines xdist worker files at session end, so there may be
	# no parallel .coverage.* files left for `coverage combine` to merge.
	# Tolerate that — the single .coverage file is still usable for the XML
	# conversion. Leading `-` makes `make` ignore combine's non-zero exit.
	-$(UV_RUN) coverage combine
	$(UV_RUN) coverage xml -o coverage-combined.xml
	$(UV_RUN) coverage erase

docs-build:
	$(UV_RUN) --group docs pydoc-markdown
	yarn --cwd docs/dqx build

docs-serve-dev:
	$(UV_RUN) --group docs pydoc-markdown
	yarn --cwd docs/dqx start

docs-install:
	yarn --cwd docs/dqx install --frozen-lockfile

docs-serve: docs-build
	$(UV_RUN) --group docs pydoc-markdown
	yarn --cwd docs/dqx serve

docs-clean:
	rm -rf docs/dqx/build docs/dqx/.docusaurus docs/dqx/.cache
	find docs/dqx/docs/reference/api -mindepth 1 -not -name 'index.mdx' -exec rm -rf {} +

app-install:
	yarn --cwd app install --frozen-lockfile

app-build:
	cd app && \
	  UV_BUILD_CONSTRAINT=.build-constraints.txt \
	  UV_REQUIRE_HASHES=1 \
	  $(UV_RUN) python scripts/build_app.py && \
	  uv build ../ --wheel --out-dir .build/ && \
	  uv build tasks/ --wheel --out-dir .build/tasks/

# Start the local dev loop (foreground). ``scripts/dev.py`` spawns
# uvicorn (FastAPI, port 9002) and vite (port 9001) and wires vite's
# built-in proxy so the documented http://localhost:9001 URL works
# end-to-end. Ctrl+C cleanly terminates both children.
#
# We depend on ``app-build`` to ensure ``_metadata.py``, the typed
# UI client (``api.ts``), and the UI bundle exist before the dev
# servers come up — otherwise vite would fail to import them.
app-start-dev: app-build
	cd app && $(UV_RUN) python scripts/dev.py

# Stop any dev servers started in another shell. The pkill scopes are
# narrow enough that they only match THIS project's processes.
app-stop-dev:
	-pkill -f "$(CURDIR)/app/scripts/dev.py"
	-pkill -f "$(CURDIR)/app/node_modules/.bin/vite"
	-pkill -f "uvicorn databricks_labs_dqx_app.backend.app:app"

# Regenerate ``src/databricks_labs_dqx_app/ui/lib/api.ts`` from the
# current FastAPI app's OpenAPI schema. Run this after editing pydantic
# models or adding new routes so the typed React Query client picks up
# the changes without a full ``make app-build``. ``uvicorn --reload``
# inside ``app-start-dev`` already handles backend hot reload; this
# target closes the loop for the UI's typed-client layer.
app-regen-api:
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
app-check:
	@echo "🔍 Checking TypeScript..."
	cd app && bun run tsc -b --incremental
	@echo "🔍 Checking Python..."
	cd app && $(UV_RUN) basedpyright --level error

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
app-test:
	cd app && (uv sync --group test --extra all 2>/dev/null || uv sync --group test)
	cd app && $(UV_RUN) --group test pytest tests/ \
	  $(if $(K),-k "$(K)") \
	  $(if $(COV),--cov=src/databricks_labs_dqx_app/backend --cov-report=term-missing --cov-report=xml:coverage-app.xml)

# Grant Unity Catalog permissions after bundle deploy.
#
# Usage: make app-grant-permissions PROFILE=my-profile
#        make app-grant-permissions PROFILE=my-profile TARGET=dev \
#                                   BUNDLE_VARS='--var=catalog_name=foo'
#
# BUNDLE_VARS must match the overrides used at deploy time. The script
# reads bundle variables to discover which catalog / schema / volume to
# grant on, so an unforwarded override would point the GRANTs at the
# wrong objects.
app-grant-permissions:
	@test -n "$(PROFILE)" || (echo "Usage: make app-grant-permissions PROFILE=<databricks-profile> [TARGET=<bundle-target>]"; exit 1)
	app/scripts/post_deploy_grants.sh -p $(PROFILE) $(if $(TARGET),-t $(TARGET)) $(if $(BUNDLE_VARS),-- $(BUNDLE_VARS))

# Adopt pre-existing storage resources into bundle management.
#
# Use ONCE per target on workspaces where the schemas / volume /
# Lakebase instance already exist (e.g. from a previous bootstrap-
# script deploy, or from manual creation). The app's Postgres schema
# inside ``databricks_postgres`` is not bundle-managed and does not
# need binding — the app re-creates it on first connection if missing.
# Without binding, ``databricks bundle deploy`` would try to CREATE
# them and fail with "already exists" / "Instance name is not unique".
#
# Bind is idempotent at the CLI level — re-binding the same resource
# is a no-op. Skip this target on fresh workspaces; ``bundle deploy``
# creates the resources directly.
#
# Usage: make app-bind PROFILE=my-profile TARGET=dev
#        make app-bind PROFILE=my-profile TARGET=dev \
#                      BUNDLE_VARS='--var=lakebase_instance_name=<fresh-name>'
#
# BUNDLE_VARS forwards arbitrary ``--var key=value`` arguments through
# to the bundle CLI. Use the same override here as you intend to pass
# to ``make app-deploy`` — the bind step reads the resolved bundle
# variables to know which instance/schema name to bind to, so an
# override applied only at deploy time would bind the wrong resource.
app-bind:
	@test -n "$(PROFILE)" || (echo "Usage: make app-bind PROFILE=<databricks-profile> TARGET=<bundle-target>"; exit 1)
	@test -n "$(TARGET)" || (echo "Usage: make app-bind PROFILE=<databricks-profile> TARGET=<bundle-target>"; exit 1)
	app/scripts/bind_resources.sh -p $(PROFILE) -t $(TARGET) $(if $(BUNDLE_VARS),-- $(BUNDLE_VARS))

# Full deploy: build, bundle deploy (creates storage on fresh
# workspaces, updates managed resources otherwise), grant permissions
# to the app SP, and start the app. Run ``make app-bind`` once before
# the FIRST deploy on a workspace where the storage was previously
# provisioned out-of-band — otherwise the bundle will try to CREATE
# the existing resources and fail.
#
# Usage: make app-deploy PROFILE=my-profile TARGET=dev
#        make app-deploy PROFILE=my-profile TARGET=dev \
#                        BUNDLE_VARS='--var=lakebase_instance_name=<fresh-name>'
#
# BUNDLE_VARS forwards arbitrary ``--var key=value`` arguments to
# every step of the deploy: ``bundle deploy``, ``post_deploy_grants.sh``,
# and ``bundle run``. Threading the same overrides through all three is
# load-bearing — the grants script re-runs ``bundle validate`` to
# discover the deployed catalog / schema / volume, and without the
# overrides it would issue GRANTs on the bundle defaults instead of
# the resources actually deployed.
#
# The common use case is overriding ``lakebase_instance_name`` when the
# original name is locked by Lakebase's ~7-day soft-delete retention
# after a manual delete (the deploy errors out with "Instance name is
# not unique" otherwise). Per-deploy CLI overrides keep ``databricks.yml``
# clean.
app-deploy: app-build
	@test -n "$(PROFILE)" || (echo "Usage: make app-deploy PROFILE=<databricks-profile> TARGET=<bundle-target>"; exit 1)
	@test -n "$(TARGET)" || (echo "Usage: make app-deploy PROFILE=<databricks-profile> TARGET=<bundle-target>"; exit 1)
	cd app && databricks bundle deploy -p $(PROFILE) -t $(TARGET) $(BUNDLE_VARS)
	app/scripts/post_deploy_grants.sh -p $(PROFILE) -t $(TARGET) $(if $(BUNDLE_VARS),-- $(BUNDLE_VARS))
	cd app && databricks bundle run $(APP_NAME) -p $(PROFILE) -t $(TARGET) $(BUNDLE_VARS)

APP_NAME ?= dqx-studio

# Regenerate app lockfiles (uv.lock, .build-constraints.txt) and
# scrub private-proxy URLs so the committed files resolve against whatever
# registry the install environment is configured for (JFrog in CI, public in fork PRs).
lock-app-dependencies: export UV_FROZEN := 0
lock-app-dependencies:
	rm -f app/bun.lock app/package-lock.json
	yarn --cwd app install
	perl -ni -e 'print unless /^  resolved /' app/yarn.lock
	cd app && uv lock --exclude-newer "7 days"
	perl -pi -e 's|registry = "https://[^"]*"|registry = "https://pypi.org/simple"|g' app/uv.lock
	$(UV_RUN) --group yq tomlq -r '.["build-system"].requires[]' app/pyproject.toml | \
	  uv pip compile --generate-hashes --universal --no-header - > app/build-constraints-new.txt
	mv app/build-constraints-new.txt app/.build-constraints.txt

# Sync fork PR to branch in main repo for CI testing (acceptance/anomaly/perf run on non-fork PRs).
# Usage: make fork-sync PR=123
fork-sync:
	@test -n "$(PR)" || (echo "Usage: make fork-sync PR=<number>"; exit 1)
	./.github/scripts/fork-sync-pr.sh $(PR)

build:
	uv build --require-hashes --build-constraints=.build-constraints.txt

lock-dependencies: export UV_FROZEN := 0
lock-dependencies:
	uv lock --exclude-newer "7 days"
	$(UV_RUN) --group yq tomlq -r '.["build-system"].requires[]' pyproject.toml | \
	  uv pip compile --generate-hashes --universal --no-header - > build-constraints-new.txt
	mv build-constraints-new.txt .build-constraints.txt
	perl -pi -e 's|registry = "https://[^"]*"|registry = "https://pypi.org/simple"|g' uv.lock

.DEFAULT: all
.PHONY: all clean dev lint fmt test integration e2e perf anomaly coverage combine-coverage docs-build docs-serve-dev docs-install docs-serve docs-clean app-install app-build app-start-dev app-stop-dev app-check app-test app-grant-permissions app-bind app-deploy fork-sync build lock-dependencies lock-app-dependencies
