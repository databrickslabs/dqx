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

app-build: app-install
	cd app && \
	  UV_BUILD_CONSTRAINT=$(CURDIR)/app/.build-constraints.txt \
	  UV_REQUIRE_HASHES=1 \
	  $(UV_RUN) apx build

app-start-dev: app-build
	cd app && $(UV_RUN) apx dev start

app-stop-dev:
	cd app && $(UV_RUN) apx dev stop

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
.PHONY: all clean dev lint fmt test integration e2e perf anomaly coverage combine-coverage docs-build docs-serve-dev docs-install docs-serve docs-clean app-install app-build app-start-dev app-stop-dev fork-sync build lock-dependencies lock-app-dependencies
