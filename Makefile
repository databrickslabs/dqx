all: clean dev lint fmt test integration perf coverage e2e

clean: docs-clean
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml ./mlruns
	rm -fr **/*.pyc

.venv/bin/python:
	pip install hatch 'virtualenv<21'
	hatch env create
	hatch run pip install ".[llm,pii,datacontract,anomaly]"

dev: .venv/bin/python
	@hatch run which python

lint:
	hatch run verify

fmt:
	hatch run fmt
	hatch run update_github_urls

test:
	hatch run test

integration:
	hatch run integration

e2e:
	hatch run e2e

perf:
	hatch run perf

coverage:
	hatch run test_coverage; open htmlcov/index.html

docs-build:
	hatch run docs:pydoc-markdown
	yarn --cwd docs/dqx build

docs-serve-dev:
	hatch run docs:pydoc-markdown
	yarn --cwd docs/dqx start

docs-install:
	yarn --cwd docs/dqx install --frozen-lockfile

docs-serve: docs-build
	hatch run docs:pydoc-markdown
	yarn --cwd docs/dqx serve

docs-clean:
	rm -rf docs/dqx/build
	rm -rf docs/dqx/.docusaurus docs/dqx/.cache
	find docs/dqx/docs/reference/api -mindepth 1 -not -name 'index.mdx' -exec rm -rf {} +

# Sync fork PR to branch in main repo for CI testing (acceptance/anomaly/perf run on non-fork PRs).
# Usage: make fork-sync PR=123
fork-sync:
	@test -n "$(PR)" || (echo "Usage: make fork-sync PR=<number>"; exit 1)
	./.github/scripts/fork-sync-pr.sh $(PR)
