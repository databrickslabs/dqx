all: clean dev lint fmt test integration perf coverage e2e

clean: docs-clean
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	rm -fr **/*.pyc

.venv/bin/python:
	pip install hatch
	hatch env create
	hatch run pip install ".[llm,pii,datacontract]"

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

integration-all:
	hatch run integration-all

e2e:
	hatch run e2e

perf:
	hatch run perf

coverage:
	hatch run coverage; open htmlcov/index.html

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
