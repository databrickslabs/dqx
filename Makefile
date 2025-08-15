all: clean dev lint fmt test integration coverage e2e

clean: docs-clean
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	rm -fr **/*.pyc

.venv/bin/python:
	pip install hatch
	hatch env create

dev: .venv/bin/python
	@hatch run which python

lint:
	hatch run verify

fmt:
	hatch run fmt
	hatch run update_github_urls
	hatch run extract_yaml_checks_examples

test:
	hatch run test

integration:
	hatch run integration

e2e:
	hatch run e2e

coverage:
	hatch run coverage; open htmlcov/index.html

docs-build: 
	hatch run docs:pydoc-markdown
	yarn --cwd docs/dqx build

docs-serve-dev:
	hatch run docs:pydoc-markdown
	yarn --cwd docs/dqx start

docs-install:
	yarn --cwd docs/dqx install

docs-serve: docs-build
	hatch run docs:pydoc-markdown
	yarn --cwd docs/dqx serve

docs-clean:
	rm -rf docs/dqx/build
	rm -rf docs/dqx/.docusaurus docs/dqx/.cache
	rm -rf docs/dqx/docs/reference/api/*
