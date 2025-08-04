all: clean dev lint fmt test integration coverage e2e

clean:
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	rm -fr **/*.pyc

.venv/bin/python:
	pip install hatch
	hatch env create

dev: .venv/bin/python
	@hatch run which python

# Extract YAML examples from MDX files for LLM resources
extract-yaml:
	@echo "Extracting YAML examples from MDX files..."
	@python3 .github/script/extract_yaml_checks_examples.py

lint:
	hatch run lint

fmt:
	hatch run fmt

test:
	hatch run test

integration: extract-yaml
	hatch run integration

coverage:
	hatch run coverage

e2e:
	hatch run e2e

dev-docs:
	hatch run docs:dev

build-docs:
	hatch run docs:build

# Build wheel with YAML extraction
build: extract-yaml
	hatch build

.PHONY: all dev lint fmt test integration coverage e2e clean extract-yaml build