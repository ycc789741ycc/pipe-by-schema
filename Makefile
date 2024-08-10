.PHONY: help install install-dev unit-test lint type-check security-check static-checks format publish clean

PYPI_USERNAME ?= null
PYPI_PASSWORD ?= null

install:
	@echo "Installing dependencies"
	@poetry install

install-dev:
	@echo "Installing dependencies"
	@poetry install --no-root --with=dev

unit-test:
	@echo "Running unit tests"
	@poetry run pytest

lint:
	@echo "Running linter"
	@poetry run pylint ./pipesche ./tests

type-check:
	@echo "Running type checker"
	@poetry run mypy ./pipesche ./tests

security-check:
	@echo "Running security check"
	@poetry run bandit -r ./pipesche

static-checks: lint type-check security-check

format:
	@echo "Running formatter"
	@poetry run black .
	@poetry run isort .

publish:
	@echo "Publishing package"
	@poetry publish --build --username $(PYPI_USERNAME) --password $(PYPI_PASSWORD)

clean:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f  {} +
	rm -rf build/
	rm -rf .mypy_cache/
	rm -rf dist/
	rm -rf docs/build
	rm -rf docs/.docusaurus
	rm -rf .pytest_cache/
