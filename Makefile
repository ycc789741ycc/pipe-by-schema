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

release:
	@if [ "$$(git rev-parse --abbrev-ref HEAD)" != "master" ]; then \
		echo "Error: You are not on the master branch! Please switch to the master branch before releasing."; \
		exit 1; \
	fi
	@echo "Current version is: $$(poetry version --short)"
	@echo "Enter the new version (e.g., 1.0.1):"
	@read VERSION; \
	poetry version $$VERSION && \
	git add pyproject.toml && \
	git commit -m "Bump version to $$VERSION" && \
	git tag -a "$$VERSION" -m "Release version $$VERSION" && \
	git push origin master && \
	git push origin "$$VERSION" && \
	echo "Version updated to $$VERSION, pyproject.toml committed, and tag pushed to Git."
