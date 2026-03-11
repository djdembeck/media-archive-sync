.PHONY: help install-hooks dev-install test test-cov lint format clean

help:
	@echo "Available targets:"
	@echo "  install-hooks  - Configure git to use hooks from .githooks/"
	@echo "  dev-install    - Install package with dev dependencies"
	@echo "  test           - Run tests"
	@echo "  lint           - Run ruff linter"
	@echo "  format         - Run black formatter"
	@echo "  clean          - Clean build artifacts"

install-hooks:
	git config --local include.path ../.gitconfig
	@echo "Git hooks configured! They will run on commit/push."

dev-install:
	pip install -e ".[dev]"
	$(MAKE) install-hooks

test:
	pytest -v --tb=short

test-cov:
	pytest --cov=src/media_archive_sync --cov-report=term-missing --cov-fail-under=80

lint:
	ruff check --fix src/

format:
	black src/ tests/

clean:
	rm -rf build/ dist/ *.egg-info .pytest_cache .coverage htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
