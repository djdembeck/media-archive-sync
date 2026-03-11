# Contributing to Media Archive Sync

Thank you for your interest in contributing! This document outlines the development workflow.

## Development Setup

### 1. Clone the repository

```bash
git clone https://github.com/djdembeck/media-archive-sync.git
cd media-archive-sync
```

### 2. Enable Git Hooks (Required)

This repository uses git hooks for code quality. Enable them with:

```bash
# Option 1: Use make
make install-hooks

# Option 2: Manual git config
git config --local core.hooksPath .githooks
```

**Note:** The hooks will now run automatically on every commit and push.

### 3. Install Development Dependencies

```bash
pip install -e ".[dev]"
```

Or use the Makefile:

```bash
make dev-install  # Installs deps + enables hooks
```

## Git Hooks

The following hooks run automatically:

### pre-commit
- Runs `ruff` linter with auto-fix
- Runs `black` formatter
- Runs quick tests with pytest

### pre-push
- Runs full test suite with coverage (requires 80%)
- Runs `mypy` type checking

### Bypassing Hooks

If you need to bypass hooks temporarily:

```bash
git commit --no-verify  # or -n
git push --no-verify
```

## Development Commands

```bash
# Run tests
make test

# Run tests with coverage
make test-cov

# Run linter
make lint

# Run formatter
make format

# Clean build artifacts
make clean
```

## Code Quality Standards

- **Linting:** Ruff (configured in `pyproject.toml`)
- **Formatting:** Black (88 char line length)
- **Type Checking:** MyPy
- **Testing:** pytest with coverage (80% minimum)

## Submitting Changes

1. Create a feature branch
2. Make your changes
3. Ensure hooks pass (they run automatically)
4. Push and create a Pull Request

## Questions?

Open an issue or discussion on GitHub.
