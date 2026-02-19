# Contributing to Chronis

Thank you for your interest in contributing to Chronis! This document provides guidelines and instructions for contributing to the project.

## Development Setup

### Quick Setup

```bash
# Clone repository
git clone https://github.com/enginerd-kr/chronis.git
cd chronis

# Setup development environment
make setup-dev
```

### Using Makefile

The project includes a Makefile for common development tasks:

```bash
# Show all available commands
make help

# Install dependencies
make install

# Run tests with coverage
make test

# Run linter
make lint

# Format code
make format

# Run all checks (lint + type-check)
make check

# Fix linting issues and format
make fix

# Run CI pipeline
make ci

# Clean build artifacts
make clean

# Build package
make build

# Show project info
make info
```

### Manual Commands

If you prefer to run commands directly:

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest
uv run pytest tests/ -m "not slow"
uv run pytest tests/ -m "slow"

# Run linter
uv run ruff check chronis/

# Format code
uv run ruff format chronis/

# Type check
uv run mypy chronis/
```

## Testing

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=chronis --cov-report=html

# Run specific test file
uv run pytest tests/test_basic.py -v

# Run with verbose output
uv run pytest -v
```

### Writing Tests

- Place test files in the `tests/` directory
- Name test files with `test_*.py` pattern
- Use pytest fixtures for common setup
- Aim for high test coverage (80%+)
- Include both unit tests and integration tests

## Code Style

This project follows strict code quality standards:

- **Python Version**: 3.12+
- **Formatter**: Ruff (Black-compatible)
- **Linter**: Ruff
- **Type Checker**: MyPy
- **Line Length**: 100 characters

### Code Quality Checks

Before submitting a pull request, ensure your code passes all checks:

```bash
# Run all checks
make check

# Or run individually
make lint      # Linting
make format    # Code formatting
```

## Pull Request Process

1. **Fork the repository** and create your branch from `main`
2. **Make your changes** following the code style guidelines
3. **Add tests** for any new functionality
4. **Update documentation** if needed (README.md, docstrings)
5. **Run all tests and checks** to ensure they pass
6. **Commit your changes** with a clear commit message
7. **Push to your fork** and submit a pull request

### Commit Message Guidelines

Follow conventional commit format:

- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `test:` - Adding or updating tests
- `refactor:` - Code refactoring
- `chore:` - Maintenance tasks

Example:
```
feat(scheduler): add support for recurring jobs

- Implement RecurringJobTrigger
- Add tests for recurring jobs
- Update documentation
```

## Development Guidelines

### Project Structure

```
chronis/
├── chronis/              # Main package
│   ├── core/            # Core scheduler logic
│   │   ├── base/       # Abstract base classes (BaseScheduler)
│   │   ├── schedulers/ # Scheduler implementations (PollingScheduler, job builders)
│   │   ├── execution/  # Execution components (ExecutionCoordinator)
│   │   ├── jobs/       # Job definitions
│   │   ├── state/      # State management (JobStatus, TriggerType enums)
│   │   ├── triggers/   # Trigger strategies
│   │   └── misfire.py  # Misfire handling (unified)
│   ├── adapters/        # Storage and lock adapters
│   └── utils/           # Utility functions
├── tests/               # Test files
│   ├── unit/           # Unit tests
│   ├── integration/    # Integration tests
│   └── distributed/    # Distributed tests
└── examples/            # Example scripts
```

### Adding New Features

1. **Discuss first** - Open an issue to discuss the feature before implementing
2. **Follow patterns** - Use existing patterns (services, adapters, specifications, etc.)
3. **Write tests** - Include comprehensive tests for new features
4. **Document** - Add docstrings and update relevant documentation
5. **Keep it simple** - Maintain the simplified API philosophy

### Adding New Adapters

When adding storage or lock adapters:

1. Inherit from the appropriate base class (`JobStorageAdapter` or `LockAdapter`)
2. **JobStorageAdapter requires 5 core methods**: `create_job`, `get_job`, `update_job`, `delete_job`, `query_jobs`
3. **CRITICAL**: Ensure `query_jobs()` returns misfire fields:
   - `if_missed`, `misfire_threshold_seconds`, `last_run_time`, `last_scheduled_time`
4. **Optional overrides** for production:
   - `compare_and_swap_job()` - Atomic updates for multi-instance safety
   - `count_jobs()` - Optimized counting for large datasets
5. Add comprehensive tests for the adapter
6. Update documentation with usage examples
7. Add to `[project.optional-dependencies]` in pyproject.toml if it requires external dependencies

**Reference**: See [docs/ADAPTER_GUIDE.md](docs/ADAPTER_GUIDE.md) for detailed implementation guide

## Questions or Issues?

- **Bug reports**: Open an issue with detailed description and reproduction steps
- **Feature requests**: Open an issue describing the feature and use case
- **Questions**: Open a discussion or issue

## AI Assistance

Parts of this project were developed with the assistance of AI tools
(e.g. Claude) for drafting, refactoring, and documentation.

All code has been reviewed, validated, and is fully maintained by
human contributors. The use of AI tools does not replace human
responsibility for code quality, correctness, or project direction.

## License

By contributing to Chronis, you agree that your contributions will be licensed under the MIT License.
