# Contributing to FS Data Sink

Thank you for your interest in contributing to FS Data Sink! This document provides guidelines and instructions for contributing.

## Code of Conduct

Be respectful and constructive in all interactions.

## Getting Started

### Development Setup

1. **Clone the repository**

```bash
git clone https://github.com/poly-workshop/fs-data-sink.git
cd fs-data-sink
```

2. **Create a virtual environment**

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install in development mode**

```bash
make install-dev
# or
pip install -e ".[dev]"
```

4. **Start test services**

```bash
make docker-up
```

### Project Structure

```
fs-data-sink/
├── src/fs_data_sink/      # Main source code
│   ├── sources/           # Data source implementations
│   ├── sinks/             # Data sink implementations
│   ├── config/            # Configuration management
│   ├── telemetry/         # Observability
│   ├── pipeline.py        # Pipeline orchestration
│   └── cli.py             # CLI interface
├── tests/                 # Test suite
│   ├── unit/              # Unit tests
│   └── integration/       # Integration tests
├── config/                # Example configurations
└── docs/                  # Documentation
```

## Development Workflow

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Changes

Write your code following the style guidelines below.

### 3. Run Tests

```bash
make test
```

### 4. Run Linters

```bash
make lint
```

### 5. Format Code

```bash
make format
```

### 6. Commit Changes

Use clear, descriptive commit messages:

```bash
git commit -m "Add support for Azure Blob Storage sink"
```

### 7. Push and Create PR

```bash
git push origin feature/your-feature-name
```

Then create a pull request on GitHub.

## Code Style

### Python Style

- Follow PEP 8
- Use Black for formatting (line length 100)
- Use type hints where appropriate
- Write docstrings for public APIs

Example:

```python
def write_batch(self, batch: pa.RecordBatch, partition_cols: Optional[list[str]] = None) -> None:
    """
    Write a batch of data to the sink.
    
    Args:
        batch: Arrow RecordBatch to write
        partition_cols: Optional list of column names to use for partitioning
    """
    pass
```

### Import Order

1. Standard library imports
2. Third-party imports
3. Local imports

Example:

```python
import logging
from typing import Iterator, Optional

import pyarrow as pa
from opentelemetry import trace

from fs_data_sink.types import DataSource
```

## Testing

### Writing Tests

- Write unit tests for all new functionality
- Use pytest fixtures for common test data
- Mock external dependencies
- Aim for >80% code coverage

Example test:

```python
def test_kafka_source_connect():
    """Test Kafka source connection."""
    source = KafkaSource(
        bootstrap_servers=["localhost:9092"],
        topics=["test"],
        group_id="test"
    )
    # Mock and test...
```

### Running Tests

```bash
# All tests
make test

# Specific test file
pytest tests/unit/test_config.py -v

# Specific test
pytest tests/unit/test_config.py::test_load_config_from_yaml -v

# With coverage
pytest --cov=fs_data_sink --cov-report=html
```

## Adding New Features

### Adding a New Source

1. Create a new file in `src/fs_data_sink/sources/`
2. Implement the `DataSource` interface
3. Add to `sources/__init__.py`
4. Update `pipeline.py` to support the new source
5. Add configuration options
6. Write tests
7. Update documentation

Example:

```python
from fs_data_sink.types import DataSource

class MyNewSource(DataSource):
    def connect(self) -> None:
        # Implementation
        pass
    
    def read_batch(self, batch_size: int = 1000) -> Iterator[pa.RecordBatch]:
        # Implementation
        pass
    
    def close(self) -> None:
        # Implementation
        pass
```

### Adding a New Sink

Similar to adding a source, but implement the `DataSink` interface.

## Documentation

- Update README.md for user-facing changes
- Update USAGE.md for new features
- Add docstrings to all public APIs
- Include examples in documentation

## Pull Request Guidelines

### Before Submitting

- [ ] Tests pass (`make test`)
- [ ] Code is formatted (`make format`)
- [ ] Linters pass (`make lint`)
- [ ] Documentation is updated
- [ ] Commit messages are clear

### PR Description

Include:
- What the change does
- Why it's needed
- How to test it
- Any breaking changes

### Review Process

1. Automated checks must pass
2. At least one maintainer approval required
3. All review comments addressed

## Release Process

(For maintainers)

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Create a git tag
4. Build and publish to PyPI

```bash
# Build
python -m build

# Publish
python -m twine upload dist/*
```

## Getting Help

- Open an issue for bugs or feature requests
- Ask questions in discussions
- Check existing issues and PRs first

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
