"""Pytest configuration and fixtures."""

import pytest


@pytest.fixture
def sample_arrow_data():
    """Provide sample Arrow data for tests."""
    import pyarrow as pa

    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "value": [10.5, 20.3, 30.1, 40.9, 50.2],
        "timestamp": ["2024-01-01T00:00:00Z"] * 5,
    }

    table = pa.Table.from_pydict(data)
    return table.to_batches()[0]
