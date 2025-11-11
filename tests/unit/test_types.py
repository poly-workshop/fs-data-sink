"""Tests for type definitions and interfaces."""

import pyarrow as pa
import pytest

from fs_data_sink.types import DataSink, DataSource


def test_data_source_interface():
    """Test that DataSource is an abstract base class."""
    # Should not be able to instantiate directly
    with pytest.raises(TypeError):
        DataSource()


def test_data_sink_interface():
    """Test that DataSink is an abstract base class."""
    # Should not be able to instantiate directly
    with pytest.raises(TypeError):
        DataSink()


class MockSource(DataSource):
    """Mock implementation for testing."""

    def connect(self):
        self.connected = True

    def read_batch(self, batch_size=1000):
        # Create a simple test batch
        data = {"id": [1, 2, 3], "name": ["a", "b", "c"]}
        table = pa.Table.from_pydict(data)
        yield table.to_batches()[0]

    def close(self):
        self.connected = False


class MockSink(DataSink):
    """Mock implementation for testing."""

    def connect(self):
        self.connected = True
        self.batches = []

    def write_batch(self, batch, partition_cols=None):
        self.batches.append(batch)

    def flush(self):
        pass

    def close(self):
        self.connected = False


def test_mock_source():
    """Test that mock source implementation works."""
    source = MockSource()
    source.connect()
    assert source.connected

    batches = list(source.read_batch())
    assert len(batches) == 1
    assert batches[0].num_rows == 3

    source.close()
    assert not source.connected


def test_mock_sink():
    """Test that mock sink implementation works."""
    sink = MockSink()
    sink.connect()
    assert sink.connected

    # Create test batch
    data = {"id": [1, 2], "value": [10, 20]}
    table = pa.Table.from_pydict(data)
    batch = table.to_batches()[0]

    sink.write_batch(batch)
    assert len(sink.batches) == 1
    assert sink.batches[0].num_rows == 2

    sink.flush()
    sink.close()
    assert not sink.connected
