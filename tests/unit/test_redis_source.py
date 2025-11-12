"""Tests for Redis source with continuous consumer support."""

import json
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from fs_data_sink.sources import RedisSource


@pytest.fixture
def mock_redis_client():
    """Create a mock Redis client."""
    with patch("fs_data_sink.sources.redis_source.redis.Redis") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


def test_redis_source_continuous_mode_enabled(mock_redis_client):
    """Test that Redis source with continuous=True keeps yielding batches."""
    # Setup mock to return messages on first two calls, then empty on third
    mock_redis_client.xread.side_effect = [
        [(b"stream1", [(b"1-0", {b"value": json.dumps({"id": 1, "name": "test1"}).encode()})])],
        [(b"stream1", [(b"1-1", {b"value": json.dumps({"id": 2, "name": "test2"}).encode()})])],
        [],  # Empty result to break the test loop
    ]

    source = RedisSource(
        host="localhost",
        port=6379,
        stream_keys=["stream1"],
        continuous=True,
    )
    source.connect()

    # Read batches - should get 2 batches before the empty result
    batches = []
    batch_gen = source.read_batch(batch_size=10)

    # Get first batch
    batch1 = next(batch_gen)
    assert batch1.num_rows == 1
    batches.append(batch1)

    # Get second batch
    batch2 = next(batch_gen)
    assert batch2.num_rows == 1
    batches.append(batch2)

    # Verify xread was called multiple times (continuous mode)
    assert mock_redis_client.xread.call_count >= 2

    source.close()


def test_redis_source_continuous_mode_disabled(mock_redis_client):
    """Test that Redis source with continuous=False reads once and stops."""
    mock_redis_client.xread.return_value = [
        (b"stream1", [(b"1-0", {b"value": json.dumps({"id": 1, "name": "test1"}).encode()})])
    ]

    source = RedisSource(
        host="localhost",
        port=6379,
        stream_keys=["stream1"],
        continuous=False,
    )
    source.connect()

    # Read batches - should get exactly 1 batch and stop
    batches = list(source.read_batch(batch_size=10))

    assert len(batches) == 1
    assert batches[0].num_rows == 1

    # Verify xread was called only once (non-continuous mode)
    assert mock_redis_client.xread.call_count == 1

    source.close()


def test_redis_source_continuous_mode_with_lists(mock_redis_client):
    """Test continuous mode with Redis lists."""
    # Setup mock to return messages on first two calls, then None (timeout)
    mock_redis_client.blpop.side_effect = [
        ("list1", json.dumps({"id": 1, "name": "test1"}).encode()),
        ("list1", json.dumps({"id": 2, "name": "test2"}).encode()),
        None,  # Timeout to break the test loop
    ]

    source = RedisSource(
        host="localhost",
        port=6379,
        list_keys=["list1"],
        continuous=True,
    )
    source.connect()

    # Read batches - should get 2 batches
    batch_gen = source.read_batch(batch_size=1)

    batch1 = next(batch_gen)
    assert batch1.num_rows == 1

    batch2 = next(batch_gen)
    assert batch2.num_rows == 1

    # In continuous mode, it keeps trying even after timeout
    assert mock_redis_client.blpop.call_count >= 2

    source.close()


def test_redis_source_continuous_mode_default(mock_redis_client):
    """Test that continuous mode is True by default."""
    mock_redis_client.xread.side_effect = [
        [(b"stream1", [(b"1-0", {b"value": json.dumps({"id": 1}).encode()})])],
        [],
    ]

    source = RedisSource(
        host="localhost",
        port=6379,
        stream_keys=["stream1"],
    )

    # Should default to continuous=True
    assert source.continuous is True

    source.connect()

    # Should keep trying to read
    batch_gen = source.read_batch(batch_size=10)
    batch1 = next(batch_gen)
    assert batch1.num_rows == 1

    source.close()


def test_redis_source_mixed_streams_and_lists(mock_redis_client):
    """Test reading from both streams and lists in continuous mode."""
    mock_redis_client.xread.return_value = [
        (b"stream1", [(b"1-0", {b"value": json.dumps({"id": 1, "source": "stream"}).encode()})])
    ]
    mock_redis_client.blpop.side_effect = [
        ("list1", json.dumps({"id": 2, "source": "list"}).encode()),
        None,
    ]

    source = RedisSource(
        host="localhost",
        port=6379,
        stream_keys=["stream1"],
        list_keys=["list1"],
        continuous=False,  # Use non-continuous for predictable test
    )
    source.connect()

    batches = list(source.read_batch(batch_size=10))

    assert len(batches) == 1
    assert batches[0].num_rows == 2  # One from stream, one from list

    source.close()


def test_redis_source_arrow_ipc_format(mock_redis_client):
    """Test continuous mode with Arrow IPC format."""
    # Create Arrow IPC data
    table = pa.table({"id": [1], "name": ["test"]})
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    arrow_data = sink.getvalue().to_pybytes()

    mock_redis_client.xread.side_effect = [
        [(b"stream1", [(b"1-0", {b"value": arrow_data})])],
        [],
    ]

    source = RedisSource(
        host="localhost",
        port=6379,
        stream_keys=["stream1"],
        value_format="arrow_ipc",
        continuous=False,
    )
    source.connect()

    batches = list(source.read_batch(batch_size=10))

    assert len(batches) == 1
    assert batches[0].num_rows == 1
    assert batches[0].column_names == ["id", "name"]

    source.close()
