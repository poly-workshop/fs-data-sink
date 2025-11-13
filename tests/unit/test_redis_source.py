"""Tests for Redis source with continuous consumer support."""

import json
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
import redis

from fs_data_sink.sources import RedisSource


@pytest.fixture
def mock_redis_client():
    """Create a mock Redis client."""
    with patch("fs_data_sink.sources.redis_source.redis.Redis") as mock:
        client = MagicMock()
        mock.return_value = client
        # Mock xgroup_create to simulate group already exists
        client.xgroup_create.side_effect = redis.exceptions.ResponseError(
            "BUSYGROUP Consumer Group name already exists"
        )
        yield client


def test_redis_source_continuous_mode_enabled(mock_redis_client):
    """Test that Redis source with continuous=True keeps yielding batches and None."""
    # Setup mock to return messages on first two calls, then empty on third
    mock_redis_client.xreadgroup.side_effect = [
        [(b"stream1", [(b"1-0", {b"value": json.dumps({"id": 1, "name": "test1"}).encode()})])],
        [(b"stream1", [(b"1-1", {b"value": json.dumps({"id": 2, "name": "test2"}).encode()})])],
        [],  # Empty result - will yield None in continuous mode
        [],  # Another empty to verify continuous yielding
    ]

    source = RedisSource(
        host="localhost",
        port=6379,
        stream_keys=["stream1"],
        continuous=True,
        consumer_group="test-group",
        consumer_name="test-consumer",
    )
    source.connect()

    # Read batches - should get 2 batches, then None values
    batch_gen = source.read_batch(batch_size=10)

    # Get first batch
    batch1 = next(batch_gen)
    assert batch1 is not None
    assert batch1.num_rows == 1

    # Get second batch
    batch2 = next(batch_gen)
    assert batch2 is not None
    assert batch2.num_rows == 1

    # Get None when no data (continuous mode yields None instead of blocking)
    batch3 = next(batch_gen)
    assert batch3 is None

    # Verify xreadgroup was called multiple times (continuous mode)
    assert mock_redis_client.xreadgroup.call_count >= 3
    # Verify messages were acknowledged
    assert mock_redis_client.xack.call_count >= 2

    source.close()


def test_redis_source_continuous_mode_disabled(mock_redis_client):
    """Test that Redis source with continuous=False reads once and stops."""
    mock_redis_client.xreadgroup.return_value = [
        (b"stream1", [(b"1-0", {b"value": json.dumps({"id": 1, "name": "test1"}).encode()})])
    ]

    source = RedisSource(
        host="localhost",
        port=6379,
        stream_keys=["stream1"],
        continuous=False,
        consumer_group="test-group",
        consumer_name="test-consumer",
    )
    source.connect()

    # Read batches - should get exactly 1 batch and stop
    batches = list(source.read_batch(batch_size=10))

    assert len(batches) == 1
    assert batches[0].num_rows == 1

    # Verify xreadgroup was called only once (non-continuous mode)
    assert mock_redis_client.xreadgroup.call_count == 1
    # Verify message was acknowledged
    assert mock_redis_client.xack.call_count == 1

    source.close()


def test_redis_source_continuous_mode_with_lists(mock_redis_client):
    """Test continuous mode with Redis lists."""
    # Setup mock to return messages on first two calls, then None (timeout)
    mock_redis_client.blpop.side_effect = [
        ("list1", json.dumps({"id": 1, "name": "test1"}).encode()),
        ("list1", json.dumps({"id": 2, "name": "test2"}).encode()),
        None,  # Timeout - will yield None in continuous mode
        None,  # Another timeout
    ]

    source = RedisSource(
        host="localhost",
        port=6379,
        list_keys=["list1"],
        continuous=True,
    )
    source.connect()

    # Read batches - should get 2 batches, then None
    batch_gen = source.read_batch(batch_size=1)

    batch1 = next(batch_gen)
    assert batch1 is not None
    assert batch1.num_rows == 1

    batch2 = next(batch_gen)
    assert batch2 is not None
    assert batch2.num_rows == 1

    # Get None when no data (continuous mode yields None)
    batch3 = next(batch_gen)
    assert batch3 is None

    # In continuous mode, it keeps trying even after timeout
    assert mock_redis_client.blpop.call_count >= 3

    source.close()


def test_redis_source_continuous_mode_default(mock_redis_client):
    """Test that continuous mode is True by default."""
    mock_redis_client.xreadgroup.side_effect = [
        [(b"stream1", [(b"1-0", {b"value": json.dumps({"id": 1}).encode()})])],
        [],  # Will yield None
    ]

    source = RedisSource(
        host="localhost",
        port=6379,
        stream_keys=["stream1"],
        consumer_group="test-group",
        consumer_name="test-consumer",
    )

    # Should default to continuous=True
    assert source.continuous is True

    source.connect()

    # Should keep trying to read and yield None when no data
    batch_gen = source.read_batch(batch_size=10)
    batch1 = next(batch_gen)
    assert batch1 is not None
    assert batch1.num_rows == 1

    # Next should yield None (continuous mode, no data)
    batch2 = next(batch_gen)
    assert batch2 is None

    source.close()


def test_redis_source_mixed_streams_and_lists(mock_redis_client):
    """Test reading from both streams and lists in continuous mode."""
    mock_redis_client.xreadgroup.return_value = [
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
        consumer_group="test-group",
        consumer_name="test-consumer",
    )
    source.connect()

    batches = list(source.read_batch(batch_size=10))

    # Now returns separate batches for each stream_key/list_key
    assert len(batches) == 2
    # Verify stream_key metadata is present
    assert batches[0].schema.metadata.get(b'stream_key') in [b'stream1', b'list1']
    assert batches[1].schema.metadata.get(b'stream_key') in [b'stream1', b'list1']
    # Verify total rows
    assert batches[0].num_rows + batches[1].num_rows == 2

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

    mock_redis_client.xreadgroup.side_effect = [
        [(b"stream1", [(b"1-0", {b"value": arrow_data})])],
        [],
    ]

    source = RedisSource(
        host="localhost",
        port=6379,
        stream_keys=["stream1"],
        value_format="arrow_ipc",
        continuous=False,
        consumer_group="test-group",
        consumer_name="test-consumer",
    )
    source.connect()

    batches = list(source.read_batch(batch_size=10))

    assert len(batches) == 1
    assert batches[0].num_rows == 1
    assert batches[0].column_names == ["id", "name"]

    source.close()


def test_redis_source_requires_consumer_group_for_streams(mock_redis_client):
    """Test that consumer_group is required when using Redis Streams."""
    source = RedisSource(
        host="localhost",
        port=6379,
        stream_keys=["stream1"],
    )

    # Should raise ValueError when connecting without consumer_group
    with pytest.raises(ValueError, match="consumer_group is required when using Redis Streams"):
        source.connect()


def test_redis_source_consumer_group_creation(mock_redis_client):
    """Test that consumer group is created if it doesn't exist."""
    # Reset mock to not have side effect for this test
    mock_redis_client.xgroup_create.side_effect = None
    mock_redis_client.xgroup_create.return_value = True

    source = RedisSource(
        host="localhost",
        port=6379,
        stream_keys=["stream1", "stream2"],
        consumer_group="test-group",
        consumer_name="test-consumer",
    )
    source.connect()

    # Verify xgroup_create was called for each stream
    assert mock_redis_client.xgroup_create.call_count == 2
    mock_redis_client.xgroup_create.assert_any_call(
        name="stream1", groupname="test-group", id="0", mkstream=True
    )
    mock_redis_client.xgroup_create.assert_any_call(
        name="stream2", groupname="test-group", id="0", mkstream=True
    )

    source.close()


def test_redis_source_default_consumer_name(mock_redis_client):
    """Test that consumer_name defaults to hostname-id if not provided."""
    source = RedisSource(
        host="localhost",
        port=6379,
        stream_keys=["stream1"],
        consumer_group="test-group",
    )
    source.connect()

    # Should have generated a default consumer_name
    assert source.consumer_name is not None
    assert "-" in source.consumer_name  # Should contain hostname and id

    source.close()


def test_redis_source_lists_dont_require_consumer_group(mock_redis_client):
    """Test that lists work without consumer_group."""
    mock_redis_client.blpop.side_effect = [
        ("list1", json.dumps({"id": 1, "name": "test"}).encode()),
        None,  # Return None to stop reading
    ]

    source = RedisSource(
        host="localhost",
        port=6379,
        list_keys=["list1"],
        continuous=False,
    )
    source.connect()

    batches = list(source.read_batch(batch_size=10))

    assert len(batches) == 1
    assert batches[0].num_rows == 1

    source.close()


def test_redis_source_xack_called_for_each_message(mock_redis_client):
    """Test that XACK is called for each message read."""
    mock_redis_client.xreadgroup.return_value = [
        (
            b"stream1",
            [
                (b"1-0", {b"value": json.dumps({"id": 1}).encode()}),
                (b"1-1", {b"value": json.dumps({"id": 2}).encode()}),
                (b"1-2", {b"value": json.dumps({"id": 3}).encode()}),
            ],
        )
    ]

    source = RedisSource(
        host="localhost",
        port=6379,
        stream_keys=["stream1"],
        continuous=False,
        consumer_group="test-group",
        consumer_name="test-consumer",
    )
    source.connect()

    batches = list(source.read_batch(batch_size=10))

    assert len(batches) == 1
    assert batches[0].num_rows == 3

    # Verify XACK was called for each message
    assert mock_redis_client.xack.call_count == 3
    mock_redis_client.xack.assert_any_call("stream1", "test-group", "1-0")
    mock_redis_client.xack.assert_any_call("stream1", "test-group", "1-1")
    mock_redis_client.xack.assert_any_call("stream1", "test-group", "1-2")

    source.close()
