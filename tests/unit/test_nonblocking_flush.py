"""Tests for non-blocking source behavior with time-based flushes."""

import time
from unittest.mock import MagicMock, patch

import pyarrow as pa

from fs_data_sink.config.settings import PipelineConfig, Settings, SinkConfig, SourceConfig
from fs_data_sink.pipeline import DataPipeline


def test_redis_nonblocking_allows_time_based_flush():
    """Test that Redis source yields None when no data, allowing time-based flush."""
    # Create mock source that yields: batch, None, None, None, batch
    # This simulates: data available, then 3 polls with no data, then more data
    mock_source = MagicMock()

    def read_batch_with_none(batch_size):
        # First batch
        table = pa.table({"id": [1], "value": ["test1"]})
        yield table.to_batches()[0]

        # No data for a while (yield None 3 times)
        for _ in range(3):
            time.sleep(0.4)  # Simulate polling delay
            yield None

        # Second batch
        table = pa.table({"id": [2], "value": ["test2"]})
        yield table.to_batches()[0]

    mock_source.read_batch.side_effect = read_batch_with_none

    mock_sink = MagicMock()

    settings = Settings(
        source=SourceConfig(type="redis", stream_keys=["test"]),
        sink=SinkConfig(type="local", base_path="/tmp/test"),
        pipeline=PipelineConfig(
            flush_interval_seconds=1,  # Flush every 1 second
            max_batches=2,  # Stop after 2 real batches
        ),
    )

    pipeline = DataPipeline(settings)
    pipeline.source = mock_source
    pipeline.sink = mock_sink

    with patch.object(pipeline, "_create_source", return_value=mock_source):
        with patch.object(pipeline, "_create_sink", return_value=mock_sink):
            pipeline.run()

    # Should have flushed at least twice:
    # 1. After first batch (no time passed yet, may not flush)
    # 2. During the None yields (time-based flush triggered)
    # 3. Final flush at end
    # The key is that flush was called during the None yields due to time passing
    assert (
        mock_sink.flush.call_count >= 2
    ), f"Expected at least 2 flushes, got {mock_sink.flush.call_count}"


def test_kafka_nonblocking_allows_time_based_flush():
    """Test that Kafka source yields None when no data, allowing time-based flush."""
    # Create mock source that simulates Kafka behavior
    mock_source = MagicMock()

    def read_batch_with_none(batch_size):
        # First batch
        table = pa.table({"id": [1], "value": ["test1"]})
        yield table.to_batches()[0]

        # No data for a while
        for _ in range(3):
            time.sleep(0.4)
            yield None

        # Second batch
        table = pa.table({"id": [2], "value": ["test2"]})
        yield table.to_batches()[0]

    mock_source.read_batch.side_effect = read_batch_with_none

    mock_sink = MagicMock()

    settings = Settings(
        source=SourceConfig(type="kafka", topics=["test"]),
        sink=SinkConfig(type="local", base_path="/tmp/test"),
        pipeline=PipelineConfig(
            flush_interval_seconds=1,
            max_batches=2,
        ),
    )

    pipeline = DataPipeline(settings)
    pipeline.source = mock_source
    pipeline.sink = mock_sink

    with patch.object(pipeline, "_create_source", return_value=mock_source):
        with patch.object(pipeline, "_create_sink", return_value=mock_sink):
            pipeline.run()

    # Should have flushed during None yields due to time passing
    assert mock_sink.flush.call_count >= 2


def test_pipeline_handles_none_batches_correctly():
    """Test that pipeline correctly skips None batches and continues."""
    mock_source = MagicMock()

    def read_batch_with_nones(batch_size):
        table = pa.table({"id": [1]})
        yield table.to_batches()[0]
        yield None
        yield None
        table = pa.table({"id": [2]})
        yield table.to_batches()[0]
        yield None
        table = pa.table({"id": [3]})
        yield table.to_batches()[0]

    mock_source.read_batch.side_effect = read_batch_with_nones

    mock_sink = MagicMock()

    settings = Settings(
        source=SourceConfig(type="redis", stream_keys=["test"]),
        sink=SinkConfig(type="local", base_path="/tmp/test"),
        pipeline=PipelineConfig(max_batches=3),
    )

    pipeline = DataPipeline(settings)
    pipeline.source = mock_source
    pipeline.sink = mock_sink

    with patch.object(pipeline, "_create_source", return_value=mock_source):
        with patch.object(pipeline, "_create_sink", return_value=mock_sink):
            pipeline.run()

    # Should have written 3 real batches
    assert mock_sink.write_batch.call_count == 3

    # Should have final flush
    assert mock_sink.flush.call_count >= 1


def test_continuous_redis_yields_none_when_no_data():
    """Test that Redis source in continuous mode yields None when no data."""
    from unittest.mock import MagicMock, patch

    import redis

    from fs_data_sink.sources import RedisSource

    mock_client = MagicMock()
    mock_client.xreadgroup.side_effect = [
        [(b"stream1", [(b"1-0", {b"value": b'{"id": 1}'})])],  # First call returns data
        [],  # Second call returns no data
        [],  # Third call returns no data
    ]
    # Mock xgroup_create to simulate group already exists
    mock_client.xgroup_create.side_effect = redis.exceptions.ResponseError(
        "BUSYGROUP Consumer Group name already exists"
    )

    with patch("fs_data_sink.sources.redis_source.redis.Redis", return_value=mock_client):
        source = RedisSource(
            host="localhost",
            stream_keys=["stream1"],
            continuous=True,
            consumer_group="test-group",
            consumer_name="test-consumer",
        )
        source.connect()

        batch_gen = source.read_batch(batch_size=10)

        # First yield should be a real batch
        batch1 = next(batch_gen)
        assert batch1 is not None
        assert batch1.num_rows == 1

        # Second yield should be None (no data)
        batch2 = next(batch_gen)
        assert batch2 is None

        # Third yield should be None (no data)
        batch3 = next(batch_gen)
        assert batch3 is None

        source.close()
