"""Tests for pipeline flush interval functionality."""

import time
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from fs_data_sink.config.settings import PipelineConfig, Settings, SinkConfig, SourceConfig
from fs_data_sink.pipeline import DataPipeline


@pytest.fixture
def mock_source():
    """Create a mock data source."""
    source = MagicMock()

    # Mock source that yields batches
    def read_batch(batch_size):
        for i in range(5):  # Yield 5 batches
            table = pa.table({"id": [i], "value": [f"test_{i}"]})
            yield table.to_batches()[0]

    source.read_batch.side_effect = read_batch
    return source


@pytest.fixture
def mock_sink():
    """Create a mock data sink."""
    sink = MagicMock()
    return sink


def test_flush_interval_batches(mock_source, mock_sink):
    """Test flush is called after specified number of batches."""
    settings = Settings(
        source=SourceConfig(type="redis", stream_keys=["test"]),
        sink=SinkConfig(type="local", base_path="/tmp/test"),
        pipeline=PipelineConfig(flush_interval_batches=2),  # Flush every 2 batches
    )

    pipeline = DataPipeline(settings)
    pipeline.source = mock_source
    pipeline.sink = mock_sink

    with patch.object(pipeline, "_create_source", return_value=mock_source):
        with patch.object(pipeline, "_create_sink", return_value=mock_sink):
            pipeline.run()

    # Should flush at batches 2, 4, and final flush = 3 times total
    assert mock_sink.flush.call_count == 3


def test_flush_interval_seconds(mock_source, mock_sink):
    """Test flush is called after specified time interval."""
    settings = Settings(
        source=SourceConfig(type="redis", stream_keys=["test"]),
        sink=SinkConfig(type="local", base_path="/tmp/test"),
        pipeline=PipelineConfig(flush_interval_seconds=1),  # Flush every 1 second
    )

    # Mock source that adds delay to simulate time passing
    def read_batch_with_delay(batch_size):
        for i in range(3):
            time.sleep(0.6)  # Each batch takes 0.6 seconds
            table = pa.table({"id": [i], "value": [f"test_{i}"]})
            yield table.to_batches()[0]

    mock_source.read_batch.side_effect = read_batch_with_delay

    pipeline = DataPipeline(settings)
    pipeline.source = mock_source
    pipeline.sink = mock_sink

    with patch.object(pipeline, "_create_source", return_value=mock_source):
        with patch.object(pipeline, "_create_sink", return_value=mock_sink):
            pipeline.run()

    # Should flush after ~1 second (batch 2), and final flush
    # At least 2 flushes (could be more depending on timing)
    assert mock_sink.flush.call_count >= 2


def test_flush_interval_both_conditions(mock_source, mock_sink):
    """Test flush when either time or batch condition is met."""
    settings = Settings(
        source=SourceConfig(type="redis", stream_keys=["test"]),
        sink=SinkConfig(type="local", base_path="/tmp/test"),
        pipeline=PipelineConfig(
            flush_interval_seconds=10,  # 10 seconds (won't be reached)
            flush_interval_batches=2,  # 2 batches (will be reached)
        ),
    )

    pipeline = DataPipeline(settings)
    pipeline.source = mock_source
    pipeline.sink = mock_sink

    with patch.object(pipeline, "_create_source", return_value=mock_source):
        with patch.object(pipeline, "_create_sink", return_value=mock_sink):
            pipeline.run()

    # Should flush based on batch count (2, 4) + final = 3 times
    assert mock_sink.flush.call_count == 3


def test_no_flush_interval(mock_source, mock_sink):
    """Test flush only at the end when no interval is configured."""
    settings = Settings(
        source=SourceConfig(type="redis", stream_keys=["test"]),
        sink=SinkConfig(type="local", base_path="/tmp/test"),
        pipeline=PipelineConfig(),  # No flush interval
    )

    pipeline = DataPipeline(settings)
    pipeline.source = mock_source
    pipeline.sink = mock_sink

    with patch.object(pipeline, "_create_source", return_value=mock_source):
        with patch.object(pipeline, "_create_sink", return_value=mock_sink):
            pipeline.run()

    # Should only flush once at the end
    assert mock_sink.flush.call_count == 1


def test_config_loading_flush_intervals():
    """Test that flush interval configuration is loaded correctly."""
    config = PipelineConfig(
        flush_interval_seconds=60,
        flush_interval_batches=100,
    )

    assert config.flush_interval_seconds == 60
    assert config.flush_interval_batches == 100


def test_config_default_flush_intervals():
    """Test default flush interval values."""
    config = PipelineConfig()

    assert config.flush_interval_seconds is None
    assert config.flush_interval_batches is None
