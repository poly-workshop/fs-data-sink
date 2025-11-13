"""Tests for file merging functionality."""

import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from fs_data_sink.sinks import LocalSink, S3Sink


def test_local_sink_merge_disabled_by_default():
    """Test that merge is disabled by default."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = LocalSink(base_path=tmpdir)
        sink.connect()

        # Write and flush some batches
        for i in range(3):
            batch = pa.table({"id": [i], "value": [f"test_{i}"]}).to_batches()[0]
            sink.write_batch(batch)

        sink.flush()

        # Call merge - should return 0 (disabled)
        merged_count = sink.merge_files()
        assert merged_count == 0

        # All original files should still exist
        files = list(Path(tmpdir).glob("data_*.parquet"))
        assert len(files) == 1

        sink.close()


def test_local_sink_merge_by_hour():
    """Test merging files by hour."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = LocalSink(
            base_path=tmpdir,
            merge_enabled=True,
            merge_period="hour",
            merge_min_files=2,
        )
        sink.connect()

        # Write multiple batches (will create separate files)
        for i in range(3):
            batch = pa.table({"id": [i], "value": [f"test_{i}"]}).to_batches()[0]
            sink.write_batch(batch)
            sink.flush()
            time.sleep(0.01)  # Small delay to ensure different timestamps

        # Should have 3 separate files
        files_before = list(Path(tmpdir).glob("data_*.parquet"))
        assert len(files_before) == 3

        # Merge files
        merged_count = sink.merge_files()

        # Should have merged all 3 files
        assert merged_count == 3

        # Should now have 1 merged file
        merged_files = list(Path(tmpdir).glob("merged_*.parquet"))
        assert len(merged_files) == 1

        # Original files should be deleted
        original_files = list(Path(tmpdir).glob("data_*.parquet"))
        assert len(original_files) == 0

        # Verify merged file contains all data
        table = pq.read_table(merged_files[0])
        assert table.num_rows == 3

        sink.close()


def test_local_sink_merge_by_day():
    """Test merging files by day."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = LocalSink(
            base_path=tmpdir,
            merge_enabled=True,
            merge_period="day",
            merge_min_files=2,
        )
        sink.connect()

        # Write multiple batches
        for i in range(3):
            batch = pa.table({"id": [i], "value": [f"test_{i}"]}).to_batches()[0]
            sink.write_batch(batch)
            sink.flush()

        # Merge files
        merged_count = sink.merge_files()
        assert merged_count == 3

        # Should have 1 merged file
        merged_files = list(Path(tmpdir).glob("merged_*.parquet"))
        assert len(merged_files) == 1

        sink.close()


def test_local_sink_merge_min_files():
    """Test that merge respects minimum file count."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = LocalSink(
            base_path=tmpdir,
            merge_enabled=True,
            merge_period="hour",
            merge_min_files=5,  # Require at least 5 files
        )
        sink.connect()

        # Write only 3 batches
        for i in range(3):
            batch = pa.table({"id": [i], "value": [f"test_{i}"]}).to_batches()[0]
            sink.write_batch(batch)
            sink.flush()

        # Merge should not happen (only 3 files, need 5)
        merged_count = sink.merge_files()
        assert merged_count == 0

        # Original files should still exist
        original_files = list(Path(tmpdir).glob("data_*.parquet"))
        assert len(original_files) == 3

        # No merged files
        merged_files = list(Path(tmpdir).glob("merged_*.parquet"))
        assert len(merged_files) == 0

        sink.close()


def test_local_sink_merge_on_flush():
    """Test automatic merge during flush."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = LocalSink(
            base_path=tmpdir,
            merge_enabled=True,
            merge_period="hour",
            merge_min_files=2,
            merge_on_flush=True,
        )
        sink.connect()

        # Write and flush batches multiple times to create separate files
        for i in range(3):
            batch = pa.table({"id": [i], "value": [f"test_{i}"]}).to_batches()[0]
            sink.write_batch(batch)
            sink.flush()  # Each flush creates a separate file

        # After last flush with merge_on_flush=True, files should be merged
        # Files should be merged
        merged_files = list(Path(tmpdir).glob("merged_*.parquet"))
        assert len(merged_files) == 1

        sink.close()


def test_local_sink_merge_with_partitioning():
    """Test merging with partitioned data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = LocalSink(
            base_path=tmpdir,
            partition_by=["date"],
            merge_enabled=True,
            merge_period="hour",
            merge_min_files=2,
        )
        sink.connect()

        schema = pa.schema([("id", pa.int64()), ("date", pa.string()), ("value", pa.string())])

        # Write batches to same partition
        for i in range(3):
            batch = pa.table(
                {"id": [i], "date": ["2024-01-01"], "value": [f"test_{i}"]}, schema=schema
            ).to_batches()[0]
            sink.write_batch(batch)
            sink.flush()

        # Merge files
        merged_count = sink.merge_files()
        assert merged_count == 3

        # Check merged file exists in partition directory
        partition_dir = Path(tmpdir) / "date=2024-01-01"
        assert partition_dir.exists()

        merged_files = list(partition_dir.glob("merged_*.parquet"))
        assert len(merged_files) == 1

        # Verify merged data (use ParquetFile to read single file, not as dataset)
        pf = pq.ParquetFile(merged_files[0])
        table = pf.read()
        assert table.num_rows == 3

        sink.close()


def test_s3_sink_merge_disabled_by_default():
    """Test that S3 merge is disabled by default."""
    mock_s3_client = MagicMock()

    with patch("fs_data_sink.sinks.s3_sink.Minio", return_value=mock_s3_client):
        sink = S3Sink(
            bucket="test-bucket",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        sink.connect()

        # Call merge - should return 0 (disabled)
        merged_count = sink.merge_files()
        assert merged_count == 0

        sink.close()


def test_s3_sink_merge_enabled():
    """Test S3 merge with mock client."""
    mock_s3_client = MagicMock()

    # Mock list_objects to return some files
    mock_obj1 = MagicMock()
    mock_obj1.object_name = "data_20241113_100000_000001.parquet"
    mock_obj2 = MagicMock()
    mock_obj2.object_name = "data_20241113_100100_000002.parquet"

    mock_s3_client.list_objects.return_value = [mock_obj1, mock_obj2]

    # Mock get_object to return parquet data
    def mock_get_object(bucket_name, object_name):
        # Create a simple parquet file in memory
        table = pa.table({"id": [1], "value": ["test"]})
        from io import BytesIO

        buffer = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        mock_response = MagicMock()
        mock_response.read.return_value = buffer.getvalue()
        return mock_response

    mock_s3_client.get_object.side_effect = mock_get_object

    with patch("fs_data_sink.sinks.s3_sink.Minio", return_value=mock_s3_client):
        sink = S3Sink(
            bucket="test-bucket",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            merge_enabled=True,
            merge_period="hour",
            merge_min_files=2,
        )
        sink.connect()

        # Call merge
        merged_count = sink.merge_files()

        # Should have merged 2 files
        assert merged_count == 2

        # Verify put_object was called for merged file
        assert mock_s3_client.put_object.call_count == 1

        # Verify remove_object was called for original files
        assert mock_s3_client.remove_object.call_count == 2

        sink.close()


def test_local_sink_merge_skips_already_merged_files():
    """Test that merge skips already merged files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = LocalSink(
            base_path=tmpdir,
            merge_enabled=True,
            merge_period="hour",
            merge_min_files=2,
        )
        sink.connect()

        # Write and merge files
        for i in range(3):
            batch = pa.table({"id": [i], "value": [f"test_{i}"]}).to_batches()[0]
            sink.write_batch(batch)
            sink.flush()

        # First merge
        merged_count = sink.merge_files()
        assert merged_count == 3

        # Try to merge again - should find no files to merge
        merged_count2 = sink.merge_files()
        assert merged_count2 == 0

        # Still only 1 merged file
        merged_files = list(Path(tmpdir).glob("merged_*.parquet"))
        assert len(merged_files) == 1

        sink.close()


def test_merge_config_loading():
    """Test that merge configuration is loaded correctly."""
    from fs_data_sink.config.settings import SinkConfig

    config = SinkConfig(
        type="local",
        base_path="/tmp/test",
        merge_enabled=True,
        merge_period="day",
        merge_min_files=5,
        merge_on_flush=True,
    )

    assert config.merge_enabled is True
    assert config.merge_period == "day"
    assert config.merge_min_files == 5
    assert config.merge_on_flush is True


def test_merge_config_defaults():
    """Test default merge configuration values."""
    from fs_data_sink.config.settings import SinkConfig

    config = SinkConfig(type="local", base_path="/tmp/test")

    assert config.merge_enabled is False
    assert config.merge_period == "hour"
    assert config.merge_min_files == 2
    assert config.merge_on_flush is False
