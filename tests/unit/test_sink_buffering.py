"""Tests for sink buffering behavior."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from fs_data_sink.sinks import LocalSink, S3Sink


def test_local_sink_buffers_batches():
    """Test that LocalSink buffers batches and writes on flush."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = LocalSink(base_path=tmpdir)
        sink.connect()

        # Write 3 batches
        for i in range(3):
            batch = pa.table({"id": [i], "value": [f"test_{i}"]}).to_batches()[0]
            sink.write_batch(batch)

        # No files should be written yet
        files = list(Path(tmpdir).glob("*.parquet"))
        assert len(files) == 0, "No files should be written before flush"

        # Verify batches are buffered
        assert len(sink.buffered_batches) == 3

        # Flush should write the file
        sink.flush()

        # Now exactly 1 file should exist with all 3 rows
        files = list(Path(tmpdir).glob("*.parquet"))
        assert len(files) == 1, "Exactly 1 file should be written after flush"

        # Read the file and verify it contains all 3 rows
        table = pq.read_table(files[0])
        assert table.num_rows == 3
        assert table.column_names == ["id", "value"]

        # Buffer should be cleared
        assert len(sink.buffered_batches) == 0

        sink.close()


def test_local_sink_multiple_flushes():
    """Test that LocalSink can handle multiple flush cycles."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = LocalSink(base_path=tmpdir)
        sink.connect()

        # First batch and flush
        batch1 = pa.table({"id": [1], "value": ["test1"]}).to_batches()[0]
        sink.write_batch(batch1)
        sink.flush()

        # Second batch and flush
        batch2 = pa.table({"id": [2], "value": ["test2"]}).to_batches()[0]
        sink.write_batch(batch2)
        sink.flush()

        # Should have 2 files
        files = list(Path(tmpdir).glob("*.parquet"))
        assert len(files) == 2

        # First file should have 1 row
        table1 = pq.read_table(files[0])
        assert table1.num_rows == 1

        # Second file should have 1 row
        table2 = pq.read_table(files[1])
        assert table2.num_rows == 1

        sink.close()


def test_local_sink_flush_with_no_batches():
    """Test that flush with no batches is a no-op."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = LocalSink(base_path=tmpdir)
        sink.connect()

        # Flush without any batches
        sink.flush()

        # No files should be created
        files = list(Path(tmpdir).glob("*.parquet"))
        assert len(files) == 0

        sink.close()


def test_s3_sink_buffers_batches():
    """Test that S3Sink buffers batches and writes on flush."""
    # Mock the MinIO client
    mock_s3_client = MagicMock()

    with patch("fs_data_sink.sinks.s3_sink.Minio", return_value=mock_s3_client):
        sink = S3Sink(
            bucket="test-bucket",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        sink.connect()

        # Write 3 batches
        for i in range(3):
            batch = pa.table({"id": [i], "value": [f"test_{i}"]}).to_batches()[0]
            sink.write_batch(batch)

        # No put_object calls yet
        assert mock_s3_client.put_object.call_count == 0

        # Verify batches are buffered
        assert len(sink.buffered_batches) == 3

        # Flush should upload the file
        sink.flush()

        # Now put_object should have been called once
        assert mock_s3_client.put_object.call_count == 1

        # Buffer should be cleared
        assert len(sink.buffered_batches) == 0

        sink.close()


def test_s3_sink_flush_with_no_batches():
    """Test that S3Sink flush with no batches is a no-op."""
    mock_s3_client = MagicMock()

    with patch("fs_data_sink.sinks.s3_sink.Minio", return_value=mock_s3_client):
        sink = S3Sink(
            bucket="test-bucket",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        sink.connect()

        # Flush without any batches
        sink.flush()

        # No put_object calls
        assert mock_s3_client.put_object.call_count == 0

        sink.close()


def test_local_sink_partitioning_with_buffering():
    """Test that partitioning works correctly with buffering."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = LocalSink(base_path=tmpdir, partition_by=["date"])
        sink.connect()

        # Create a consistent schema for all batches
        schema = pa.schema([("id", pa.int64()), ("date", pa.string()), ("value", pa.string())])

        # Write batches with same partition value
        for i in range(3):
            batch = pa.table(
                {"id": [i], "date": ["2024-01-01"], "value": [f"test_{i}"]}, schema=schema
            ).to_batches()[0]
            sink.write_batch(batch)

        sink.flush()

        # Check partition directory was created
        partition_dir = Path(tmpdir) / "date=2024-01-01"
        assert partition_dir.exists()

        # Check file exists in partition directory
        files = list(partition_dir.glob("*.parquet"))
        assert len(files) == 1

        # Verify combined data - read directly from file to avoid dictionary encoding issues
        with open(files[0], "rb") as f:
            table = pq.read_table(f)
        assert table.num_rows == 3

        sink.close()
