"""Tests for topic/stream_key folder separation."""

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa

from fs_data_sink.sinks import LocalSink, S3Sink
from fs_data_sink.sources import KafkaSource, RedisSource


def test_local_sink_separates_by_topic():
    """Test that LocalSink creates separate folders for different topics."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = LocalSink(base_path=tmpdir)
        sink.connect()

        # Create batches with different topic metadata
        schema_topic1 = pa.schema(
            [("id", pa.int64()), ("value", pa.string())], metadata={b"topic": b"topic1"}
        )
        batch1 = pa.record_batch([[1], ["data1"]], schema=schema_topic1)

        schema_topic2 = pa.schema(
            [("id", pa.int64()), ("value", pa.string())], metadata={b"topic": b"topic2"}
        )
        batch2 = pa.record_batch([[2], ["data2"]], schema=schema_topic2)

        # Write batches
        sink.write_batch(batch1)
        sink.write_batch(batch2)
        sink.flush()

        # Check that separate folders were created
        topic1_dir = Path(tmpdir) / "topic1"
        topic2_dir = Path(tmpdir) / "topic2"

        assert topic1_dir.exists(), "topic1 folder should exist"
        assert topic2_dir.exists(), "topic2 folder should exist"

        # Check that files exist in each folder
        topic1_files = list(topic1_dir.glob("*.parquet"))
        topic2_files = list(topic2_dir.glob("*.parquet"))

        assert len(topic1_files) == 1, "topic1 should have 1 file"
        assert len(topic2_files) == 1, "topic2 should have 1 file"

        sink.close()


def test_local_sink_separates_by_stream_key():
    """Test that LocalSink creates separate folders for different stream_keys."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = LocalSink(base_path=tmpdir)
        sink.connect()

        # Create batches with different stream_key metadata
        schema_stream1 = pa.schema(
            [("id", pa.int64()), ("value", pa.string())], metadata={b"stream_key": b"stream1"}
        )
        batch1 = pa.record_batch([[1], ["data1"]], schema=schema_stream1)

        schema_stream2 = pa.schema(
            [("id", pa.int64()), ("value", pa.string())], metadata={b"stream_key": b"stream2"}
        )
        batch2 = pa.record_batch([[2], ["data2"]], schema=schema_stream2)

        # Write batches
        sink.write_batch(batch1)
        sink.write_batch(batch2)
        sink.flush()

        # Check that separate folders were created
        stream1_dir = Path(tmpdir) / "stream1"
        stream2_dir = Path(tmpdir) / "stream2"

        assert stream1_dir.exists(), "stream1 folder should exist"
        assert stream2_dir.exists(), "stream2 folder should exist"

        # Check that files exist in each folder
        stream1_files = list(stream1_dir.glob("*.parquet"))
        stream2_files = list(stream2_dir.glob("*.parquet"))

        assert len(stream1_files) == 1, "stream1 should have 1 file"
        assert len(stream2_files) == 1, "stream2 should have 1 file"

        sink.close()


def test_local_sink_with_partitioning_and_topic():
    """Test that LocalSink handles both topic separation and partitioning."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = LocalSink(base_path=tmpdir, partition_by=["date"])
        sink.connect()

        # Create batches with topic metadata and partition column
        schema = pa.schema(
            [("id", pa.int64()), ("date", pa.string()), ("value", pa.string())],
            metadata={b"topic": b"events"},
        )
        batch = pa.record_batch([[1], ["2024-01-01"], ["data1"]], schema=schema)

        sink.write_batch(batch)
        sink.flush()

        # Check folder structure: topic/partition
        expected_dir = Path(tmpdir) / "events" / "date=2024-01-01"
        assert expected_dir.exists(), "events/date=2024-01-01 folder should exist"

        files = list(expected_dir.glob("*.parquet"))
        assert len(files) == 1, "Should have 1 file in partitioned folder"

        sink.close()


def test_s3_sink_separates_by_topic():
    """Test that S3Sink creates separate folders for different topics."""
    mock_s3_client = MagicMock()

    with patch("fs_data_sink.sinks.s3_sink.Minio", return_value=mock_s3_client):
        sink = S3Sink(
            bucket="test-bucket",
            prefix="data",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        sink.connect()

        # Create batches with different topic metadata
        schema_topic1 = pa.schema(
            [("id", pa.int64()), ("value", pa.string())], metadata={b"topic": b"topic1"}
        )
        batch1 = pa.record_batch([[1], ["data1"]], schema=schema_topic1)

        schema_topic2 = pa.schema(
            [("id", pa.int64()), ("value", pa.string())], metadata={b"topic": b"topic2"}
        )
        batch2 = pa.record_batch([[2], ["data2"]], schema=schema_topic2)

        # Write batches
        sink.write_batch(batch1)
        sink.write_batch(batch2)
        sink.flush()

        # Should have called put_object twice (once per topic)
        assert mock_s3_client.put_object.call_count == 2

        # Check that the keys include topic folders
        call_args = [call[1] for call in mock_s3_client.put_object.call_args_list]
        keys = [args["object_name"] for args in call_args]

        assert any("topic1" in key for key in keys), "Should have a key with topic1"
        assert any("topic2" in key for key in keys), "Should have a key with topic2"

        sink.close()


def test_kafka_source_adds_topic_metadata():
    """Test that KafkaSource adds topic metadata to batches."""
    with patch("fs_data_sink.sources.kafka_source.KafkaConsumer") as mock_consumer_cls:
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        # Mock TopicPartition
        from kafka import TopicPartition

        tp1 = TopicPartition("topic1", 0)
        tp2 = TopicPartition("topic2", 0)

        # Mock message
        mock_message1 = MagicMock()
        mock_message1.value = json.dumps({"id": 1, "data": "test1"}).encode()

        mock_message2 = MagicMock()
        mock_message2.value = json.dumps({"id": 2, "data": "test2"}).encode()

        # First poll returns messages, second returns empty, third returns empty again for None yield
        def poll_side_effect(**kwargs):
            if not hasattr(poll_side_effect, "call_count"):
                poll_side_effect.call_count = 0
            poll_side_effect.call_count += 1

            if poll_side_effect.call_count == 1:
                return {tp1: [mock_message1], tp2: [mock_message2]}
            else:
                return {}

        mock_consumer.poll.side_effect = poll_side_effect

        source = KafkaSource(
            bootstrap_servers=["localhost:9092"],
            topics=["topic1", "topic2"],
            group_id="test-group",
        )
        source.connect()

        batches = []
        for batch in source.read_batch(batch_size=10):
            if batch is None:
                break
            batches.append(batch)

        # Should have 2 batches (one per topic)
        assert len(batches) == 2

        # Check that each batch has topic metadata
        topics = {
            batch.schema.metadata.get(b"topic").decode()
            for batch in batches
            if batch.schema.metadata
        }
        assert "topic1" in topics
        assert "topic2" in topics

        source.close()


def test_redis_source_adds_stream_key_metadata():
    """Test that RedisSource adds stream_key metadata to batches."""
    with patch("fs_data_sink.sources.redis_source.redis.Redis") as mock_redis_cls:
        mock_client = MagicMock()
        mock_redis_cls.return_value = mock_client

        # Mock stream data
        mock_client.xreadgroup.return_value = [
            (b"stream1", [(b"1-0", {b"value": json.dumps({"id": 1, "data": "test1"}).encode()})]),
            (b"stream2", [(b"2-0", {b"value": json.dumps({"id": 2, "data": "test2"}).encode()})]),
        ]

        source = RedisSource(
            host="localhost",
            stream_keys=["stream1", "stream2"],
            continuous=False,
            consumer_group="test-group",
            consumer_name="test-consumer",
        )
        source.connect()

        batches = list(source.read_batch(batch_size=10))

        # Should have 2 batches (one per stream)
        assert len(batches) == 2

        # Check that each batch has stream_key metadata
        stream_keys = {
            batch.schema.metadata.get(b"stream_key").decode()
            for batch in batches
            if batch.schema.metadata
        }
        assert "stream1" in stream_keys
        assert "stream2" in stream_keys

        source.close()


def test_batches_without_metadata_still_work():
    """Test that batches without topic/stream_key metadata still work."""
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = LocalSink(base_path=tmpdir)
        sink.connect()

        # Create batch without metadata
        schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
        batch = pa.record_batch([[1], ["data1"]], schema=schema)

        sink.write_batch(batch)
        sink.flush()

        # Should write to base directory when no metadata
        files = list(Path(tmpdir).glob("*.parquet"))
        assert len(files) == 1, "Should have 1 file in base directory"

        sink.close()
