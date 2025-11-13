"""Kafka data source implementation."""

import json
import logging
from typing import Iterator, Optional

import pyarrow as pa
from kafka import KafkaConsumer
from opentelemetry import trace

from fs_data_sink.types import DataSource

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class KafkaSource(DataSource):
    """
    Kafka data source that reads Arrow data from Kafka topics.

    Supports both JSON messages (auto-converted to Arrow) and Arrow IPC format.
    """

    def __init__(
        self,
        bootstrap_servers: list[str],
        topics: list[str],
        group_id: str,
        value_format: str = "json",
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = True,
        consumer_config: Optional[dict] = None,
        poll_timeout_ms: int = 1000,
    ):
        """
        Initialize Kafka source.

        Args:
            bootstrap_servers: List of Kafka broker addresses
            topics: List of topics to consume from
            group_id: Consumer group ID
            value_format: Format of message values ('json' or 'arrow_ipc')
            auto_offset_reset: What to do when there is no initial offset
            enable_auto_commit: Whether to auto-commit offsets
            consumer_config: Additional Kafka consumer configuration
            poll_timeout_ms: Timeout in milliseconds for polling (default: 1000ms)
        """
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.group_id = group_id
        self.value_format = value_format
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.consumer_config = consumer_config or {}
        self.poll_timeout_ms = poll_timeout_ms
        self.consumer: Optional[KafkaConsumer] = None

    def connect(self) -> None:
        """Establish connection to Kafka."""
        with tracer.start_as_current_span("kafka_connect"):
            logger.info(
                f"Connecting to Kafka: servers={self.bootstrap_servers}, "
                f"topics={self.topics}, group_id={self.group_id}"
            )

            config = {
                "bootstrap_servers": self.bootstrap_servers,
                "group_id": self.group_id,
                "auto_offset_reset": self.auto_offset_reset,
                "enable_auto_commit": self.enable_auto_commit,
                **self.consumer_config,
            }

            self.consumer = KafkaConsumer(*self.topics, **config)
            logger.info("Successfully connected to Kafka")

    def read_batch(self, batch_size: int = 1000) -> Iterator[Optional[pa.RecordBatch]]:
        """
        Read data batches from Kafka.

        Args:
            batch_size: Number of messages to accumulate per batch

        Yields:
            Arrow RecordBatch containing the data with topic metadata, or None when no data
            is available (to allow pipeline to check flush conditions without blocking)
        """
        if not self.consumer:
            raise RuntimeError("Not connected. Call connect() first.")

        with tracer.start_as_current_span("kafka_read_batch"):
            # Group messages by topic
            messages_by_topic = {}

            while True:
                # Poll for messages with timeout (non-blocking)
                raw_messages = self.consumer.poll(
                    timeout_ms=self.poll_timeout_ms, max_records=batch_size
                )

                if not raw_messages:
                    # No messages available - yield accumulated messages by topic first
                    if messages_by_topic:
                        for topic, messages in messages_by_topic.items():
                            batch = self._json_to_arrow_batch(messages, topic)
                            logger.debug(
                                "Created batch with %d records from topic %s", len(messages), topic
                            )
                            yield batch
                        messages_by_topic = {}
                    else:
                        # No messages accumulated, yield None to allow flush checks
                        logger.debug("No messages from Kafka, yielding None to allow flush checks")
                        yield None
                    continue

                # Process messages from poll
                for topic_partition, records in raw_messages.items():
                    topic = topic_partition.topic
                    for message in records:
                        with tracer.start_as_current_span("kafka_process_message"):
                            try:
                                if self.value_format == "json":
                                    data = json.loads(message.value.decode("utf-8"))
                                    # Group messages by topic
                                    if topic not in messages_by_topic:
                                        messages_by_topic[topic] = []
                                    messages_by_topic[topic].append(data)
                                elif self.value_format == "arrow_ipc":
                                    # Deserialize Arrow IPC format and add topic metadata
                                    reader = pa.ipc.open_stream(message.value)
                                    for batch in reader:
                                        # Add topic metadata to the batch
                                        schema_with_metadata = batch.schema.with_metadata(
                                            {
                                                **(batch.schema.metadata or {}),
                                                b"topic": topic.encode(),
                                            }
                                        )
                                        arrays = [batch.column(i) for i in range(batch.num_columns)]
                                        batch_with_metadata = pa.record_batch(
                                            arrays, schema=schema_with_metadata
                                        )
                                        yield batch_with_metadata
                                    continue
                                else:
                                    raise ValueError(
                                        f"Unsupported value format: {self.value_format}"
                                    )

                                # Check if any topic has accumulated enough messages
                                topics_to_yield = [
                                    t
                                    for t, msgs in messages_by_topic.items()
                                    if len(msgs) >= batch_size
                                ]
                                for t in topics_to_yield:
                                    batch = self._json_to_arrow_batch(messages_by_topic[t], t)
                                    logger.debug(
                                        "Created batch with %d records from topic %s",
                                        len(messages_by_topic[t]),
                                        t,
                                    )
                                    yield batch
                                    del messages_by_topic[t]

                            except Exception as e:
                                logger.error("Error processing message: %s", e, exc_info=True)
                                continue

    def _json_to_arrow_batch(self, messages: list[dict], topic: str) -> pa.RecordBatch:
        """
        Convert a list of JSON messages to Arrow RecordBatch with topic metadata.

        Args:
            messages: List of JSON message dictionaries
            topic: Kafka topic name

        Returns:
            Arrow RecordBatch with topic stored in schema metadata
        """
        # Create a table from the list of dictionaries
        table = pa.Table.from_pylist(messages)
        # Add topic metadata to schema
        schema_with_metadata = table.schema.with_metadata({b"topic": topic.encode()})
        # Rebuild table with metadata
        table = pa.table(table.to_pydict(), schema=schema_with_metadata)
        # Convert to a single batch
        if table.num_rows > 0:
            return table.to_batches()[0]
        # Return empty batch with metadata
        return pa.record_batch([], schema=pa.schema([], metadata={b"topic": topic.encode()}))

    def close(self) -> None:
        """Close the Kafka consumer."""
        with tracer.start_as_current_span("kafka_close"):
            if self.consumer:
                logger.info("Closing Kafka consumer")
                self.consumer.close()
                self.consumer = None
