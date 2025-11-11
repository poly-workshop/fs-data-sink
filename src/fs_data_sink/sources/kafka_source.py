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
        """
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.group_id = group_id
        self.value_format = value_format
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.consumer_config = consumer_config or {}
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

    def read_batch(self, batch_size: int = 1000) -> Iterator[pa.RecordBatch]:
        """
        Read data batches from Kafka.

        Args:
            batch_size: Number of messages to accumulate per batch

        Yields:
            Arrow RecordBatch containing the data
        """
        if not self.consumer:
            raise RuntimeError("Not connected. Call connect() first.")

        with tracer.start_as_current_span("kafka_read_batch"):
            messages = []

            for message in self.consumer:
                with tracer.start_as_current_span("kafka_process_message"):
                    try:
                        if self.value_format == "json":
                            data = json.loads(message.value.decode("utf-8"))
                            messages.append(data)
                        elif self.value_format == "arrow_ipc":
                            # Deserialize Arrow IPC format
                            reader = pa.ipc.open_stream(message.value)
                            for batch in reader:
                                yield batch
                            continue
                        else:
                            raise ValueError(f"Unsupported value format: {self.value_format}")

                        if len(messages) >= batch_size:
                            # Convert accumulated JSON messages to Arrow RecordBatch
                            if messages:
                                batch = self._json_to_arrow_batch(messages)
                                logger.debug("Created batch with %d records", len(messages))
                                yield batch
                                messages = []

                    except Exception as e:
                        logger.error("Error processing message: %s", e, exc_info=True)
                        continue

            # Yield remaining messages
            if messages:
                batch = self._json_to_arrow_batch(messages)
                logger.debug("Created final batch with %d records", len(messages))
                yield batch

    def _json_to_arrow_batch(self, messages: list[dict]) -> pa.RecordBatch:
        """Convert a list of JSON messages to Arrow RecordBatch."""
        # Create a table from the list of dictionaries
        table = pa.Table.from_pylist(messages)
        # Convert to a single batch
        if table.num_rows > 0:
            return table.to_batches()[0]
        return pa.record_batch([], schema=pa.schema([]))

    def close(self) -> None:
        """Close the Kafka consumer."""
        with tracer.start_as_current_span("kafka_close"):
            if self.consumer:
                logger.info("Closing Kafka consumer")
                self.consumer.close()
                self.consumer = None
