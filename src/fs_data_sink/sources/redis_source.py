"""Redis data source implementation."""

import json
import logging
from typing import Iterator, Optional

import pyarrow as pa
import redis
from opentelemetry import trace

from fs_data_sink.types import DataSource

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class RedisSource(DataSource):
    """
    Redis data source that reads Arrow data from Redis streams or lists.

    Supports Redis Streams (XREAD) and Lists (BLPOP) as data sources.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        stream_keys: Optional[list[str]] = None,
        list_keys: Optional[list[str]] = None,
        value_format: str = "json",
        block_timeout: int = 1000,
        continuous: bool = True,
        redis_config: Optional[dict] = None,
        consumer_group: Optional[str] = None,
        consumer_name: Optional[str] = None,
    ):
        """
        Initialize Redis source.

        Args:
            host: Redis host
            port: Redis port
            db: Redis database number
            password: Redis password
            stream_keys: List of Redis stream keys to read from
            list_keys: List of Redis list keys to read from
            value_format: Format of message values ('json' or 'arrow_ipc')
            block_timeout: Timeout in milliseconds for blocking operations
            continuous: If True, continuously consume data in a loop; if False, read once and stop
            redis_config: Additional Redis configuration
            consumer_group: Consumer group name (required for Redis Streams)
            consumer_name: Consumer name within the group (defaults to hostname if not provided)
        """
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.stream_keys = stream_keys or []
        self.list_keys = list_keys or []
        self.value_format = value_format
        self.block_timeout = block_timeout
        self.continuous = continuous
        self.redis_config = redis_config or {}
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name
        self.client: Optional[redis.Redis] = None
        self.stream_ids: dict[str, str] = dict.fromkeys(self.stream_keys, ">")

    def connect(self) -> None:
        """Establish connection to Redis."""
        with tracer.start_as_current_span("redis_connect"):
            logger.info(
                "Connecting to Redis: host=%s, port=%s, db=%s", self.host, self.port, self.db
            )

            # Validate that consumer_group is provided if using streams
            if self.stream_keys and not self.consumer_group:
                raise ValueError("consumer_group is required when using Redis Streams")

            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=False,  # Keep as bytes for Arrow IPC
                **self.redis_config,
            )

            # Test connection
            self.client.ping()
            logger.info("Successfully connected to Redis")

            # Set default consumer name if not provided
            if self.stream_keys and not self.consumer_name:
                import socket

                self.consumer_name = f"{socket.gethostname()}-{id(self)}"
                logger.info("Using default consumer name: %s", self.consumer_name)

            # Create consumer groups for streams if they don't exist
            if self.stream_keys and self.consumer_group:
                for stream_key in self.stream_keys:
                    try:
                        # Try to create the consumer group
                        # Start from the beginning ('0') to consume all messages
                        # Use mkstream=True to create stream if it doesn't exist
                        self.client.xgroup_create(
                            name=stream_key, groupname=self.consumer_group, id="0", mkstream=True
                        )
                        logger.info(
                            "Created consumer group '%s' for stream '%s'",
                            self.consumer_group,
                            stream_key,
                        )
                    except redis.exceptions.ResponseError as e:
                        # Group already exists, which is fine
                        if "BUSYGROUP" in str(e):
                            logger.info(
                                "Consumer group '%s' already exists for stream '%s'",
                                self.consumer_group,
                                stream_key,
                            )
                        else:
                            raise

    def read_batch(self, batch_size: int = 1000) -> Iterator[Optional[pa.RecordBatch]]:
        """
        Read data batches from Redis.

        Args:
            batch_size: Number of messages to accumulate per batch

        Yields:
            Arrow RecordBatch containing the data, or None when no data is available
            (to allow pipeline to check flush conditions without blocking)
        """
        if not self.client:
            raise RuntimeError("Not connected. Call connect() first.")

        with tracer.start_as_current_span("redis_read_batch"):
            # Continuously read and yield batches if continuous mode is enabled
            while True:
                messages = []

                # Read from streams if configured
                if self.stream_keys:
                    messages.extend(self._read_from_streams(batch_size))

                # Read from lists if configured
                if self.list_keys:
                    messages.extend(self._read_from_lists(batch_size - len(messages)))

                if messages:
                    if self.value_format == "json":
                        batch = self._json_to_arrow_batch(messages)
                        logger.debug("Created batch with %d records", len(messages))
                        yield batch
                    elif self.value_format == "arrow_ipc":
                        # Process Arrow IPC messages
                        for msg in messages:
                            try:
                                reader = pa.ipc.open_stream(msg)
                                yield from reader
                            except Exception as e:
                                logger.error("Error processing Arrow IPC message: %s", e)
                else:
                    # Yield None when no data is available to allow pipeline to check flush conditions
                    # This makes read_batch non-blocking so time-based flushes can be checked
                    if self.continuous:
                        logger.debug("No messages found, yielding None to allow flush checks")
                        yield None

                # If not in continuous mode, exit after one iteration
                if not self.continuous:
                    break

    def _read_from_streams(self, batch_size: int) -> list:
        """Read messages from Redis streams using consumer groups."""
        messages = []
        message_ids_to_ack = []  # Track message IDs for acknowledgment

        try:
            # Prepare stream IDs for XREADGROUP
            # Use ">" to receive messages never delivered to other consumers
            streams = dict.fromkeys(self.stream_keys, ">")

            # Read from all streams using consumer group
            results = self.client.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_name,
                streams=streams,
                count=batch_size,
                block=self.block_timeout,
            )

            if results:
                for stream_key, stream_messages in results:
                    stream_key_str = (
                        stream_key.decode() if isinstance(stream_key, bytes) else stream_key
                    )

                    for msg_id, msg_data in stream_messages:
                        msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id

                        # Track message ID and stream for acknowledgment
                        message_ids_to_ack.append((stream_key_str, msg_id_str))

                        # Extract message value
                        if b"value" in msg_data:
                            messages.append(msg_data[b"value"])
                        elif "value" in msg_data:
                            messages.append(msg_data["value"])

                # Acknowledge all messages after successful reading
                if message_ids_to_ack:
                    for stream_key, msg_id in message_ids_to_ack:
                        try:
                            self.client.xack(stream_key, self.consumer_group, msg_id)
                            logger.debug(
                                "Acknowledged message %s from stream %s", msg_id, stream_key
                            )
                        except Exception as ack_error:
                            logger.error(
                                "Error acknowledging message %s from stream %s: %s",
                                msg_id,
                                stream_key,
                                ack_error,
                            )

        except Exception as e:
            logger.error("Error reading from Redis streams: %s", e, exc_info=True)

        return messages

    def _read_from_lists(self, batch_size: int) -> list:
        """Read messages from Redis lists."""
        messages = []

        try:
            for _ in range(batch_size):
                if not self.list_keys:
                    break

                result = self.client.blpop(
                    self.list_keys, timeout=self.block_timeout // 1000  # Convert to seconds
                )

                if result:
                    _, value = result
                    messages.append(value)
                else:
                    break  # Timeout, no more messages

        except Exception as e:
            logger.error("Error reading from Redis lists: %s", e, exc_info=True)

        return messages

    def _json_to_arrow_batch(self, messages: list) -> pa.RecordBatch:
        """Convert a list of JSON messages to Arrow RecordBatch."""
        parsed_messages = []

        for msg in messages:
            try:
                if isinstance(msg, bytes):
                    data = json.loads(msg.decode("utf-8"))
                else:
                    data = json.loads(msg)
                parsed_messages.append(data)
            except Exception as e:
                logger.error("Error parsing JSON message: %s", e)

        if parsed_messages:
            table = pa.Table.from_pylist(parsed_messages)
            if table.num_rows > 0:
                return table.to_batches()[0]
            return pa.record_batch([], schema=pa.schema([]))

        return pa.record_batch([], schema=pa.schema([]))

    def close(self) -> None:
        """Close the Redis connection."""
        with tracer.start_as_current_span("redis_close"):
            if self.client:
                logger.info("Closing Redis connection")
                self.client.close()
                self.client = None
