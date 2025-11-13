"""Main data pipeline orchestration."""

import logging
import time
from typing import Optional

from opentelemetry import metrics, trace

from fs_data_sink.config import Settings
from fs_data_sink.sinks import HDFSSink, LocalSink, S3Sink
from fs_data_sink.sources import KafkaSource, RedisSource
from fs_data_sink.types import DataSink, DataSource

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)
meter = metrics.get_meter(__name__)

# Metrics
batches_processed = meter.create_counter(
    "fs_data_sink.batches_processed",
    description="Number of batches processed",
    unit="1",
)
records_processed = meter.create_counter(
    "fs_data_sink.records_processed",
    description="Number of records processed",
    unit="1",
)
errors_encountered = meter.create_counter(
    "fs_data_sink.errors",
    description="Number of errors encountered",
    unit="1",
)


class DataPipeline:
    """
    Main data pipeline that orchestrates data flow from source to sink.
    """

    def __init__(self, settings: Settings):
        """
        Initialize data pipeline.

        Args:
            settings: Complete pipeline settings
        """
        self.settings = settings
        self.source: Optional[DataSource] = None
        self.sink: Optional[DataSink] = None

    def _create_source(self) -> DataSource:
        """Create and configure the data source."""
        source_config = self.settings.source

        if source_config.type == "kafka":
            if not source_config.bootstrap_servers or not source_config.topics:
                raise ValueError("Kafka source requires bootstrap_servers and topics")

            return KafkaSource(
                bootstrap_servers=source_config.bootstrap_servers,
                topics=source_config.topics,
                group_id=source_config.group_id or "fs-data-sink-group",
                value_format=source_config.value_format,
                consumer_config=source_config.extra_config,
            )

        if source_config.type == "redis":
            return RedisSource(
                host=source_config.host or "localhost",
                port=source_config.port or 6379,
                db=source_config.db or 0,
                password=source_config.password,
                stream_keys=source_config.stream_keys,
                list_keys=source_config.list_keys,
                value_format=source_config.value_format,
                continuous=source_config.continuous,
                redis_config=source_config.extra_config,
            )

        raise ValueError(f"Unsupported source type: {source_config.type}")

    def _create_sink(self) -> DataSink:
        """Create and configure the data sink."""
        sink_config = self.settings.sink

        if sink_config.type == "s3":
            if not sink_config.bucket:
                raise ValueError("S3 sink requires bucket")

            return S3Sink(
                bucket=sink_config.bucket,
                prefix=sink_config.prefix or "",
                aws_access_key_id=sink_config.aws_access_key_id,
                aws_secret_access_key=sink_config.aws_secret_access_key,
                region_name=sink_config.region_name,
                endpoint_url=sink_config.endpoint_url,
                compression=sink_config.compression,
                partition_by=sink_config.partition_by,
                s3_config=sink_config.extra_config,
            )

        if sink_config.type == "hdfs":
            if not sink_config.url or not sink_config.base_path:
                raise ValueError("HDFS sink requires url and base_path")

            return HDFSSink(
                url=sink_config.url,
                base_path=sink_config.base_path,
                user=sink_config.user,
                compression=sink_config.compression,
                partition_by=sink_config.partition_by,
                hdfs_config=sink_config.extra_config,
            )

        if sink_config.type == "local":
            if not sink_config.base_path:
                raise ValueError("Local sink requires base_path")

            return LocalSink(
                base_path=sink_config.base_path,
                compression=sink_config.compression,
                partition_by=sink_config.partition_by,
            )

        raise ValueError(f"Unsupported sink type: {sink_config.type}")

    def run(self) -> None:
        """
        Run the data pipeline.

        Reads data from the configured source and writes to the configured sink.
        """
        with tracer.start_as_current_span("pipeline_run") as span:
            try:
                # Create and connect source
                logger.info("Initializing %s source", self.settings.source.type)
                self.source = self._create_source()
                self.source.connect()
                span.set_attribute("source.type", self.settings.source.type)

                # Create and connect sink
                logger.info("Initializing %s sink", self.settings.sink.type)
                self.sink = self._create_sink()
                self.sink.connect()
                span.set_attribute("sink.type", self.settings.sink.type)

                # Process batches
                logger.info("Starting data pipeline")
                batch_count = 0
                total_records = 0
                max_batches = self.settings.pipeline.max_batches

                # Flush tracking
                last_flush_time = time.time()
                batches_since_flush = 0
                flush_interval_seconds = self.settings.pipeline.flush_interval_seconds
                flush_interval_batches = self.settings.pipeline.flush_interval_batches

                for batch in self.source.read_batch(self.settings.source.batch_size):
                    try:
                        # Check if flush is needed based on time interval (even when no data)
                        current_time = time.time()
                        should_flush = False
                        flush_reason = None

                        if (
                            flush_interval_seconds
                            and (current_time - last_flush_time) >= flush_interval_seconds
                        ):
                            should_flush = True
                            flush_reason = f"time interval ({flush_interval_seconds}s)"

                        # If batch is None (no data available), just check time-based flush
                        if batch is None:
                            if should_flush:
                                logger.info("Flushing sink (reason: %s)", flush_reason)
                                self.sink.flush()
                                last_flush_time = current_time
                                batches_since_flush = 0
                            continue

                        # Process the batch
                        with tracer.start_as_current_span("process_batch") as batch_span:
                            num_rows = batch.num_rows
                            batch_span.set_attribute("batch.num_rows", num_rows)

                            # Write batch to sink
                            self.sink.write_batch(
                                batch, partition_cols=self.settings.sink.partition_by
                            )

                            batch_count += 1
                            batches_since_flush += 1
                            total_records += num_rows

                            # Update metrics
                            batches_processed.add(
                                1,
                                {
                                    "source": self.settings.source.type,
                                    "sink": self.settings.sink.type,
                                },
                            )
                            records_processed.add(
                                num_rows,
                                {
                                    "source": self.settings.source.type,
                                    "sink": self.settings.sink.type,
                                },
                            )

                            logger.info(
                                "Processed batch %d: %d records (total: %d records)",
                                batch_count,
                                num_rows,
                                total_records,
                            )

                            # Check if flush is needed (time-based already checked, now check batch-based)
                            if not should_flush and (
                                flush_interval_batches
                                and batches_since_flush >= flush_interval_batches
                            ):
                                should_flush = True
                                flush_reason = f"batch count ({flush_interval_batches} batches)"

                            if should_flush:
                                logger.info("Flushing sink (reason: %s)", flush_reason)
                                self.sink.flush()
                                last_flush_time = current_time
                                batches_since_flush = 0

                            # Check if max batches reached
                            if max_batches and batch_count >= max_batches:
                                logger.info("Reached max batches limit: %d", max_batches)
                                break

                    except (IOError, OSError, ValueError, RuntimeError) as e:
                        errors_encountered.add(
                            1,
                            {"source": self.settings.source.type, "sink": self.settings.sink.type},
                        )

                        if self.settings.pipeline.error_handling == "raise":
                            raise
                        if self.settings.pipeline.error_handling == "log":
                            logger.error("Error processing batch: %s", e, exc_info=True)
                        # else: ignore

                # Final flush at the end
                logger.info("Flushing sink (final)")
                self.sink.flush()

                logger.info(
                    "Pipeline completed: %d batches, %d records", batch_count, total_records
                )
                span.set_attribute("pipeline.batches", batch_count)
                span.set_attribute("pipeline.records", total_records)

            except Exception as e:
                logger.error("Pipeline failed: %s", e, exc_info=True)
                errors_encountered.add(
                    1, {"source": self.settings.source.type, "sink": self.settings.sink.type}
                )
                raise

            finally:
                # Cleanup
                if self.source:
                    logger.info("Closing source connection")
                    self.source.close()

                if self.sink:
                    logger.info("Closing sink connection")
                    self.sink.close()
