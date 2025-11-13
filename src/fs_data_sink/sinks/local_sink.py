"""Local filesystem data sink implementation."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from opentelemetry import trace

from fs_data_sink.types import DataSink

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class LocalSink(DataSink):
    """
    Local filesystem data sink that writes Arrow data to local disk in Parquet format.

    Supports partitioning and compression for efficient analytics.
    Batches are buffered in memory and written as Parquet files only when flush() is called.
    """

    def __init__(
        self,
        base_path: str,
        compression: str = "snappy",
        partition_by: Optional[list[str]] = None,
    ):
        """
        Initialize Local sink.

        Args:
            base_path: Base directory path for writing files
            compression: Compression codec for Parquet ('snappy', 'gzip', 'brotli', 'zstd', 'none')
            partition_by: List of column names to partition by
        """
        self.base_path = Path(base_path).resolve()
        self.compression = compression
        self.partition_by = partition_by or []
        self.file_counter = 0
        self.buffered_batches: list[pa.RecordBatch] = []

    def connect(self) -> None:
        """Establish connection (create base directory if needed)."""
        with tracer.start_as_current_span("local_connect"):
            logger.info("Initializing local sink: path=%s", self.base_path)

            # Create base directory if it doesn't exist
            try:
                self.base_path.mkdir(parents=True, exist_ok=True)
                logger.info("Successfully initialized local sink")
            except Exception as e:
                logger.error("Failed to create base directory: %s", e)
                raise

    def write_batch(
        self, batch: pa.RecordBatch, partition_cols: Optional[list[str]] = None
    ) -> None:
        """
        Buffer a batch of data in memory. Data is written to disk only when flush() is called.

        Args:
            batch: Arrow RecordBatch to buffer
            partition_cols: Optional list of column names to use for partitioning (stored for flush)
        """
        with tracer.start_as_current_span("local_write_batch") as span:
            span.set_attribute("batch.num_rows", batch.num_rows)

            # Buffer the batch in memory
            self.buffered_batches.append(batch)
            logger.debug(
                "Buffered batch: %d rows (total buffered: %d batches)",
                batch.num_rows,
                len(self.buffered_batches),
            )

    def flush(self) -> None:
        """Flush buffered batches to disk as a Parquet file."""
        if not self.buffered_batches:
            logger.debug("No buffered batches to flush")
            return

        with tracer.start_as_current_span("local_flush") as span:
            try:
                # Combine all buffered batches into a single table
                table = pa.Table.from_batches(self.buffered_batches)
                num_batches = len(self.buffered_batches)
                span.set_attribute("flush.num_batches", num_batches)
                span.set_attribute("flush.num_rows", table.num_rows)

                # Generate file path with timestamp and counter
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                self.file_counter += 1

                # Build path with partitions
                path_parts = []
                if self.partition_by and table.num_rows > 0:
                    # Create partition directories
                    for col in self.partition_by:
                        if col in table.column_names:
                            # Get first value for partition (simple partitioning)
                            val = table[col][0].as_py()
                            path_parts.append(f"{col}={val}")

                # Construct full file path
                if path_parts:
                    file_dir = self.base_path / "/".join(path_parts)
                else:
                    file_dir = self.base_path

                file_dir.mkdir(parents=True, exist_ok=True)
                file_name = f"data_{timestamp}_{self.file_counter:06d}.parquet"
                file_path = file_dir / file_name

                # Write to local file
                pq.write_table(
                    table,
                    file_path,
                    compression=self.compression,
                    use_dictionary=True,
                    version="2.6",
                )

                file_size = file_path.stat().st_size
                logger.info(
                    "Flushed %d batches to local: %s (%d rows, %d bytes)",
                    num_batches,
                    file_path,
                    table.num_rows,
                    file_size,
                )

                # Clear the buffer
                self.buffered_batches.clear()

            except Exception as e:
                logger.error("Error flushing batches to local filesystem: %s", e, exc_info=True)
                raise

    def close(self) -> None:
        """Close the connection."""
        logger.info("Closing local sink connection")
