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
        Write a batch of data to local filesystem.

        Args:
            batch: Arrow RecordBatch to write
            partition_cols: Optional list of column names to use for partitioning
        """
        with tracer.start_as_current_span("local_write_batch") as span:
            span.set_attribute("batch.num_rows", batch.num_rows)

            try:
                # Use provided partition columns or default
                parts = partition_cols or self.partition_by

                # Convert batch to table
                table = pa.Table.from_batches([batch])

                # Generate file path with timestamp and counter
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                self.file_counter += 1

                # Build path with partitions
                path_parts = []
                if parts and table.num_rows > 0:
                    # Create partition directories
                    for col in parts:
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

                logger.info(
                    "Wrote batch to local: %s (%d rows, %d bytes)",
                    file_path,
                    table.num_rows,
                    file_path.stat().st_size,
                )

            except Exception as e:
                logger.error("Error writing batch to local filesystem: %s", e, exc_info=True)
                raise

    def flush(self) -> None:
        """Flush any buffered data to disk."""
        # Local writes are immediate, no buffering needed
        pass

    def close(self) -> None:
        """Close the connection."""
        logger.info("Closing local sink connection")
