"""Local filesystem data sink implementation."""

import logging
import os
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
        merge_enabled: bool = False,
        merge_period: str = "hour",
        merge_min_files: int = 2,
        merge_on_flush: bool = False,
    ):
        """
        Initialize Local sink.

        Args:
            base_path: Base directory path for writing files
            compression: Compression codec for Parquet ('snappy', 'gzip', 'brotli', 'zstd', 'none')
            partition_by: List of column names to partition by
            merge_enabled: Enable automatic file merging
            merge_period: Period for merging files ('hour', 'day', 'week', 'month')
            merge_min_files: Minimum number of files to trigger a merge
            merge_on_flush: Merge files during flush operations
        """
        self.base_path = Path(base_path).resolve()
        self.compression = compression
        self.partition_by = partition_by or []
        self.merge_enabled = merge_enabled
        self.merge_period = merge_period
        self.merge_min_files = merge_min_files
        self.merge_on_flush = merge_on_flush
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

                # Optionally merge files after flush
                if self.merge_enabled and self.merge_on_flush:
                    logger.info("Merging files after flush")
                    self.merge_files()

            except Exception as e:
                logger.error("Error flushing batches to local filesystem: %s", e, exc_info=True)
                raise

    def merge_files(self, period: Optional[str] = None) -> int:
        """
        Merge small Parquet files into larger consolidated files by time period.

        This method reads existing Parquet files, groups them by time period based on their
        filenames, and merges files in each group into a single consolidated file. Dictionary
        encoding is explicitly disabled for merged files to avoid schema compatibility issues
        when reading files that may have been written with different dictionary encodings.

        Args:
            period: Time period for grouping files ('hour', 'day', 'week', 'month')
                   If None, uses the sink's configured merge_period

        Returns:
            Number of files merged
        """
        if not self.merge_enabled:
            logger.debug("File merging is disabled")
            return 0

        merge_period = period or self.merge_period
        logger.info("Starting file merge with period: %s", merge_period)

        with tracer.start_as_current_span("local_merge") as span:
            span.set_attribute("merge.period", merge_period)
            total_merged = 0

            try:
                # Find all directories to process (including partitioned directories)
                dirs_to_process = [self.base_path]

                # If partitioned, include partition directories
                if self.partition_by:
                    # Use os.walk for Python 3.9+ compatibility
                    for root, dirs, _ in os.walk(self.base_path):
                        root_path = Path(root)
                        for d in dirs:
                            dirs_to_process.append(root_path / d)

                for directory in dirs_to_process:
                    if not directory.exists():
                        continue

                    # Group files by time period
                    file_groups = self._group_files_by_period(directory, merge_period)

                    for period_key, files in file_groups.items():
                        if len(files) < self.merge_min_files:
                            logger.debug(
                                "Skipping merge for period %s: only %d files (min: %d)",
                                period_key,
                                len(files),
                                self.merge_min_files,
                            )
                            continue

                        # Merge files in this period
                        merged = self._merge_file_group(directory, period_key, files)
                        total_merged += merged

                span.set_attribute("merge.files_merged", total_merged)
                logger.info("Merge completed: %d files merged", total_merged)
                return total_merged

            except Exception as e:
                logger.error("Error during file merge: %s", e, exc_info=True)
                raise

    def _group_files_by_period(
        self, directory: Path, period: str
    ) -> dict[str, list[Path]]:
        """Group Parquet files by time period based on filename timestamp."""
        from collections import defaultdict

        groups = defaultdict(list)

        # Find all parquet files in the directory
        parquet_files = list(directory.glob("data_*.parquet"))

        for file_path in parquet_files:
            try:
                # Extract timestamp from filename (format: data_YYYYMMDD_HHMMSS_*.parquet)
                filename = file_path.name
                parts = filename.split("_")
                if len(parts) < 3:
                    continue

                date_str = parts[1]  # YYYYMMDD
                time_str = parts[2]  # HHMMSS

                # Parse timestamp
                year = date_str[0:4]
                month = date_str[4:6]
                day = date_str[6:8]
                hour = time_str[0:2]

                # Group by period
                if period == "hour":
                    group_key = f"{year}{month}{day}_{hour}"
                elif period == "day":
                    group_key = f"{year}{month}{day}"
                elif period == "week":
                    # Calculate week number
                    from datetime import datetime
                    dt = datetime(int(year), int(month), int(day))
                    week = dt.isocalendar()[1]
                    group_key = f"{year}W{week:02d}"
                elif period == "month":
                    group_key = f"{year}{month}"
                else:
                    logger.warning("Unknown merge period: %s, using 'day'", period)
                    group_key = f"{year}{month}{day}"

                groups[group_key].append(file_path)

            except (ValueError, IndexError) as e:
                logger.warning("Could not parse timestamp from file %s: %s", file_path, e)
                continue

        return groups

    def _merge_file_group(
        self, directory: Path, period_key: str, files: list[Path]
    ) -> int:
        """Merge a group of files into a single consolidated file."""
        try:
            logger.info("Merging %d files for period %s", len(files), period_key)

            # Read all files (avoid dictionary encoding for easier merging)
            tables = []
            for file_path in files:
                try:
                    # Read table without dictionary encoding to avoid schema conflicts
                    parquet_file = pq.ParquetFile(file_path)
                    table = parquet_file.read(use_pandas_metadata=False)

                    # Convert any dictionary-encoded columns to regular columns
                    columns = []
                    for i, field in enumerate(table.schema):
                        column = table.column(i)
                        if pa.types.is_dictionary(field.type):
                            columns.append(column.dictionary_decode())
                        else:
                            columns.append(column)

                    # Rebuild table without dictionary encoding
                    if columns:
                        schema = pa.schema([
                            pa.field(
                                field.name,
                                field.type.value_type if pa.types.is_dictionary(field.type) else field.type
                            )
                            for field in table.schema
                        ])
                        table = pa.Table.from_arrays(columns, schema=schema)

                    tables.append(table)
                except Exception as e:
                    logger.error("Failed to read file %s: %s", file_path, e)
                    continue

            if not tables:
                logger.warning("No tables to merge for period %s", period_key)
                return 0

            # Concatenate all tables
            merged_table = pa.concat_tables(tables)

            # Generate merged filename
            merged_filename = f"merged_{period_key}.parquet"
            merged_path = directory / merged_filename

            # Write merged file (without forcing dictionary encoding)
            pq.write_table(
                merged_table,
                merged_path,
                compression=self.compression,
                use_dictionary=False,  # Don't use dictionary encoding for merged files
                version="2.6",
            )

            merged_size = merged_path.stat().st_size
            logger.info(
                "Created merged file: %s (%d rows, %d bytes)",
                merged_path,
                merged_table.num_rows,
                merged_size,
            )

            # Delete original files
            for file_path in files:
                try:
                    file_path.unlink()
                    logger.debug("Deleted original file: %s", file_path)
                except Exception as e:
                    logger.error("Failed to delete file %s: %s", file_path, e)

            return len(files)

        except Exception as e:
            logger.error("Error merging files for period %s: %s", period_key, e, exc_info=True)
            return 0

    def close(self) -> None:
        """Close the connection."""
        logger.info("Closing local sink connection")
