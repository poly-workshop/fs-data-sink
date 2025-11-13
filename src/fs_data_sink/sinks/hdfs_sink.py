"""HDFS data sink implementation."""

import logging
from datetime import datetime
from io import BytesIO
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from hdfs import InsecureClient
from opentelemetry import trace

from fs_data_sink.types import DataSink

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class HDFSSink(DataSink):
    """
    HDFS data sink that writes Arrow data to HDFS in Parquet format.

    Supports partitioning and compression for efficient analytics.
    Batches are buffered in memory and written as Parquet files only when flush() is called.
    """

    def __init__(
        self,
        url: str,
        base_path: str,
        user: Optional[str] = None,
        compression: str = "snappy",
        partition_by: Optional[list[str]] = None,
        hdfs_config: Optional[dict] = None,
        merge_enabled: bool = False,
        merge_period: str = "hour",
        merge_min_files: int = 2,
        merge_on_flush: bool = False,
    ):
        """
        Initialize HDFS sink.

        Args:
            url: HDFS NameNode URL (e.g., 'http://namenode:9870')
            base_path: Base path in HDFS for writing files
            user: HDFS user (optional)
            compression: Compression codec for Parquet ('snappy', 'gzip', 'brotli', 'zstd', 'none')
            partition_by: List of column names to partition by
            hdfs_config: Additional HDFS client configuration
            merge_enabled: Enable automatic file merging
            merge_period: Period for merging files ('hour', 'day', 'week', 'month')
            merge_min_files: Minimum number of files to trigger a merge
            merge_on_flush: Merge files during flush operations
        """
        self.url = url
        self.base_path = base_path.rstrip("/")
        self.user = user
        self.compression = compression
        self.partition_by = partition_by or []
        self.hdfs_config = hdfs_config or {}
        self.merge_enabled = merge_enabled
        self.merge_period = merge_period
        self.merge_min_files = merge_min_files
        self.merge_on_flush = merge_on_flush
        self.client = None
        self.file_counter = 0
        self.buffered_batches: list[pa.RecordBatch] = []

    def connect(self) -> None:
        """Establish connection to HDFS."""
        with tracer.start_as_current_span("hdfs_connect"):
            logger.info("Connecting to HDFS: url=%s, user=%s", self.url, self.user)

            client_config = {
                "url": self.url,
                **self.hdfs_config,
            }

            if self.user:
                client_config["user"] = self.user

            self.client = InsecureClient(**client_config)

            # Test connection and create base path if it doesn't exist
            try:
                # Check if base path exists, create if not
                if not self.client.status(self.base_path, strict=False):
                    self.client.makedirs(self.base_path)
                    logger.info("Created base path: %s", self.base_path)

                logger.info("Successfully connected to HDFS")
            except Exception as e:
                logger.error("Failed to connect to HDFS: %s", e)
                raise

    def write_batch(
        self, batch: pa.RecordBatch, partition_cols: Optional[list[str]] = None
    ) -> None:
        """
        Buffer a batch of data in memory. Data is written to HDFS only when flush() is called.

        Args:
            batch: Arrow RecordBatch to buffer
            partition_cols: Optional list of column names to use for partitioning (stored for flush)
        """
        if not self.client:
            raise RuntimeError("Not connected. Call connect() first.")

        with tracer.start_as_current_span("hdfs_write_batch") as span:
            span.set_attribute("batch.num_rows", batch.num_rows)

            # Buffer the batch in memory
            self.buffered_batches.append(batch)
            logger.debug(
                "Buffered batch: %d rows (total buffered: %d batches)",
                batch.num_rows,
                len(self.buffered_batches),
            )

    def flush(self) -> None:
        """Flush buffered batches to HDFS as a Parquet file."""
        if not self.buffered_batches:
            logger.debug("No buffered batches to flush")
            return

        if not self.client:
            raise RuntimeError("Not connected. Call connect() first.")

        with tracer.start_as_current_span("hdfs_flush") as span:
            try:
                # Combine all buffered batches into a single table
                table = pa.Table.from_batches(self.buffered_batches)
                num_batches = len(self.buffered_batches)
                span.set_attribute("flush.num_batches", num_batches)
                span.set_attribute("flush.num_rows", table.num_rows)

                # Generate HDFS path with timestamp and counter
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                self.file_counter += 1

                # Build path with partitions
                path_parts = [self.base_path]

                if self.partition_by and table.num_rows > 0:
                    # Create partition directories
                    for col in self.partition_by:
                        if col in table.column_names:
                            # Get first value for partition (simple partitioning)
                            val = table[col][0].as_py()
                            path_parts.append(f"{col}={val}")

                # Ensure directory exists
                partition_path = "/".join(path_parts)
                if not self.client.status(partition_path, strict=False):
                    self.client.makedirs(partition_path)

                # Create file path
                file_name = f"data_{timestamp}_{self.file_counter:06d}.parquet"
                hdfs_path = f"{partition_path}/{file_name}"

                # Write to buffer
                buffer = BytesIO()
                pq.write_table(
                    table,
                    buffer,
                    compression=self.compression,
                    use_dictionary=True,
                    version="2.6",
                )

                # Upload to HDFS
                buffer.seek(0)
                data_length = len(buffer.getvalue())
                with self.client.write(hdfs_path, overwrite=False) as writer:
                    writer.write(buffer.getvalue())

                logger.info(
                    "Flushed %d batches to HDFS: %s (%d rows, %d bytes)",
                    num_batches,
                    hdfs_path,
                    table.num_rows,
                    data_length,
                )

                # Clear the buffer
                self.buffered_batches.clear()

                # Optionally merge files after flush
                if self.merge_enabled and self.merge_on_flush:
                    logger.info("Merging files after flush")
                    self.merge_files()

            except Exception as e:
                logger.error("Error flushing batches to HDFS: %s", e, exc_info=True)
                raise

    def merge_files(self, period: Optional[str] = None) -> int:
        """
        Merge small Parquet files into larger consolidated files by time period.

        Args:
            period: Time period for grouping files ('hour', 'day', 'week', 'month')
                   If None, uses the sink's configured merge_period

        Returns:
            Number of files merged
        """
        if not self.merge_enabled:
            logger.debug("File merging is disabled")
            return 0

        if not self.client:
            raise RuntimeError("Not connected. Call connect() first.")

        merge_period = period or self.merge_period
        logger.info("Starting HDFS file merge with period: %s", merge_period)

        with tracer.start_as_current_span("hdfs_merge") as span:
            span.set_attribute("merge.period", merge_period)
            total_merged = 0

            try:
                # Find all directories to process
                dirs_to_process = [self.base_path]

                # If partitioned, include partition directories
                if self.partition_by:
                    for item in self.client.list(self.base_path, status=False):
                        item_path = f"{self.base_path}/{item}"
                        if self.client.status(item_path)["type"] == "DIRECTORY":
                            dirs_to_process.append(item_path)

                for directory in dirs_to_process:
                    try:
                        # Group files by time period
                        file_groups = self._group_hdfs_files_by_period(directory, merge_period)

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
                            merged = self._merge_hdfs_file_group(directory, period_key, files)
                            total_merged += merged

                    except Exception as e:
                        logger.error("Error processing directory %s: %s", directory, e)
                        continue

                span.set_attribute("merge.files_merged", total_merged)
                logger.info("HDFS merge completed: %d files merged", total_merged)
                return total_merged

            except Exception as e:
                logger.error("Error during HDFS file merge: %s", e, exc_info=True)
                raise

    def _group_hdfs_files_by_period(
        self, directory: str, period: str
    ) -> dict[str, list[str]]:
        """Group HDFS Parquet files by time period based on filename timestamp."""
        from collections import defaultdict

        groups = defaultdict(list)

        try:
            # List all files in the directory
            files = self.client.list(directory, status=False)

            for filename in files:
                if not filename.endswith(".parquet") or not filename.startswith("data_"):
                    continue

                file_path = f"{directory}/{filename}"

                # Extract period from filename
                period_key = self._extract_period_from_filename(filename, period)
                if period_key:
                    groups[period_key].append(file_path)

        except Exception as e:
            logger.error("Error listing HDFS directory %s: %s", directory, e)

        return groups

    def _extract_period_from_filename(self, filename: str, period: str) -> Optional[str]:
        """Extract time period key from filename."""
        try:
            # Filename format: data_YYYYMMDD_HHMMSS_*.parquet
            parts = filename.split("_")
            if len(parts) < 3 or not filename.startswith("data_"):
                return None

            date_str = parts[1]  # YYYYMMDD
            time_str = parts[2]  # HHMMSS

            year = date_str[0:4]
            month = date_str[4:6]
            day = date_str[6:8]
            hour = time_str[0:2]

            if period == "hour":
                return f"{year}{month}{day}_{hour}"
            elif period == "day":
                return f"{year}{month}{day}"
            elif period == "week":
                from datetime import datetime
                dt = datetime(int(year), int(month), int(day))
                week = dt.isocalendar()[1]
                return f"{year}W{week:02d}"
            elif period == "month":
                return f"{year}{month}"
            else:
                return f"{year}{month}{day}"

        except (ValueError, IndexError) as e:
            logger.warning("Could not parse timestamp from filename %s: %s", filename, e)
            return None

    def _merge_hdfs_file_group(
        self, directory: str, period_key: str, files: list[str]
    ) -> int:
        """Merge a group of HDFS files into a single consolidated file."""
        try:
            logger.info("Merging %d HDFS files for period %s in %s", len(files), period_key, directory)

            # Read all files (avoid dictionary encoding for easier merging)
            tables = []
            for file_path in files:
                try:
                    with self.client.read(file_path) as reader:
                        data = reader.read()
                    buffer = BytesIO(data)
                    parquet_file = pq.ParquetFile(buffer)
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
                    logger.error("Failed to read HDFS file %s: %s", file_path, e)
                    continue

            if not tables:
                logger.warning("No tables to merge for period %s", period_key)
                return 0

            # Concatenate all tables
            merged_table = pa.concat_tables(tables)

            # Generate merged filename
            merged_filename = f"merged_{period_key}.parquet"
            merged_path = f"{directory}/{merged_filename}"

            # Write merged file (without forcing dictionary encoding)
            buffer = BytesIO()
            pq.write_table(
                merged_table,
                buffer,
                compression=self.compression,
                use_dictionary=False,  # Don't use dictionary encoding for merged files
                version="2.6",
            )

            buffer.seek(0)
            with self.client.write(merged_path, overwrite=False) as writer:
                writer.write(buffer.getvalue())

            logger.info(
                "Created merged HDFS file: %s (%d rows, %d bytes)",
                merged_path,
                merged_table.num_rows,
                len(buffer.getvalue()),
            )

            # Delete original files
            for file_path in files:
                try:
                    self.client.delete(file_path)
                    logger.debug("Deleted original HDFS file: %s", file_path)
                except Exception as e:
                    logger.error("Failed to delete HDFS file %s: %s", file_path, e)

            return len(files)

        except Exception as e:
            logger.error("Error merging HDFS files for period %s: %s", period_key, e, exc_info=True)
            return 0

    def close(self) -> None:
        """Close the HDFS connection."""
        with tracer.start_as_current_span("hdfs_close"):
            if self.client:
                logger.info("Closing HDFS connection")
                self.client = None
