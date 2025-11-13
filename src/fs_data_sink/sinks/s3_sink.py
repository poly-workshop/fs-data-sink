"""S3 data sink implementation using MinIO client."""

import logging
from datetime import datetime
from io import BytesIO
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error
from opentelemetry import trace

from fs_data_sink.types import DataSink

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class S3Sink(DataSink):
    """
    S3 data sink that writes Arrow data to S3 in Parquet format using MinIO client.

    Supports partitioning and compression for efficient analytics.
    Works with AWS S3, MinIO, and other S3-compatible storage services.
    Batches are buffered in memory and written as Parquet files only when flush() is called.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        compression: str = "snappy",
        partition_by: Optional[list[str]] = None,
        s3_config: Optional[dict] = None,
        secure: bool = True,
        merge_enabled: bool = False,
        merge_period: str = "hour",
        merge_min_files: int = 2,
        merge_on_flush: bool = False,
    ):
        """
        Initialize S3 sink with MinIO client.

        Args:
            bucket: S3 bucket name
            prefix: Prefix (folder) for objects in the bucket
            aws_access_key_id: AWS access key (optional, can use IAM role)
            aws_secret_access_key: AWS secret key (optional, can use IAM role)
            region_name: AWS region
            endpoint_url: S3 endpoint URL (optional, defaults to AWS S3)
            compression: Compression codec for Parquet ('snappy', 'gzip', 'brotli', 'zstd', 'none')
            partition_by: List of column names to partition by
            s3_config: Additional MinIO client configuration
            secure: Use HTTPS for connections (default: True)
            merge_enabled: Enable automatic file merging
            merge_period: Period for merging files ('hour', 'day', 'week', 'month')
            merge_min_files: Minimum number of files to trigger a merge
            merge_on_flush: Merge files during flush operations
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.compression = compression
        self.partition_by = partition_by or []
        self.s3_config = s3_config or {}
        self.secure = secure
        self.merge_enabled = merge_enabled
        self.merge_period = merge_period
        self.merge_min_files = merge_min_files
        self.merge_on_flush = merge_on_flush
        self.s3_client = None
        self.file_counter = 0
        self.buffered_batches: list[pa.RecordBatch] = []

    def connect(self) -> None:
        """Establish connection to S3 using MinIO client."""
        with tracer.start_as_current_span("s3_connect"):
            # Determine endpoint
            if self.endpoint_url:
                # Parse endpoint URL to extract host
                endpoint = self.endpoint_url.replace("http://", "").replace("https://", "")
                secure = (
                    self.endpoint_url.startswith("https://")
                    if "://" in self.endpoint_url
                    else self.secure
                )
            else:
                # Default to AWS S3 endpoint
                endpoint = f"s3.{self.region_name}.amazonaws.com"
                secure = self.secure

            logger.info(
                "Connecting to S3: bucket=%s, endpoint=%s, region=%s, secure=%s",
                self.bucket,
                endpoint,
                self.region_name,
                secure,
            )

            # Create MinIO client
            self.s3_client = Minio(
                endpoint=endpoint,
                access_key=self.aws_access_key_id,
                secret_key=self.aws_secret_access_key,
                region=self.region_name,
                secure=secure,
                **self.s3_config,
            )

            # Test connection by checking if bucket exists
            try:
                if not self.s3_client.bucket_exists(self.bucket):
                    logger.warning(
                        "Bucket '%s' does not exist. Attempting to create it.", self.bucket
                    )
                    self.s3_client.make_bucket(self.bucket, location=self.region_name)
                    logger.info("Created bucket '%s'", self.bucket)
                logger.info("Successfully connected to S3")
            except S3Error as e:
                logger.error("Failed to access S3 bucket: %s", e)
                raise
            except Exception as e:
                logger.error("Unexpected error connecting to S3: %s", e)
                raise

    def write_batch(
        self, batch: pa.RecordBatch, partition_cols: Optional[list[str]] = None
    ) -> None:
        """
        Buffer a batch of data in memory. Data is written to S3 only when flush() is called.

        Args:
            batch: Arrow RecordBatch to buffer
            partition_cols: Optional list of column names to use for partitioning (stored for flush)
        """
        if not self.s3_client:
            raise RuntimeError("Not connected. Call connect() first.")

        with tracer.start_as_current_span("s3_write_batch") as span:
            span.set_attribute("batch.num_rows", batch.num_rows)

            # Buffer the batch in memory
            self.buffered_batches.append(batch)
            logger.debug(
                "Buffered batch: %d rows (total buffered: %d batches)",
                batch.num_rows,
                len(self.buffered_batches),
            )

    def flush(self) -> None:
        """Flush buffered batches to S3 as Parquet files, grouped by topic/stream_key."""
        if not self.buffered_batches:
            logger.debug("No buffered batches to flush")
            return

        if not self.s3_client:
            raise RuntimeError("Not connected. Call connect() first.")

        with tracer.start_as_current_span("s3_flush") as span:
            try:
                # Group batches by topic/stream_key from metadata
                batches_by_source = {}
                for batch in self.buffered_batches:
                    # Extract topic or stream_key from schema metadata
                    topic = None
                    stream_key = None
                    if batch.schema.metadata:
                        topic = batch.schema.metadata.get(b"topic")
                        stream_key = batch.schema.metadata.get(b"stream_key")

                    # Use topic or stream_key as the source identifier
                    source_id = None
                    if topic:
                        source_id = topic.decode() if isinstance(topic, bytes) else topic
                    elif stream_key:
                        source_id = (
                            stream_key.decode() if isinstance(stream_key, bytes) else stream_key
                        )

                    # Group batches by source_id (default to 'default' if no metadata)
                    if source_id not in batches_by_source:
                        batches_by_source[source_id] = []
                    batches_by_source[source_id].append(batch)

                # Flush each group separately
                total_batches = len(self.buffered_batches)
                span.set_attribute("flush.num_batches", total_batches)
                span.set_attribute("flush.num_sources", len(batches_by_source))

                for source_id, batches in batches_by_source.items():
                    self._flush_batches_for_source(source_id, batches)

                # Clear the buffer
                self.buffered_batches.clear()

                # Optionally merge files after flush
                if self.merge_enabled and self.merge_on_flush:
                    logger.info("Merging files after flush")
                    self.merge_files()

            except S3Error as e:
                logger.error("S3 error flushing batches: %s", e, exc_info=True)
                raise
            except Exception as e:
                logger.error("Unexpected error flushing batches to S3: %s", e, exc_info=True)
                raise

    def _flush_batches_for_source(
        self, source_id: Optional[str], batches: list[pa.RecordBatch]
    ) -> None:
        """
        Flush batches for a specific topic/stream_key to S3.

        Args:
            source_id: Topic name or stream_key (None for data without metadata)
            batches: List of batches to flush
        """
        # Combine batches into a single table
        table = pa.Table.from_batches(batches)

        # Generate S3 key with timestamp and counter
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        self.file_counter += 1

        # Build path with topic/stream_key folder
        path_parts = []
        if self.prefix:
            path_parts.append(self.prefix)

        # Add topic/stream_key as a folder level
        if source_id:
            path_parts.append(source_id)

        if self.partition_by and table.num_rows > 0:
            # Create partition directories
            for col in self.partition_by:
                if col in table.column_names:
                    # Get first value for partition (simple partitioning)
                    val = table[col][0].as_py()
                    path_parts.append(f"{col}={val}")

        path_parts.append(f"data_{timestamp}_{self.file_counter:06d}.parquet")
        s3_key = "/".join(path_parts)

        # Write to buffer
        buffer = BytesIO()
        pq.write_table(
            table,
            buffer,
            compression=self.compression,
            use_dictionary=True,
            version="2.6",
        )

        # Upload to S3 using MinIO client
        buffer.seek(0)
        data_length = len(buffer.getvalue())
        self.s3_client.put_object(
            bucket_name=self.bucket,
            object_name=s3_key,
            data=buffer,
            length=data_length,
            content_type="application/octet-stream",
        )

        logger.info(
            "Flushed %d batches to S3: s3://%s/%s (%d rows, %d bytes)",
            len(batches),
            self.bucket,
            s3_key,
            table.num_rows,
            data_length,
        )

    def merge_files(self, period: Optional[str] = None) -> int:
        """
        Merge small Parquet files into larger consolidated files by time period.

        This method reads existing Parquet files from S3, groups them by time period based on
        their filenames, and merges files in each group into a single consolidated file.
        Dictionary encoding is explicitly disabled for merged files to avoid schema compatibility
        issues when reading files that may have been written with different dictionary encodings.

        Args:
            period: Time period for grouping files ('hour', 'day', 'week', 'month')
                   If None, uses the sink's configured merge_period

        Returns:
            Number of files merged
        """
        if not self.merge_enabled:
            logger.debug("File merging is disabled")
            return 0

        if not self.s3_client:
            raise RuntimeError("Not connected. Call connect() first.")

        merge_period = period or self.merge_period
        logger.info("Starting S3 file merge with period: %s", merge_period)

        with tracer.start_as_current_span("s3_merge") as span:
            span.set_attribute("merge.period", merge_period)
            total_merged = 0

            try:
                # List all objects with the prefix
                objects = self.s3_client.list_objects(
                    bucket_name=self.bucket, prefix=self.prefix, recursive=True
                )

                # Group objects by directory and time period
                from collections import defaultdict

                dir_groups = defaultdict(lambda: defaultdict(list))

                for obj in objects:
                    if not obj.object_name.endswith(".parquet"):
                        continue
                    if obj.object_name.startswith(
                        f"{self.prefix}/merged_" if self.prefix else "merged_"
                    ):
                        # Skip already merged files
                        continue

                    # Get directory path
                    obj_dir = "/".join(obj.object_name.split("/")[:-1])
                    filename = obj.object_name.split("/")[-1]

                    # Extract period from filename
                    period_key = self._extract_period_from_filename(filename, merge_period)
                    if period_key:
                        dir_groups[obj_dir][period_key].append(obj)

                # Merge files in each directory/period group
                for obj_dir, period_groups in dir_groups.items():
                    for period_key, objects_list in period_groups.items():
                        if len(objects_list) < self.merge_min_files:
                            logger.debug(
                                "Skipping merge for %s/%s: only %d files (min: %d)",
                                obj_dir,
                                period_key,
                                len(objects_list),
                                self.merge_min_files,
                            )
                            continue

                        # Merge files in this period
                        merged = self._merge_s3_file_group(obj_dir, period_key, objects_list)
                        total_merged += merged

                span.set_attribute("merge.files_merged", total_merged)
                logger.info("S3 merge completed: %d files merged", total_merged)
                return total_merged

            except S3Error as e:
                logger.error("S3 error during merge: %s", e, exc_info=True)
                raise
            except Exception as e:
                logger.error("Error during S3 file merge: %s", e, exc_info=True)
                raise

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

    def _merge_s3_file_group(self, obj_dir: str, period_key: str, objects_list: list) -> int:
        """Merge a group of S3 objects into a single consolidated file."""
        try:
            logger.info(
                "Merging %d S3 objects for period %s in %s", len(objects_list), period_key, obj_dir
            )

            # Download and read all objects (avoid dictionary encoding for easier merging)
            tables = []
            for obj in objects_list:
                try:
                    # Download object to memory
                    response = self.s3_client.get_object(
                        bucket_name=self.bucket, object_name=obj.object_name
                    )
                    data = response.read()
                    response.close()
                    response.release_conn()

                    # Read as parquet
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
                        schema = pa.schema(
                            [
                                pa.field(
                                    field.name,
                                    (
                                        field.type.value_type
                                        if pa.types.is_dictionary(field.type)
                                        else field.type
                                    ),
                                )
                                for field in table.schema
                            ]
                        )
                        table = pa.Table.from_arrays(columns, schema=schema)

                    tables.append(table)

                except Exception as e:
                    logger.error("Failed to read S3 object %s: %s", obj.object_name, e)
                    continue

            if not tables:
                logger.warning("No tables to merge for period %s", period_key)
                return 0

            # Concatenate all tables
            merged_table = pa.concat_tables(tables)

            # Generate merged object name
            merged_name = f"merged_{period_key}.parquet"
            merged_key = f"{obj_dir}/{merged_name}" if obj_dir else merged_name

            # Write merged file to buffer (without forcing dictionary encoding)
            buffer = BytesIO()
            pq.write_table(
                merged_table,
                buffer,
                compression=self.compression,
                use_dictionary=False,  # Don't use dictionary encoding for merged files
                version="2.6",
            )

            # Upload merged file
            buffer.seek(0)
            data_length = len(buffer.getvalue())
            self.s3_client.put_object(
                bucket_name=self.bucket,
                object_name=merged_key,
                data=buffer,
                length=data_length,
                content_type="application/octet-stream",
            )

            logger.info(
                "Created merged S3 object: s3://%s/%s (%d rows, %d bytes)",
                self.bucket,
                merged_key,
                merged_table.num_rows,
                data_length,
            )

            # Delete original objects
            for obj in objects_list:
                try:
                    self.s3_client.remove_object(
                        bucket_name=self.bucket, object_name=obj.object_name
                    )
                    logger.debug("Deleted original S3 object: %s", obj.object_name)
                except Exception as e:
                    logger.error("Failed to delete S3 object %s: %s", obj.object_name, e)

            return len(objects_list)

        except Exception as e:
            logger.error("Error merging S3 files for period %s: %s", period_key, e, exc_info=True)
            return 0

    def close(self) -> None:
        """Close the S3 connection."""
        with tracer.start_as_current_span("s3_close"):
            if self.s3_client:
                logger.info("Closing S3 connection")
                self.s3_client = None
