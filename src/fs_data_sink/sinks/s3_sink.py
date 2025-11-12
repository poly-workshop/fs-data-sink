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
        """Flush buffered batches to S3 as a Parquet file."""
        if not self.buffered_batches:
            logger.debug("No buffered batches to flush")
            return

        if not self.s3_client:
            raise RuntimeError("Not connected. Call connect() first.")

        with tracer.start_as_current_span("s3_flush") as span:
            try:
                # Combine all buffered batches into a single table
                table = pa.Table.from_batches(self.buffered_batches)
                num_batches = len(self.buffered_batches)
                span.set_attribute("flush.num_batches", num_batches)
                span.set_attribute("flush.num_rows", table.num_rows)

                # Generate S3 key with timestamp and counter
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                self.file_counter += 1

                # Build path with partitions
                path_parts = [self.prefix] if self.prefix else []

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
                    num_batches,
                    self.bucket,
                    s3_key,
                    table.num_rows,
                    data_length,
                )

                # Clear the buffer
                self.buffered_batches.clear()

            except S3Error as e:
                logger.error("S3 error flushing batches: %s", e, exc_info=True)
                raise
            except Exception as e:
                logger.error("Unexpected error flushing batches to S3: %s", e, exc_info=True)
                raise

    def close(self) -> None:
        """Close the S3 connection."""
        with tracer.start_as_current_span("s3_close"):
            if self.s3_client:
                logger.info("Closing S3 connection")
                self.s3_client = None
