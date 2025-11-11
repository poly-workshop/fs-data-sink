"""S3 data sink implementation."""

import logging
from datetime import datetime
from io import BytesIO
from typing import Optional

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from opentelemetry import trace

from fs_data_sink.types import DataSink

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class S3Sink(DataSink):
    """
    S3 data sink that writes Arrow data to S3 in Parquet format.

    Supports partitioning and compression for efficient analytics.
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
    ):
        """
        Initialize S3 sink.

        Args:
            bucket: S3 bucket name
            prefix: Prefix (folder) for objects in the bucket
            aws_access_key_id: AWS access key (optional, can use IAM role)
            aws_secret_access_key: AWS secret key (optional, can use IAM role)
            region_name: AWS region
            endpoint_url: S3 endpoint URL (optional, for S3-compatible services like MinIO)
            compression: Compression codec for Parquet ('snappy', 'gzip', 'brotli', 'zstd', 'none')
            partition_by: List of column names to partition by
            s3_config: Additional S3 client configuration
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
        self.s3_client = None
        self.file_counter = 0

    def connect(self) -> None:
        """Establish connection to S3."""
        with tracer.start_as_current_span("s3_connect"):
            logger.info("Connecting to S3: bucket=%s, region=%s", self.bucket, self.region_name)

            session_config = {
                "region_name": self.region_name,
                **self.s3_config,
            }

            if self.aws_access_key_id and self.aws_secret_access_key:
                session_config["aws_access_key_id"] = self.aws_access_key_id
                session_config["aws_secret_access_key"] = self.aws_secret_access_key

            if self.endpoint_url:
                session_config["endpoint_url"] = self.endpoint_url

            self.s3_client = boto3.client("s3", **session_config)

            # Test connection by checking if bucket exists
            try:
                self.s3_client.head_bucket(Bucket=self.bucket)
                logger.info("Successfully connected to S3")
            except Exception as e:
                logger.error("Failed to access S3 bucket: %s", e)
                raise

    def write_batch(
        self, batch: pa.RecordBatch, partition_cols: Optional[list[str]] = None
    ) -> None:
        """
        Write a batch of data to S3.

        Args:
            batch: Arrow RecordBatch to write
            partition_cols: Optional list of column names to use for partitioning
        """
        if not self.s3_client:
            raise RuntimeError("Not connected. Call connect() first.")

        with tracer.start_as_current_span("s3_write_batch"):
            try:
                # Use provided partition columns or default
                parts = partition_cols or self.partition_by

                # Convert batch to table
                table = pa.Table.from_batches([batch])

                # Generate S3 key with timestamp and counter
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                self.file_counter += 1

                # Build path with partitions
                path_parts = [self.prefix] if self.prefix else []

                if parts and table.num_rows > 0:
                    # Create partition directories
                    for col in parts:
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

                # Upload to S3
                buffer.seek(0)
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=s3_key,
                    Body=buffer.getvalue(),
                )

                logger.info(
                    "Wrote batch to S3: s3://%s/%s (%d rows, %d bytes)",
                    self.bucket,
                    s3_key,
                    table.num_rows,
                    buffer.tell(),
                )

            except Exception as e:
                logger.error("Error writing batch to S3: %s", e, exc_info=True)
                raise

    def flush(self) -> None:
        """Flush any buffered data to S3."""
        # S3 writes are immediate, no buffering
        logger.debug("S3Sink flush called (no-op)")

    def close(self) -> None:
        """Close the S3 connection."""
        with tracer.start_as_current_span("s3_close"):
            if self.s3_client:
                logger.info("Closing S3 connection")
                self.s3_client = None
