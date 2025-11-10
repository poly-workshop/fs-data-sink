"""HDFS data sink implementation."""

import logging
from datetime import datetime
from typing import Optional
import pyarrow as pa
import pyarrow.parquet as pq
from hdfs import InsecureClient
from io import BytesIO
from opentelemetry import trace

from fs_data_sink.types import DataSink

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class HDFSSink(DataSink):
    """
    HDFS data sink that writes Arrow data to HDFS in Parquet format.
    
    Supports partitioning and compression for efficient analytics.
    """

    def __init__(
        self,
        url: str,
        base_path: str,
        user: Optional[str] = None,
        compression: str = "snappy",
        partition_by: Optional[list[str]] = None,
        hdfs_config: Optional[dict] = None,
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
        """
        self.url = url
        self.base_path = base_path.rstrip("/")
        self.user = user
        self.compression = compression
        self.partition_by = partition_by or []
        self.hdfs_config = hdfs_config or {}
        self.client = None
        self.file_counter = 0

    def connect(self) -> None:
        """Establish connection to HDFS."""
        with tracer.start_as_current_span("hdfs_connect"):
            logger.info(f"Connecting to HDFS: url={self.url}, user={self.user}")
            
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
                    logger.info(f"Created base path: {self.base_path}")
                
                logger.info("Successfully connected to HDFS")
            except Exception as e:
                logger.error(f"Failed to connect to HDFS: {e}")
                raise

    def write_batch(self, batch: pa.RecordBatch, partition_cols: Optional[list[str]] = None) -> None:
        """
        Write a batch of data to HDFS.
        
        Args:
            batch: Arrow RecordBatch to write
            partition_cols: Optional list of column names to use for partitioning
        """
        if not self.client:
            raise RuntimeError("Not connected. Call connect() first.")

        with tracer.start_as_current_span("hdfs_write_batch"):
            try:
                # Use provided partition columns or default
                parts = partition_cols or self.partition_by
                
                # Convert batch to table
                table = pa.Table.from_batches([batch])
                
                # Generate HDFS path with timestamp and counter
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                self.file_counter += 1
                
                # Build path with partitions
                path_parts = [self.base_path]
                
                if parts and table.num_rows > 0:
                    # Create partition directories
                    for col in parts:
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
                with self.client.write(hdfs_path, overwrite=False) as writer:
                    writer.write(buffer.getvalue())
                
                logger.info(
                    f"Wrote batch to HDFS: {hdfs_path} "
                    f"({table.num_rows} rows, {buffer.tell()} bytes)"
                )
                
            except Exception as e:
                logger.error(f"Error writing batch to HDFS: {e}", exc_info=True)
                raise

    def flush(self) -> None:
        """Flush any buffered data to HDFS."""
        # HDFS writes are immediate, no buffering
        logger.debug("HDFSSink flush called (no-op)")

    def close(self) -> None:
        """Close the HDFS connection."""
        with tracer.start_as_current_span("hdfs_close"):
            if self.client:
                logger.info("Closing HDFS connection")
                self.client = None
