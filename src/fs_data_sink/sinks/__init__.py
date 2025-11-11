"""Data sink implementations."""

from fs_data_sink.sinks.hdfs_sink import HDFSSink
from fs_data_sink.sinks.local_sink import LocalSink
from fs_data_sink.sinks.s3_sink import S3Sink

__all__ = ["HDFSSink", "LocalSink", "S3Sink"]
