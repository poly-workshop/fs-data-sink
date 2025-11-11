"""
FS Data Sink - Apache Arrow data pipeline from Kafka/Redis to HDFS/S3
"""

__version__ = "0.1.0"

from fs_data_sink.pipeline import DataPipeline

__all__ = ["DataPipeline"]
