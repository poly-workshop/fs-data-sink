"""Common types and interfaces for the data pipeline."""

from abc import ABC, abstractmethod
from typing import Iterator, Optional
import pyarrow as pa


class DataSource(ABC):
    """Abstract base class for data sources."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data source."""
        pass

    @abstractmethod
    def read_batch(self, batch_size: int = 1000) -> Iterator[pa.RecordBatch]:
        """
        Read data in batches as Arrow RecordBatch.
        
        Args:
            batch_size: Number of records per batch
            
        Yields:
            Arrow RecordBatch containing the data
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the connection to the data source."""
        pass


class DataSink(ABC):
    """Abstract base class for data sinks."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data sink."""
        pass

    @abstractmethod
    def write_batch(self, batch: pa.RecordBatch, partition_cols: Optional[list[str]] = None) -> None:
        """
        Write a batch of data to the sink.
        
        Args:
            batch: Arrow RecordBatch to write
            partition_cols: Optional list of column names to use for partitioning
        """
        pass

    @abstractmethod
    def flush(self) -> None:
        """Flush any buffered data to the sink."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the connection to the data sink."""
        pass
