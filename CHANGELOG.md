# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **BREAKING**: Replaced boto3 with MinIO Python client for S3 operations
  - Better compatibility with MinIO servers
  - Simplified API with improved error handling
  - Enhanced streaming support for large files
  - Built-in retry logic for improved resilience
  - Cross-cloud compatibility (AWS S3, MinIO, GCS, etc.)
- Updated docker-compose.yml to use MinIO instead of LocalStack for S3 testing
- Updated documentation (README, USAGE) to reflect MinIO client usage

### Added
- New `secure` parameter in S3Sink for controlling HTTPS/HTTP connections
- Example configuration file for Kafka to MinIO (config/example-kafka-to-minio.yaml)
- Enhanced S3 endpoint configuration with better MinIO support

### Fixed
- Improved S3 connection handling with better error messages

## [0.1.0] - 2024-11-10

### Added

#### Core Features
- Initial implementation of Apache Arrow data pipeline
- Support for reading from Kafka topics
- Support for reading from Redis streams and lists
- Support for writing to S3 in Parquet format
- Support for writing to HDFS in Parquet format
- Apache Arrow native data processing for high performance

#### Configuration
- Flexible configuration system supporting YAML files, environment variables, and CLI options
- Configuration precedence: CLI > Environment > Config File
- Example configurations for common use cases
- Environment variable override support

#### Observability
- Structured logging with JSON and text formats
- OpenTelemetry integration for distributed tracing
- OpenTelemetry metrics for monitoring
- Custom metrics: batches_processed, records_processed, errors
- Configurable log levels and formats

#### Data Processing
- Batch processing with configurable batch sizes
- Data partitioning support for efficient storage
- Multiple compression options: snappy, gzip, brotli, zstd, none
- JSON and Arrow IPC format support
- Automatic schema detection from JSON messages

#### CLI
- Command-line interface with `fs-data-sink` command
- Options for source type, sink type, log level, and max batches
- Help text and usage examples

#### Development
- Modern Python project structure with pyproject.toml
- Hatch build system
- Pre-configured development dependencies
- Code formatting with Black
- Linting with Ruff
- Type checking with mypy
- Testing with pytest

#### Documentation
- Comprehensive README with architecture diagram
- Detailed USAGE guide with examples
- CONTRIBUTING guidelines
- Docker Compose setup for local testing
- Example configurations for all source/sink combinations
- Integration examples with ClickHouse, Presto, and Spark

#### Testing
- Unit tests for configuration management
- Unit tests for type interfaces
- Test fixtures for Arrow data
- Docker Compose for integration testing
- Makefile for common development tasks

### Dependencies (v0.1.0)
- pyarrow >= 14.0.0 for Arrow data processing
- kafka-python >= 2.0.2 for Kafka integration
- redis >= 5.0.0 for Redis integration
- boto3 >= 1.34.0 for S3 integration (replaced by minio in later versions)
- hdfs >= 2.7.0 for HDFS integration
- click >= 8.1.0 for CLI
- pyyaml >= 6.0 for configuration
- opentelemetry-api >= 1.21.0 for observability
- opentelemetry-sdk >= 1.21.0 for observability
- opentelemetry-exporter-otlp >= 1.21.0 for observability
- python-dotenv >= 1.0.0 for environment variables

[0.1.0]: https://github.com/poly-workshop/fs-data-sink/releases/tag/v0.1.0
