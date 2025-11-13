# FS Data Sink

Apache Arrow data pipeline that reads and transfers data from Kafka or Redis to HDFS or S3 in Parquet format, enabling easy import into analytical databases like ClickHouse.

## Features

- **Multiple Sources**: Read from Kafka topics or Redis streams/lists
- **Multiple Sinks**: Write to S3 or HDFS
- **Apache Arrow**: Native Arrow support for high-performance data processing
- **Parquet Format**: Efficient columnar storage with compression
- **Batch Buffering**: Accumulate multiple batches in memory and write as single Parquet files on flush
- **Configurable Flushing**: Flush based on time intervals or batch counts
- **Partitioning**: Support for data partitioning by columns
- **OpenTelemetry**: Built-in observability with traces and metrics
- **Flexible Configuration**: YAML files, environment variables, or CLI options
- **Modern Python**: Built with Python 3.9+ and modern tooling

## Architecture

```
┌─────────────────┐
│  Data Sources   │
├─────────────────┤
│ • Kafka Topics  │  ──┐
│ • Redis Streams │    │
│ • Redis Lists   │    │
└─────────────────┘    │
                       ▼
              ┌─────────────────┐
              │  FS Data Sink   │
              ├─────────────────┤
              │ • Arrow batches │
              │ • Partitioning  │
              │ • Compression   │
              │ • Telemetry     │
              └─────────────────┘
                       │
                       ▼
┌─────────────────────────────────┐
│        Data Sinks               │
├─────────────────────────────────┤
│ • S3 (Parquet files)            │
│ • HDFS (Parquet files)          │
└─────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────┐
│   Analytics Databases           │
├─────────────────────────────────┤
│ • ClickHouse                    │
│ • Presto/Trino                  │
│ • Spark                         │
└─────────────────────────────────┘
```

## Installation

### Using pip

```bash
pip install fs-data-sink
```

### From source

```bash
git clone https://github.com/poly-workshop/fs-data-sink.git
cd fs-data-sink
pip install -e .
```

### Development installation

```bash
pip install -e ".[dev]"
```

## Quick Start

### 1. Configure the pipeline

Create a configuration file `config.yaml`:

```yaml
source:
  type: kafka
  bootstrap_servers:
    - localhost:9092
  topics:
    - my-data-topic
  group_id: fs-data-sink-group
  value_format: json
  batch_size: 1000

sink:
  type: s3
  bucket: my-data-bucket
  prefix: raw-data
  region_name: us-east-1
  compression: snappy
  partition_by:
    - date

telemetry:
  log_level: INFO
  log_format: json
```

### 2. Run the pipeline

```bash
fs-data-sink --config config.yaml
```

Or using environment variables:

```bash
export SOURCE_TYPE=kafka
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPICS=my-data-topic
export SINK_TYPE=s3
export S3_BUCKET=my-data-bucket

fs-data-sink
```

## Configuration

### Configuration Sources (in order of precedence)

1. Command-line options
2. Environment variables
3. Configuration file (YAML)

### Source Configuration

#### Kafka Source

```yaml
source:
  type: kafka
  bootstrap_servers:
    - broker1:9092
    - broker2:9092
  topics:
    - topic1
    - topic2
  group_id: my-consumer-group
  value_format: json  # or arrow_ipc
  batch_size: 1000
  extra_config:
    auto_offset_reset: earliest
    enable_auto_commit: true
```

Environment variables:
- `KAFKA_BOOTSTRAP_SERVERS`: Comma-separated list of brokers
- `KAFKA_TOPICS`: Comma-separated list of topics
- `KAFKA_GROUP_ID`: Consumer group ID

#### Redis Source

```yaml
source:
  type: redis
  host: localhost
  port: 6379
  db: 0
  password: optional
  stream_keys:
    - stream1
    - stream2
  list_keys:
    - list1
  value_format: json  # or arrow_ipc
  batch_size: 1000
  continuous: true  # Continuously consume data (default: true)
  consumer_group: my-consumer-group  # Required for Redis Streams
  consumer_name: optional  # Defaults to hostname-id if not provided
```

The Redis source supports two consumption modes:
- **Continuous mode** (default): Continuously polls Redis for new data, similar to Kafka consumer
- **One-shot mode**: Reads available data once and stops

**Redis Streams Consumer Groups:**
When using Redis Streams (`stream_keys`), a `consumer_group` is required. The source uses Redis consumer groups to:
- Track message consumption across multiple consumers
- Enable parallel processing with multiple pipeline instances
- Automatically acknowledge messages after successful processing with XACK
- Provide at-least-once delivery semantics

If `consumer_name` is not provided, it defaults to `{hostname}-{id}` to ensure uniqueness.

Environment variables:
- `REDIS_HOST`: Redis host
- `REDIS_PORT`: Redis port
- `REDIS_PASSWORD`: Redis password
- `REDIS_STREAM_KEYS`: Comma-separated list of stream keys
- `REDIS_CONTINUOUS`: Enable continuous consumption (true/false)
- `REDIS_CONSUMER_GROUP`: Consumer group name (required for streams)
- `REDIS_CONSUMER_NAME`: Consumer name within the group

### Sink Configuration

#### S3 Sink (MinIO Client)

The S3 sink now uses the MinIO Python client for better compatibility with MinIO servers, enhanced features, and improved reliability. It works seamlessly with AWS S3, MinIO, and other S3-compatible storage services.

```yaml
sink:
  type: s3
  bucket: my-bucket
  prefix: data/raw
  aws_access_key_id: optional  # Use IAM role if not provided
  aws_secret_access_key: optional
  region_name: us-east-1
  endpoint_url: optional  # For MinIO or other S3-compatible services
  compression: snappy  # snappy, gzip, brotli, zstd, none
  partition_by:
    - date
    - hour
```

Benefits of MinIO client:
- Better compatibility with MinIO servers
- Simplified API with better error handling
- Enhanced streaming support for large files
- Built-in retry logic for improved resilience
- Cross-cloud compatibility (AWS S3, MinIO, GCS, etc.)

Environment variables:
- `S3_BUCKET`: S3 bucket name
- `S3_PREFIX`: Prefix for objects
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `AWS_REGION`: AWS region
- `S3_ENDPOINT_URL`: Custom endpoint for MinIO or S3-compatible services

#### HDFS Sink

```yaml
sink:
  type: hdfs
  url: http://namenode:9870
  base_path: /data/raw
  user: hdfs
  compression: snappy
  partition_by:
    - date
```

Environment variables:
- `HDFS_URL`: HDFS NameNode URL
- `HDFS_BASE_PATH`: Base path for files
- `HDFS_USER`: HDFS user

### Telemetry Configuration

```yaml
telemetry:
  log_level: INFO  # DEBUG, INFO, WARNING, ERROR, CRITICAL
  log_format: json  # json or text
  enabled: true
  service_name: fs-data-sink
  otlp_endpoint: http://localhost:4317
  trace_enabled: true
  metrics_enabled: true
```

Environment variables:
- `LOG_LEVEL`: Logging level
- `OTEL_ENABLED`: Enable OpenTelemetry (true/false)
- `OTEL_SERVICE_NAME`: Service name for telemetry
- `OTEL_EXPORTER_OTLP_ENDPOINT`: OTLP endpoint URL

### Pipeline Configuration

```yaml
pipeline:
  max_batches: null  # null for unlimited
  batch_timeout_seconds: 30
  error_handling: log  # log, raise, or ignore
  flush_interval_seconds: null  # Flush interval in seconds (null = flush only at end)
  flush_interval_batches: null  # Flush after N batches (null = flush only at end)
```

**Flush Interval and Batching:**

The pipeline uses an efficient batching strategy where data is buffered in memory and written to Parquet files only when flushed. This reduces the number of small files created and improves performance.

**Flush Behavior:**
- Batches are accumulated in memory via `write_batch()` calls
- Parquet files are written to disk/S3/HDFS only when `flush()` is called
- Multiple buffered batches are combined into a single Parquet file per flush

**Flush Triggers:**

By default, the sink is flushed only when the pipeline completes. You can configure periodic flushing based on:

- **Time interval** (`flush_interval_seconds`): Flush after a specified number of seconds
- **Batch count** (`flush_interval_batches`): Flush after processing a specified number of batches

If both are set, the sink will flush when either condition is met.

**Non-blocking Behavior:**

Sources are non-blocking to enable time-based flushes even when no data is available:
- When no data is available from the source, the generator yields `None`
- This allows the pipeline to check flush conditions (time-based) without blocking
- Previously, the pipeline would block waiting for data, preventing time-based flushes

Example with flush intervals:
```yaml
pipeline:
  flush_interval_seconds: 60    # Flush every 60 seconds
  flush_interval_batches: 100   # OR flush every 100 batches
```

**Benefits:**
- Fewer, larger Parquet files instead of many small files
- Better compression ratios
- Reduced I/O operations
- Improved query performance in analytics databases

Environment variables:
- `PIPELINE_FLUSH_INTERVAL_SECONDS`: Flush interval in seconds
- `PIPELINE_FLUSH_INTERVAL_BATCHES`: Flush interval in batches

### File Merging Configuration

The sink can automatically merge small Parquet files into larger consolidated files based on configured time periods (hourly, daily, weekly, monthly). This reduces the number of small files and improves query performance.

```yaml
sink:
  type: s3  # or hdfs, local
  bucket: my-bucket
  prefix: raw-data
  compression: snappy
  partition_by:
    - date
  # File merging configuration
  merge_enabled: true              # Enable automatic file merging
  merge_period: hour               # Time period for grouping files: 'hour', 'day', 'week', 'month'
  merge_min_files: 2               # Minimum number of files required to trigger a merge
  merge_on_flush: false            # Whether to merge files during flush operations
```

**Merge Behavior:**
- Files are grouped by their timestamp (extracted from filename) and configured period
- Only groups with at least `merge_min_files` files will be merged
- Merged files are named with the pattern `merged_{period_key}.parquet` (e.g., `merged_20241113_06.parquet` for hourly)
- Original files are deleted after successful merge
- Already merged files are skipped in subsequent merge operations

**Merge Periods:**
- **hour**: Merges files created within the same hour (e.g., all files from 2024-11-13 06:00-06:59)
- **day**: Merges files created within the same day (e.g., all files from 2024-11-13)
- **week**: Merges files created within the same ISO week (e.g., week 46 of 2024)
- **month**: Merges files created within the same month (e.g., November 2024)

**Merge Triggers:**
- **Manual**: Call `sink.merge_files()` explicitly
- **On flush**: Set `merge_on_flush: true` to merge after each flush operation
- **Scheduled**: Run merge as a separate scheduled job/cron

Example configurations:

```yaml
# Hourly merge (merge files from each hour)
sink:
  merge_enabled: true
  merge_period: hour
  merge_min_files: 5
  merge_on_flush: false

# Daily merge (merge files from each day)
sink:
  merge_enabled: true
  merge_period: day
  merge_min_files: 10
  merge_on_flush: true
```

**Benefits:**
- Reduces the number of small files (Small File Problem)
- Improves query performance in analytics databases
- Better resource utilization in distributed systems (Spark, Presto, Hive)
- Reduced metadata overhead

Environment variables:
- `SINK_MERGE_ENABLED`: Enable file merging (true/false)
- `SINK_MERGE_PERIOD`: Merge period (hour/day/week/month)
- `SINK_MERGE_MIN_FILES`: Minimum files to trigger merge
- `SINK_MERGE_ON_FLUSH`: Merge on flush (true/false)

## Data Formats

### JSON Format

Messages are expected to be JSON objects:

```json
{
  "id": 1,
  "name": "Example",
  "timestamp": "2024-01-01T00:00:00Z",
  "value": 42.5
}
```

### Arrow IPC Format

Messages can be in Apache Arrow IPC (Inter-Process Communication) format for maximum performance. This is useful when producing data from systems that already use Arrow.

## Use Cases

### Real-time Data Lake Ingestion

Stream data from Kafka to S3/HDFS for long-term storage and analytics:

```bash
fs-data-sink \
  --source-type kafka \
  --sink-type s3 \
  --config production-config.yaml
```

### Batch Processing from Redis

Process accumulated data from Redis streams/lists:

```bash
fs-data-sink \
  --source-type redis \
  --sink-type hdfs \
  --max-batches 100
```

### Integration with ClickHouse

After data is written to S3/HDFS, import into ClickHouse:

```sql
-- Create table in ClickHouse
CREATE TABLE my_table
(
    id UInt32,
    name String,
    timestamp DateTime,
    value Float64
)
ENGINE = MergeTree()
ORDER BY (timestamp, id);

-- Import from S3
INSERT INTO my_table
SELECT * FROM s3(
    'https://my-bucket.s3.amazonaws.com/raw-data/*.parquet',
    'Parquet'
);
```

## Development

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/poly-workshop/fs-data-sink.git
cd fs-data-sink

# Install with development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run linters
ruff check .
black --check .

# Format code
black .
```

### Project Structure

```
fs-data-sink/
├── src/fs_data_sink/
│   ├── __init__.py
│   ├── cli.py              # CLI entry point
│   ├── pipeline.py         # Main pipeline orchestration
│   ├── types.py            # Base interfaces
│   ├── sources/            # Data source implementations
│   │   ├── kafka_source.py
│   │   └── redis_source.py
│   ├── sinks/              # Data sink implementations
│   │   ├── s3_sink.py
│   │   └── hdfs_sink.py
│   ├── config/             # Configuration management
│   │   └── settings.py
│   └── telemetry/          # Observability
│       └── setup.py
├── tests/                  # Test suite
├── config/                 # Example configurations
├── pyproject.toml          # Project metadata
└── README.md
```

## Observability

### Logging

Structured JSON logging is available:

```json
{
  "time": "2024-01-01T12:00:00",
  "level": "INFO",
  "name": "fs_data_sink.pipeline",
  "message": "Processed batch 1: 1000 records",
  "function": "run",
  "line": 145
}
```

### Metrics

When OpenTelemetry is enabled, the following metrics are exposed:

- `fs_data_sink.batches_processed`: Number of batches processed
- `fs_data_sink.records_processed`: Number of records processed
- `fs_data_sink.errors`: Number of errors encountered

### Traces

Distributed traces are available for:

- Pipeline execution
- Source operations (connect, read_batch, close)
- Sink operations (connect, write_batch, flush, close)
- Individual batch processing

## Performance Tips

1. **Batch Size**: Adjust `batch_size` based on message size and available memory
2. **Compression**: Use `snappy` for balance of speed and compression, `zstd` for better compression
3. **Partitioning**: Use appropriate partition columns for your query patterns
4. **Arrow IPC**: Use Arrow IPC format for maximum performance when possible
5. **Parallel Pipelines**: Run multiple pipeline instances with different consumer groups

## Troubleshooting

### Connection Issues

```bash
# Test Kafka connection
kafka-console-consumer --bootstrap-server localhost:9092 --topic my-topic --max-messages 1

# Test Redis connection
redis-cli ping

# Test S3 access
aws s3 ls s3://my-bucket/

# Test HDFS access
hdfs dfs -ls /
```

### Logging

Enable debug logging for troubleshooting:

```bash
fs-data-sink --log-level DEBUG --config config.yaml
```

## License

MIT License

## Contributing

Contributions are welcome! Please open an issue or pull request.

## Support

For issues and questions:
- GitHub Issues: https://github.com/poly-workshop/fs-data-sink/issues
- Documentation: See this README