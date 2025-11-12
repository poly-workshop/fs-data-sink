# Usage Guide

This guide provides detailed usage instructions for fs-data-sink.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Use Cases](#use-cases)
- [Integration Examples](#integration-examples)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## Installation

### From PyPI (when published)

```bash
pip install fs-data-sink
```

### From Source

```bash
git clone https://github.com/poly-workshop/fs-data-sink.git
cd fs-data-sink
pip install -e .
```

## Quick Start

### Example 1: Kafka to S3

```bash
# Set environment variables
export SOURCE_TYPE=kafka
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPICS=events
export KAFKA_GROUP_ID=fs-sink-consumer

export SINK_TYPE=s3
export S3_BUCKET=my-data-lake
export S3_PREFIX=raw/events
export AWS_REGION=us-east-1

# Run the pipeline
fs-data-sink --log-level INFO
```

### Example 2: Redis Streams to HDFS

```bash
# Set environment variables
export SOURCE_TYPE=redis
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_STREAM_KEYS=sensor-data,user-events

export SINK_TYPE=hdfs
export HDFS_URL=http://namenode:9870
export HDFS_BASE_PATH=/data/raw
export HDFS_USER=hdfs

# Run the pipeline
fs-data-sink --log-level INFO
```

### Example 3: Using Configuration File

Create `config.yaml`:

```yaml
source:
  type: kafka
  bootstrap_servers:
    - broker1.example.com:9092
    - broker2.example.com:9092
  topics:
    - user-events
    - system-logs
  group_id: fs-data-sink
  value_format: json
  batch_size: 5000

sink:
  type: s3
  bucket: company-data-lake
  prefix: raw/events
  region_name: us-west-2
  compression: zstd
  partition_by:
    - date
    - event_type

telemetry:
  log_level: INFO
  log_format: json
  enabled: true
  service_name: fs-data-sink-production
  otlp_endpoint: http://otel-collector:4317
```

Run with:

```bash
fs-data-sink --config config.yaml
```

## Configuration

### Source Configuration

#### Kafka

Full configuration options:

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
    # Any additional Kafka consumer configuration
    auto_offset_reset: earliest
    enable_auto_commit: true
    session_timeout_ms: 30000
    max_poll_records: 500
```

#### Redis

Full configuration options:

```yaml
source:
  type: redis
  host: redis.example.com
  port: 6379
  db: 0
  password: my-secret-password
  stream_keys:
    - stream:events
    - stream:logs
  list_keys:
    - queue:tasks
  value_format: json  # or arrow_ipc
  batch_size: 1000
  extra_config:
    # Any additional Redis client configuration
    socket_timeout: 5
    socket_connect_timeout: 5
```

### Sink Configuration

#### S3 (MinIO Client)

The S3 sink uses the MinIO Python client for enhanced compatibility and features. Works with AWS S3, MinIO, and other S3-compatible storage services.

Full configuration options:

```yaml
sink:
  type: s3
  bucket: my-bucket
  prefix: data/raw
  aws_access_key_id: AKIA...  # Optional, can use IAM role
  aws_secret_access_key: secret...  # Optional
  region_name: us-east-1
  endpoint_url: http://minio:9000  # Optional, for MinIO or S3-compatible services
  compression: snappy  # Options: snappy, gzip, brotli, zstd, none
  partition_by:
    - year
    - month
    - day
  extra_config:
    # Additional MinIO client configuration
    secure: true  # Use HTTPS (default: true)
```

**MinIO-specific example:**
```yaml
sink:
  type: s3
  bucket: data-lake
  prefix: raw
  aws_access_key_id: minioadmin
  aws_secret_access_key: minioadmin
  endpoint_url: http://localhost:9000
  region_name: us-east-1
  extra_config:
    secure: false  # Use HTTP for local MinIO
```

#### HDFS

Full configuration options:

```yaml
sink:
  type: hdfs
  url: http://namenode:9870
  base_path: /data/lakehouse/raw
  user: hdfs
  compression: snappy
  partition_by:
    - date
    - source
  extra_config:
    # Additional HDFS client configuration
    timeout: 120
```

### Environment Variables

All configuration can be overridden with environment variables:

#### Source
- `SOURCE_TYPE`: kafka or redis
- `KAFKA_BOOTSTRAP_SERVERS`: comma-separated
- `KAFKA_TOPICS`: comma-separated
- `KAFKA_GROUP_ID`: consumer group
- `REDIS_HOST`: Redis hostname
- `REDIS_PORT`: Redis port
- `REDIS_PASSWORD`: Redis password
- `REDIS_STREAM_KEYS`: comma-separated

#### Sink
- `SINK_TYPE`: s3 or hdfs
- `S3_BUCKET`: S3 bucket name
- `S3_PREFIX`: S3 object prefix
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `AWS_REGION`: AWS region
- `HDFS_URL`: HDFS NameNode URL
- `HDFS_BASE_PATH`: HDFS base path
- `HDFS_USER`: HDFS user

#### Telemetry
- `LOG_LEVEL`: DEBUG, INFO, WARNING, ERROR, CRITICAL
- `OTEL_ENABLED`: true/false
- `OTEL_SERVICE_NAME`: service name
- `OTEL_EXPORTER_OTLP_ENDPOINT`: OTLP endpoint

## Use Cases

### Real-time Data Lake Ingestion

Continuously stream data from Kafka to S3 for long-term storage:

```bash
fs-data-sink \
  --source-type kafka \
  --sink-type s3 \
  --config production.yaml
```

### Batch Processing

Process a specific number of batches and exit:

```bash
fs-data-sink \
  --source-type redis \
  --sink-type hdfs \
  --max-batches 100 \
  --config config.yaml
```

### Data Migration

Migrate data between systems with partitioning:

```yaml
source:
  type: kafka
  topics: [legacy-topic]
  
sink:
  type: s3
  bucket: new-system
  partition_by: [date, region, category]
```

## Integration Examples

### ClickHouse Integration

After data is in S3/HDFS, import to ClickHouse:

```sql
-- Create table
CREATE TABLE events
(
    event_id String,
    user_id UInt64,
    event_type String,
    timestamp DateTime,
    properties String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, user_id);

-- Import from S3
INSERT INTO events
SELECT *
FROM s3(
    'https://my-bucket.s3.amazonaws.com/raw/events/**/*.parquet',
    'Parquet'
);

-- Or create external table for direct queries
CREATE TABLE events_s3 AS events
ENGINE = S3(
    'https://my-bucket.s3.amazonaws.com/raw/events/**/*.parquet',
    'Parquet'
);
```

### Presto/Trino Integration

Query directly from S3:

```sql
-- Create external table
CREATE TABLE hive.default.events (
    event_id VARCHAR,
    user_id BIGINT,
    event_type VARCHAR,
    timestamp TIMESTAMP,
    properties VARCHAR
)
WITH (
    external_location = 's3://my-bucket/raw/events/',
    format = 'PARQUET',
    partitioned_by = ARRAY['date']
);

-- Query
SELECT event_type, COUNT(*)
FROM hive.default.events
WHERE date >= DATE '2024-01-01'
GROUP BY event_type;
```

### Apache Spark Integration

```python
# Read Parquet files
df = spark.read.parquet("s3://my-bucket/raw/events/")

# Process
result = df.groupBy("event_type").count()

# Write back
result.write.parquet("s3://my-bucket/processed/event_counts/")
```

## Monitoring

### Metrics

When OpenTelemetry is enabled, the following metrics are available:

- `fs_data_sink.batches_processed`: Number of batches processed
- `fs_data_sink.records_processed`: Number of records processed
- `fs_data_sink.errors`: Number of errors encountered

Each metric includes labels:
- `source`: Source type (kafka/redis)
- `sink`: Sink type (s3/hdfs)

### Logs

Structured JSON logging example:

```json
{
  "time": "2024-01-01T12:00:00.123Z",
  "level": "INFO",
  "name": "fs_data_sink.pipeline",
  "message": "Processed batch 42: 1000 records (total: 42000 records)",
  "function": "run",
  "line": 125
}
```

### Traces

Distributed tracing spans:

- `pipeline_run`: Overall pipeline execution
- `kafka_connect`, `redis_connect`: Source connection
- `s3_connect`, `hdfs_connect`: Sink connection
- `kafka_read_batch`, `redis_read_batch`: Reading data
- `s3_write_batch`, `hdfs_write_batch`: Writing data
- `process_batch`: Individual batch processing

### Prometheus Integration

If using Prometheus, configure OTLP to Prometheus conversion:

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

exporters:
  prometheus:
    endpoint: 0.0.0.0:8889

service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [prometheus]
```

## Troubleshooting

### Connection Issues

#### Kafka

```bash
# Test Kafka connectivity
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --max-messages 1

# Check consumer group
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group fs-data-sink-group
```

#### Redis

```bash
# Test Redis connectivity
redis-cli -h localhost -p 6379 ping

# Check stream
redis-cli -h localhost -p 6379 XINFO STREAM my-stream

# Check list
redis-cli -h localhost -p 6379 LLEN my-list
```

#### S3

```bash
# Test S3 access
aws s3 ls s3://my-bucket/

# Check permissions
aws s3api head-bucket --bucket my-bucket
```

#### HDFS

```bash
# Test HDFS access
hdfs dfs -ls /data/raw/

# Check NameNode status
curl http://namenode:9870/jmx
```

### Common Issues

#### Out of Memory

Reduce batch size:

```yaml
source:
  batch_size: 500  # Default is 1000
```

#### Slow Processing

Enable compression for better throughput:

```yaml
sink:
  compression: zstd  # Better compression than snappy
```

Or use Arrow IPC format for sources:

```yaml
source:
  value_format: arrow_ipc  # Faster than JSON
```

#### Kafka Consumer Lag

Increase parallelism by running multiple instances with the same group ID, or increase batch size:

```yaml
source:
  batch_size: 5000
```

### Debug Mode

Enable debug logging:

```bash
fs-data-sink --log-level DEBUG --config config.yaml 2>&1 | tee debug.log
```

### Testing Configuration

Test with limited batches:

```bash
fs-data-sink --max-batches 10 --config config.yaml
```

## Performance Tips

1. **Optimize Batch Size**: Larger batches = fewer writes but more memory
2. **Use Appropriate Compression**: 
   - `snappy`: Fast, good for hot data
   - `zstd`: Better compression, good for cold data
   - `gzip`: Balance between speed and compression
3. **Partitioning Strategy**: Align with query patterns
4. **Arrow IPC Format**: Use when possible for best performance
5. **Multiple Instances**: Run parallel consumers with same group ID

## Best Practices

1. **Use IAM Roles** for S3 instead of access keys
2. **Enable OpenTelemetry** in production
3. **Set appropriate batch sizes** based on message size
4. **Use partitioning** for time-series data
5. **Monitor consumer lag** for Kafka sources
6. **Set up alerts** on error metrics
7. **Use proper error handling** (log/raise based on criticality)
8. **Test configurations** with `--max-batches` first
9. **Use separate configs** for dev/staging/prod
10. **Version control** your configuration files
