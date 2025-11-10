"""Configuration settings for the data pipeline."""

import os
from dataclasses import dataclass, field
from typing import Optional
import yaml
from dotenv import load_dotenv


@dataclass
class SourceConfig:
    """Configuration for data sources."""
    type: str  # 'kafka' or 'redis'
    
    # Kafka specific
    bootstrap_servers: Optional[list[str]] = None
    topics: Optional[list[str]] = None
    group_id: Optional[str] = None
    
    # Redis specific
    host: Optional[str] = None
    port: Optional[int] = None
    db: Optional[int] = None
    password: Optional[str] = None
    stream_keys: Optional[list[str]] = None
    list_keys: Optional[list[str]] = None
    
    # Common
    value_format: str = "json"
    batch_size: int = 1000
    extra_config: dict = field(default_factory=dict)


@dataclass
class SinkConfig:
    """Configuration for data sinks."""
    type: str  # 's3' or 'hdfs'
    
    # S3 specific
    bucket: Optional[str] = None
    prefix: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    region_name: str = "us-east-1"
    
    # HDFS specific
    url: Optional[str] = None
    base_path: Optional[str] = None
    user: Optional[str] = None
    
    # Common
    compression: str = "snappy"
    partition_by: Optional[list[str]] = None
    extra_config: dict = field(default_factory=dict)


@dataclass
class TelemetryConfig:
    """Configuration for telemetry (logging and metrics)."""
    log_level: str = "INFO"
    log_format: str = "json"
    
    # OpenTelemetry
    enabled: bool = False
    service_name: str = "fs-data-sink"
    otlp_endpoint: Optional[str] = None
    trace_enabled: bool = True
    metrics_enabled: bool = True


@dataclass
class PipelineConfig:
    """Configuration for the pipeline behavior."""
    max_batches: Optional[int] = None
    batch_timeout_seconds: int = 30
    error_handling: str = "log"  # 'log', 'raise', or 'ignore'


@dataclass
class Settings:
    """Complete settings for the data pipeline."""
    source: SourceConfig
    sink: SinkConfig
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)


def load_config(config_path: Optional[str] = None) -> Settings:
    """
    Load configuration from YAML file and environment variables.
    
    Environment variables take precedence over file configuration.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Settings object with complete configuration
    """
    # Load environment variables
    load_dotenv()
    
    # Load from YAML file if provided
    config_data = {}
    if config_path and os.path.exists(config_path):
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f) or {}
    
    # Override with environment variables
    _apply_env_overrides(config_data)
    
    # Build Settings object
    source_config = SourceConfig(**config_data.get("source", {}))
    sink_config = SinkConfig(**config_data.get("sink", {}))
    telemetry_config = TelemetryConfig(**config_data.get("telemetry", {}))
    pipeline_config = PipelineConfig(**config_data.get("pipeline", {}))
    
    return Settings(
        source=source_config,
        sink=sink_config,
        telemetry=telemetry_config,
        pipeline=pipeline_config,
    )


def _apply_env_overrides(config_data: dict) -> None:
    """Apply environment variable overrides to configuration."""
    # Source overrides
    source = config_data.setdefault("source", {})
    
    if os.getenv("SOURCE_TYPE"):
        source["type"] = os.getenv("SOURCE_TYPE")
    
    # Kafka
    if os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
        source["bootstrap_servers"] = os.getenv("KAFKA_BOOTSTRAP_SERVERS").split(",")
    if os.getenv("KAFKA_TOPICS"):
        source["topics"] = os.getenv("KAFKA_TOPICS").split(",")
    if os.getenv("KAFKA_GROUP_ID"):
        source["group_id"] = os.getenv("KAFKA_GROUP_ID")
    
    # Redis
    if os.getenv("REDIS_HOST"):
        source["host"] = os.getenv("REDIS_HOST")
    if os.getenv("REDIS_PORT"):
        source["port"] = int(os.getenv("REDIS_PORT"))
    if os.getenv("REDIS_PASSWORD"):
        source["password"] = os.getenv("REDIS_PASSWORD")
    if os.getenv("REDIS_STREAM_KEYS"):
        source["stream_keys"] = os.getenv("REDIS_STREAM_KEYS").split(",")
    
    # Sink overrides
    sink = config_data.setdefault("sink", {})
    
    if os.getenv("SINK_TYPE"):
        sink["type"] = os.getenv("SINK_TYPE")
    
    # S3
    if os.getenv("S3_BUCKET"):
        sink["bucket"] = os.getenv("S3_BUCKET")
    if os.getenv("S3_PREFIX"):
        sink["prefix"] = os.getenv("S3_PREFIX")
    if os.getenv("AWS_ACCESS_KEY_ID"):
        sink["aws_access_key_id"] = os.getenv("AWS_ACCESS_KEY_ID")
    if os.getenv("AWS_SECRET_ACCESS_KEY"):
        sink["aws_secret_access_key"] = os.getenv("AWS_SECRET_ACCESS_KEY")
    if os.getenv("AWS_REGION"):
        sink["region_name"] = os.getenv("AWS_REGION")
    
    # HDFS
    if os.getenv("HDFS_URL"):
        sink["url"] = os.getenv("HDFS_URL")
    if os.getenv("HDFS_BASE_PATH"):
        sink["base_path"] = os.getenv("HDFS_BASE_PATH")
    if os.getenv("HDFS_USER"):
        sink["user"] = os.getenv("HDFS_USER")
    
    # Telemetry overrides
    telemetry = config_data.setdefault("telemetry", {})
    
    if os.getenv("LOG_LEVEL"):
        telemetry["log_level"] = os.getenv("LOG_LEVEL")
    if os.getenv("OTEL_ENABLED"):
        telemetry["enabled"] = os.getenv("OTEL_ENABLED").lower() in ("true", "1", "yes")
    if os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
        telemetry["otlp_endpoint"] = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if os.getenv("OTEL_SERVICE_NAME"):
        telemetry["service_name"] = os.getenv("OTEL_SERVICE_NAME")
