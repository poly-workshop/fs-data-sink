"""Tests for configuration management."""

import os
import tempfile

from fs_data_sink.config import load_config


def test_load_config_from_yaml():
    """Test loading configuration from YAML file."""
    config_content = """
source:
  type: kafka
  bootstrap_servers:
    - localhost:9092
  topics:
    - test-topic
  group_id: test-group

sink:
  type: s3
  bucket: test-bucket
  region_name: us-west-2
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config_content)
        config_path = f.name

    try:
        settings = load_config(config_path)

        assert settings.source.type == "kafka"
        assert settings.source.bootstrap_servers == ["localhost:9092"]
        assert settings.source.topics == ["test-topic"]
        assert settings.source.group_id == "test-group"

        assert settings.sink.type == "s3"
        assert settings.sink.bucket == "test-bucket"
        assert settings.sink.region_name == "us-west-2"

    finally:
        os.unlink(config_path)


def test_load_config_with_env_overrides(monkeypatch):
    """Test that environment variables override config file."""
    config_content = """
source:
  type: kafka
  bootstrap_servers:
    - localhost:9092

sink:
  type: s3
  bucket: original-bucket
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config_content)
        config_path = f.name

    try:
        # Set environment variable
        monkeypatch.setenv("S3_BUCKET", "overridden-bucket")
        monkeypatch.setenv("SOURCE_TYPE", "redis")

        settings = load_config(config_path)

        # Check overrides worked
        assert settings.source.type == "redis"
        assert settings.sink.bucket == "overridden-bucket"

    finally:
        os.unlink(config_path)


def test_default_values():
    """Test default configuration values."""
    config_content = """
source:
  type: kafka
  bootstrap_servers:
    - localhost:9092
  topics:
    - test

sink:
  type: s3
  bucket: test
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config_content)
        config_path = f.name

    try:
        settings = load_config(config_path)

        # Check defaults
        assert settings.telemetry.log_level == "INFO"
        assert settings.telemetry.enabled is False
        assert settings.sink.compression == "snappy"
        assert settings.source.value_format == "json"
        assert settings.source.batch_size == 1000

    finally:
        os.unlink(config_path)
