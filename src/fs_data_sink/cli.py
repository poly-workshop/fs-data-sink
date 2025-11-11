"""Command-line interface for fs-data-sink."""

import logging
import sys

import click

from fs_data_sink.config import load_config
from fs_data_sink.pipeline import DataPipeline
from fs_data_sink.telemetry import setup_telemetry

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Path to configuration file (INI format)",
)
@click.option(
    "--source-type",
    type=click.Choice(["kafka", "redis"]),
    help="Data source type",
)
@click.option(
    "--sink-type",
    type=click.Choice(["s3", "hdfs"]),
    help="Data sink type",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    default="INFO",
    help="Logging level",
)
@click.option(
    "--max-batches",
    type=int,
    help="Maximum number of batches to process (for testing)",
)
def main(config, source_type, sink_type, log_level, max_batches):
    """
    FS Data Sink - Apache Arrow data pipeline from Kafka/Redis to HDFS/S3.

    This tool reads Apache Arrow data from Kafka or Redis and writes it to
    HDFS or S3 in Parquet format for analytics consumption.

    Configuration can be provided via:
    - INI configuration file (--config)
    - Environment variables
    - Command-line options

    Environment variables take precedence over config file values.
    """
    try:
        # Load configuration
        settings = load_config(config)

        # Override with CLI options
        if source_type:
            settings.source.type = source_type
        if sink_type:
            settings.sink.type = sink_type
        if log_level:
            settings.telemetry.log_level = log_level
        if max_batches:
            settings.pipeline.max_batches = max_batches

        # Setup telemetry
        setup_telemetry(settings.telemetry)

        logger.info("=" * 60)
        logger.info("Starting FS Data Sink")
        logger.info("=" * 60)
        logger.info("Source: %s", settings.source.type)
        logger.info("Sink: %s", settings.sink.type)
        logger.info("Compression: %s", settings.sink.compression)
        logger.info("=" * 60)

        # Create and run pipeline
        pipeline = DataPipeline(settings)
        pipeline.run()

        logger.info("=" * 60)
        logger.info("FS Data Sink completed successfully")
        logger.info("=" * 60)

        sys.exit(0)

    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(130)

    except Exception as e:
        logger.error("Pipeline failed: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
