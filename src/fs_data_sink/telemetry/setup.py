"""Telemetry setup for OpenTelemetry integration."""

import logging
import sys
from typing import Optional

from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

from fs_data_sink.config.settings import TelemetryConfig


def setup_logging(log_level: str, log_format: str) -> None:
    """
    Setup structured logging.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Format type ('json' or 'text')
    """
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    if log_format == "json":
        # JSON structured logging
        formatter = logging.Formatter(
            '{"time":"%(asctime)s","level":"%(levelname)s","name":"%(name)s",'
            '"message":"%(message)s","function":"%(funcName)s","line":%(lineno)d}'
        )
    else:
        # Text logging
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers = [handler]
    
    # Reduce noise from external libraries
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def setup_tracing(service_name: str, otlp_endpoint: Optional[str]) -> None:
    """
    Setup OpenTelemetry tracing.
    
    Args:
        service_name: Name of the service for trace identification
        otlp_endpoint: OTLP endpoint for exporting traces
    """
    resource = Resource.create({"service.name": service_name})
    
    tracer_provider = TracerProvider(resource=resource)
    
    if otlp_endpoint:
        # Setup OTLP exporter
        otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
        span_processor = BatchSpanProcessor(otlp_exporter)
        tracer_provider.add_span_processor(span_processor)
    
    trace.set_tracer_provider(tracer_provider)
    
    logging.getLogger(__name__).info(
        f"Tracing initialized for service: {service_name}"
        + (f", endpoint: {otlp_endpoint}" if otlp_endpoint else " (no exporter)")
    )


def setup_metrics(service_name: str, otlp_endpoint: Optional[str]) -> None:
    """
    Setup OpenTelemetry metrics.
    
    Args:
        service_name: Name of the service for metrics identification
        otlp_endpoint: OTLP endpoint for exporting metrics
    """
    resource = Resource.create({"service.name": service_name})
    
    if otlp_endpoint:
        # Setup OTLP exporter
        otlp_exporter = OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True)
        reader = PeriodicExportingMetricReader(otlp_exporter, export_interval_millis=5000)
        meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
    else:
        meter_provider = MeterProvider(resource=resource)
    
    metrics.set_meter_provider(meter_provider)
    
    logging.getLogger(__name__).info(
        f"Metrics initialized for service: {service_name}"
        + (f", endpoint: {otlp_endpoint}" if otlp_endpoint else " (no exporter)")
    )


def setup_telemetry(config: TelemetryConfig) -> None:
    """
    Setup complete telemetry stack (logging, tracing, metrics).
    
    Args:
        config: Telemetry configuration
    """
    # Always setup logging
    setup_logging(config.log_level, config.log_format)
    
    logger = logging.getLogger(__name__)
    logger.info("Telemetry setup started")
    
    # Setup OpenTelemetry if enabled
    if config.enabled:
        if config.trace_enabled:
            setup_tracing(config.service_name, config.otlp_endpoint)
        
        if config.metrics_enabled:
            setup_metrics(config.service_name, config.otlp_endpoint)
        
        logger.info("OpenTelemetry enabled")
    else:
        logger.info("OpenTelemetry disabled")
