.PHONY: help install install-dev test lint format clean docker-up docker-down run-example

help:
	@echo "Available targets:"
	@echo "  install        - Install package"
	@echo "  install-dev    - Install package with dev dependencies"
	@echo "  test           - Run tests"
	@echo "  lint           - Run linters"
	@echo "  format         - Format code"
	@echo "  clean          - Clean build artifacts"
	@echo "  docker-up      - Start Docker services"
	@echo "  docker-down    - Stop Docker services"
	@echo "  run-example    - Run example pipeline"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

test:
	pytest tests/ -v --cov=fs_data_sink --cov-report=term-missing

lint:
	ruff check src/ tests/
	mypy src/ --ignore-missing-imports

format:
	black src/ tests/
	ruff check --fix src/ tests/

clean:
	rm -rf build/ dist/ *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache/ .coverage htmlcov/

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down -v

docker-logs:
	docker-compose logs -f

run-example:
	@echo "Running example Kafka to LocalStack S3 pipeline..."
	@echo "Make sure Docker services are running (make docker-up)"
	@echo ""
	@echo "Setting up test environment..."
	@sleep 5
	@echo "Starting pipeline..."
	SOURCE_TYPE=kafka \
	KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
	KAFKA_TOPICS=test-topic \
	KAFKA_GROUP_ID=fs-data-sink-test \
	SINK_TYPE=s3 \
	S3_BUCKET=test-bucket \
	S3_PREFIX=raw-data \
	AWS_ACCESS_KEY_ID=test \
	AWS_SECRET_ACCESS_KEY=test \
	AWS_REGION=us-east-1 \
	fs-data-sink --log-level INFO --max-batches 5
