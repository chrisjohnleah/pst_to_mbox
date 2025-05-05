.PHONY: help test run dev clean build coverage

help:
	@echo "PST to MBOX Converter - Available commands:"
	@echo "  make test      - Run tests"
	@echo "  make coverage  - Run tests with coverage report"
	@echo "  make run       - Run the converter in production mode"
	@echo "  make dev       - Run the converter in development mode"
	@echo "  make build     - Build the Docker image"
	@echo "  make clean     - Remove temporary files and directories"

test:
	@echo "Running tests..."
	docker-compose -f docker-compose.dev.yml run --rm pst_to_mbox pytest -xvs tests/

coverage:
	@echo "Running tests with coverage report..."
	docker-compose -f docker-compose.dev.yml run --rm pst_to_mbox pytest --cov=. --cov-report=term --cov-report=html tests/
	@echo "HTML coverage report generated in htmlcov/ directory"

run:
	@echo "Running converter in production mode..."
	mkdir -p target_files mbox_dir output
	docker-compose up

dev:
	@echo "Running converter in development mode..."
	mkdir -p target_files mbox_dir output
	docker-compose -f docker-compose.dev.yml up

build:
	@echo "Building Docker image..."
	docker-compose build

clean:
	@echo "Cleaning up..."
	docker-compose down
	docker-compose -f docker-compose.dev.yml down
	rm -rf __pycache__
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -f .coverage
	find . -name "*.pyc" -delete
