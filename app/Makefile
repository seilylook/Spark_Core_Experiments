.PHONY: build start stop test clean init_mongo verify_spark

# Image configuration
IMAGE_NAME = session_2-python-app
DEFAULT_TAG = latest

# ===========================
# Export Python dependencies
# ===========================
_requirements:
	@echo "=============================================="
	@echo "Exporting Python dependencies to requirements.txt..."
	@echo "=============================================="
	poetry export -f requirements.txt --output requirements.txt --without-hashes --with dev
	@echo "\n"

# ============================
# Build Docker image
# ============================
build: _requirements
	@echo "=============================================="
	@echo "Building Docker image $(IMAGE_NAME):$(DEFAULT_TAG)..."
	@echo "=============================================="
	docker build --no-cache -t $(IMAGE_NAME):$(DEFAULT_TAG) .
	@echo "\n"

# ============================
# Start application with Docker Compose
# ============================
start: build
	@echo "========================="
	@echo "Starting the application..."
	@echo "========================="
	docker compose up -d --build
	@echo "Waiting for MongoDB to start..."
	@sleep 5
	@make init_mongo
	@echo "\n"

# ============================
# Stop and remove containers, networks, and volumes
# ============================
stop:
	@echo "=========================="
	@echo "Stopping the application..."
	@echo "=========================="
	docker compose down
	@echo "\n"

# ============================
# Run tests inside the Docker container
# ============================
test: build
	@echo "======================="
	@echo "Running tests with pytest..."
	@echo "======================="
	mkdir -p target
	docker run --rm -v $(PWD)/target:/app/target $(IMAGE_NAME):$(DEFAULT_TAG) /bin/bash -c \
		'for test_file in $$(find tests -name "*.py" ! -name "__init__.py" ! -name "conftest.py"); do \
			base_name=$$(basename $$test_file .py); \
			pytest $$test_file --junitxml=target/$$base_name.xml; \
		done'
	@echo "\n"

# ============================
# Clean up local generated files
# ============================
clean:
	@echo "====================================="
	@echo "Cleaning up local generated files..."
	@echo "====================================="
	rm -f requirements.txt
	@echo "Removed requirements.txt"
	@echo "\n"

# ============================
# Run MongoDB initialization script
# ============================
init_mongo:
	@echo "====================================="
	@echo "Initializing MongoDB..."
	@echo "====================================="
	chmod +x ./scripts/init_mongo.sh
	./scripts/init_mongo.sh
	@echo "\n"

# ============================
# Verify the Spark Environment
# ============================
verify_spark:
	@echo "=============================="
	@echo "Verifying Spark cluster setup..."
	@echo "=============================="
	@docker exec spark-master spark-submit --version
	@echo "\n"
