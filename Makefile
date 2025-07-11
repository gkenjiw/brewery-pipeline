
# Makefile for managing Apache Airflow with Docker

# Variables
DOCKER = docker compose
PROJECT_NAME = brewery_pipeline

# Commands
.PHONY: up down init logs shell clean test reset

# Start the Airflow services
up:
	@$(DOCKER) up --build

# Stop the Airflow services
down:
	@$(DOCKER) down

# Initialize the Airflow database
init:
	@$(DOCKER) run --rm airflow-init airflow db init

# View logs for the Airflow webserver
logs:
	@$(DOCKER) logs -f airflow-webserver

# Open a shell in the Airflow webserver container
shell:
	@$(DOCKER) exec airflow-webserver /bin/bash

# Clean up Docker volumes
clean:
	@$(DOCKER) down -v

# Run tests
test:
	@$(DOCKER) run --rm -v $(PWD):/app -w /app python:3.9 bash -c "pip install -r docker/airflow/requirements.txt && pytest tests/"

# Reset the project (down + clean + init + up)
reset: down clean init up
