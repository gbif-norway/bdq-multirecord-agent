# BDQ Email Report Service - Docker Commands

.PHONY: help build up down logs shell test clean dev

help: ## Show this help message
	@echo "BDQ Email Report Service - Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build the Docker image
	docker-compose build

up: ## Start the service
	docker-compose up -d

down: ## Stop the service
	docker-compose down

logs: ## Show service logs
	docker-compose logs -f bdq-email-service

shell: ## Open a shell in the running container
	docker-compose exec bdq-email-service /bin/bash

test: ## Run tests using the test client
	docker-compose --profile test up test-client

clean: ## Clean up containers and images
	docker-compose down -v
	docker system prune -f

dev: ## Start in development mode with hot reload
	docker-compose -f docker-compose.dev.yml up --build

dev-logs: ## Show development logs
	docker-compose -f docker-compose.dev.yml logs -f

# Production commands
prod-build: ## Build for production
	docker build -t bdq-email-service:latest .

prod-run: ## Run production container
	docker run -d --name bdq-email-service -p 8080:8080 --env-file .env bdq-email-service:latest

prod-stop: ## Stop production container
	docker stop bdq-email-service && docker rm bdq-email-service
