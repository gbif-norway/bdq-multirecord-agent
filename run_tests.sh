#!/bin/bash

# BDQ Test Runner Script
# This script runs the comprehensive test suite in Docker

set -e

echo "🧪 Starting BDQ Test Suite..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Build the application image
echo "🔨 Building application image..."
docker-compose build bdq-app

# Run the test suite
echo "🚀 Running comprehensive test suite..."
docker-compose --profile test up --build test-runner

# Check exit code
if [ $? -eq 0 ]; then
    echo "✅ All tests passed!"
else
    echo "❌ Some tests failed. Check the output above for details."
    exit 1
fi

echo "🏁 Test suite completed."
