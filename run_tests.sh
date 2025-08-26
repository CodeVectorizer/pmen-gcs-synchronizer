#!/bin/bash

# Test runner script for PostgreSQL GCS Synchronizer

echo "🧪 Running tests for PostgreSQL GCS Synchronizer"
echo "================================================"

# Check if virtual environment is activated
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "⚠️  Warning: No virtual environment detected. Consider activating one."
fi

# Install test dependencies
echo "📦 Installing test dependencies..."
pip install -r requirements-test.txt

echo ""
echo "🏃 Running unittest test suite..."
echo "--------------------------------"
python test_synchronizer.py

echo ""
echo "🏃 Running pytest test suite..."
echo "------------------------------"
pytest test_pytest_cases.py -v --tb=short

echo ""
echo "📊 Running tests with coverage..."
echo "--------------------------------"
pytest test_pytest_cases.py --cov=clickhouse_to_gcs --cov=config --cov-report=html --cov-report=term

echo ""
echo "✅ Test execution completed!"
echo "📄 Coverage report saved to htmlcov/index.html"
