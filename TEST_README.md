# Test Documentation

## Overview
This directory contains comprehensive test cases for the PostgreSQL-based GCS Synchronizer application.

## Test Structure

### 1. Unit Tests (`test_synchronizer.py`)
- Uses Python's built-in `unittest` framework
- Tests individual functions and methods in isolation
- Includes mocking of external dependencies
- **Coverage:**
  - PostgreSQL connection and queries
  - GCS client operations
  - File upload functionality
  - Local file search
  - Document filtering and processing
  - Report generation

### 2. Pytest Tests (`test_pytest_cases.py`)
- Uses `pytest` framework with better fixtures and organization
- More modern testing approach with parametrized tests
- **Coverage:**
  - PostgreSQL integration
  - File handling workflows
  - Document processing pipeline
  - Error handling scenarios
  - Main workflow integration

### 3. Integration Tests (`test_integration.py`)
- Tests actual connection to PostgreSQL database
- Validates view structure and data access
- **Note:** Requires actual PostgreSQL instance running

### 4. Test Configuration
- `conftest.py`: Pytest fixtures and configuration
- `pytest.ini`: Pytest settings and markers
- `requirements-test.txt`: Test-specific dependencies

## Running Tests

### Quick Test Run
```bash
# Run all unit tests
python test_synchronizer.py

# Run all pytest tests
pytest test_pytest_cases.py -v
```

### Comprehensive Test Run
```bash
# Use the test runner script
./run_tests.sh
```

### Integration Test
```bash
# Only run when PostgreSQL is available
python test_integration.py
```

### Test with Coverage
```bash
pytest test_pytest_cases.py --cov=clickhouse_to_gcs --cov=config --cov-report=html
```

## Test Dependencies
Install test dependencies:
```bash
pip install -r requirements-test.txt
```

## Test Cases Covered

### PostgreSQL Functions
- [x] `get_postgres_connection()` - Connection establishment and error handling
- [x] `query_postgres()` - Query execution with and without parameters
- [x] `get_documents_from_postgres()` - Document retrieval from view

### GCS Functions
- [x] `get_gcs_client()` - Client initialization and authentication
- [x] `upload_to_gcs()` - File upload success and error scenarios

### File Operations
- [x] `find_local_file()` - File search in directory structure
- [x] File existence validation
- [x] Path handling

### Document Processing
- [x] `filter_unprocessed_documents()` - Filtering based on cache
- [x] `load_processed_cache()` - Cache loading from reports
- [x] `save_processing_report()` - Report generation and saving

### Main Workflow
- [x] `main()` - Complete workflow integration
- [x] Error handling and cleanup
- [x] Resource management (connection closing)

### Configuration
- [x] PostgreSQL configuration values
- [x] Environment variable handling

## Test Data
Tests use mock data and temporary files to avoid dependencies on external resources:
- Sample document structures
- Temporary directories for file operations
- Mock PostgreSQL responses
- Mock GCS client behavior

## Error Scenarios Tested
- PostgreSQL connection failures
- GCS authentication errors
- File not found scenarios
- Upload failures
- Database query errors
- Missing configuration values

## Coverage Goals
- **Target:** 90%+ code coverage
- **Current focus:** Core functionality and error paths
- **Excluded:** Logging and minor utility functions

## Continuous Integration
These tests are designed to run in CI/CD environments:
- No external database dependencies for unit tests
- Mock all external services
- Fast execution time
- Clear pass/fail indicators

## Adding New Tests
When adding new functionality:
1. Add unit tests in `test_synchronizer.py` or `test_pytest_cases.py`
2. Mock external dependencies
3. Test both success and failure scenarios
4. Update this documentation
5. Ensure tests pass before committing

## Test Environment Setup
For local testing:
```bash
# Create virtual environment
python -m venv test_env
source test_env/bin/activate  # On Windows: test_env\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-test.txt

# Run tests
./run_tests.sh
```
