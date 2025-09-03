# Testing Guide for BDQ Email Report Service

This document provides comprehensive information about testing the BDQ Email Report Service, which has been refactored from a Unix socket server to a CLI approach.

## Overview

The refactor involved significant architectural changes:
- **Replaced Unix socket server** with simple command-line interface approach
- **Eliminated persistent JVM process management** in favor of stateless CLI execution per request
- **Simplified Docker build** with streamlined multi-stage Dockerfile
- **Added inline BDQ libraries** with local JVM CLI instead of external API calls

## Test Structure

```
tests/
├── __init__.py                 # Test package initialization
├── conftest.py                 # Shared fixtures and configuration
├── test_main.py                # Main FastAPI application tests
├── test_bdq_cli_service.py     # BDQ CLI service tests
├── test_tg2_parser.py          # TG2 parser service tests
├── test_csv_service.py         # CSV processing service tests
├── test_email_service.py       # Email service tests
└── test_llm_service.py         # LLM service tests
```

## Test Categories

### 1. Unit Tests
- **BDQ CLI Service**: Tests the refactored CLI execution logic
- **TG2 Parser**: Tests CSV parsing and test mapping functionality
- **CSV Service**: Tests CSV processing and core detection
- **Email Service**: Tests email processing and reply generation
- **LLM Service**: Tests intelligent summary generation

### 2. Integration Tests
- **Main Application**: Tests FastAPI endpoints and request flow
- **Service Integration**: Tests how services work together
- **Error Handling**: Tests error scenarios and fallback behavior

### 3. End-to-End Tests
- **Complete Email Flow**: Tests from email receipt to reply generation
- **CLI Execution**: Tests actual CLI execution with mock data

## Running Tests

### Prerequisites
```bash
# Install test dependencies
pip install -r requirements.txt
```

### Local Test Execution
```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_bdq_cli_service.py -v

# Run tests with coverage
python -m pytest tests/ --cov=app --cov-report=html

# Run tests in parallel
python -m pytest tests/ -n auto
```

### Using the Test Runner Script
```bash
# Run all tests
python run_tests.py

# Run with coverage
python run_tests.py --coverage

# Run only unit tests
python run_tests.py --type unit

# Run tests in parallel
python run_tests.py --parallel

# Install dependencies and run tests
python run_tests.py --install-deps
```

### Docker Test Execution
```bash
# Run all tests in Docker
docker-compose -f docker-compose.test.yml --profile test up test-runner

# Run tests with coverage
docker-compose -f docker-compose.test.yml --profile test up test-runner-coverage

# Run tests in parallel
docker-compose -f docker-compose.test.yml --profile test up test-runner-parallel

# Run only unit tests
docker-compose -f docker-compose.test.yml --profile test up test-runner-unit
```

## Test Coverage

### Core Functionality Tests

#### BDQ CLI Service (`test_bdq_cli_service.py`)
- **CLI Initialization**: Tests service setup and JAR validation
- **Test Discovery**: Tests loading and filtering of available BDQ tests
- **Test Execution**: Tests CLI execution via subprocess
- **Result Processing**: Tests CLI response parsing and conversion
- **Error Handling**: Tests CLI failures, timeouts, and edge cases
- **Connection Testing**: Tests CLI availability and health checks

#### TG2 Parser (`test_tg2_parser.py`)
- **CSV Parsing**: Tests TG2 test mapping file parsing
- **Library Detection**: Tests identification of BDQ libraries from source URLs
- **Method Derivation**: Tests automatic method name generation
- **Test Type Detection**: Tests validation/amendment/measure/issue classification
- **Field Parsing**: Tests acted-upon and consulted field extraction
- **Error Handling**: Tests malformed CSV and edge cases

#### CSV Service (`test_csv_service.py`)
- **Core Detection**: Tests occurrence vs. taxon core identification
- **Delimiter Support**: Tests comma, semicolon, tab, and pipe delimiters
- **Case Insensitivity**: Tests column name matching regardless of case
- **Result Generation**: Tests raw results and amended dataset CSV creation
- **Edge Cases**: Tests missing values, unicode, and large datasets

#### Email Service (`test_email_service.py`)
- **Attachment Extraction**: Tests CSV attachment identification and parsing
- **Email Generation**: Tests error and results reply creation
- **Summary Generation**: Tests email summary text generation
- **Error Handling**: Tests network failures and invalid data

#### LLM Service (`test_llm_service.py`)
- **API Integration**: Tests Google Gemini API integration
- **Fallback Behavior**: Tests graceful degradation when LLM is unavailable
- **Prompt Construction**: Tests intelligent summary prompt generation
- **Error Handling**: Tests API failures, timeouts, and rate limits

#### Main Application (`test_main.py`)
- **Endpoint Testing**: Tests health checks and email processing endpoints
- **Request Validation**: Tests payload normalization and error handling
- **Background Processing**: Tests async email processing
- **Exception Handling**: Tests global error handlers and Discord notifications

### Test Data and Fixtures

#### Sample Data
- **CSV Datasets**: Occurrence and taxon core examples
- **Email Payloads**: Realistic Apps Script email structures
- **Test Results**: BDQ test execution results
- **Error Scenarios**: Various failure conditions

#### Mock Services
- **External APIs**: Mocked Google Gemini and Gmail APIs
- **CLI Execution**: Mocked subprocess calls for testing
- **File Operations**: Mocked file I/O operations

## Testing the Refactor

### Key Areas to Validate

#### 1. CLI Architecture
```python
# Test that CLI service properly replaces socket server
def test_cli_architecture():
    service = BDQCLIService()
    assert service.cli_jar_path is not None
    assert service.test_mappings is not None
```

#### 2. Stateless Execution
```python
# Test that each request creates new CLI process
def test_stateless_execution():
    service = BDQCLIService()
    # Each execution should be independent
    result1 = service.execute_tests([test_request1])
    result2 = service.execute_tests([test_request2])
    assert result1 != result2
```

#### 3. Error Handling
```python
# Test graceful fallback when CLI fails
def test_cli_failure_handling():
    service = BDQCLIService()
    with patch('subprocess.run') as mock_run:
        mock_run.side_effect = Exception("CLI failed")
        # Should handle failure gracefully
        result = service.test_connection()
        assert result is False
```

#### 4. Performance
```python
# Test that CLI approach is efficient
def test_cli_performance():
    service = BDQCLIService()
    start_time = time.time()
    service.execute_tests([test_request])
    execution_time = time.time() - start_time
    assert execution_time < 5.0  # Should complete within 5 seconds
```

## Continuous Integration

### GitHub Actions Integration
```yaml
# .github/workflows/test.yml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: python -m pytest tests/ --cov=app --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

### Pre-commit Hooks
```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.4.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-added-large-files
  - repo: https://github.com/pycqa/pylint
    rev: v2.17.0
    hooks:
      - id: pylint
        args: [--rcfile=.pylintrc]
  - repo: local
    hooks:
      - id: pytest
        name: pytest
        entry: pytest
        language: system
        pass_filenames: false
        always_run: true
```

## Test Maintenance

### Adding New Tests
1. **Follow Naming Convention**: `test_<functionality>_<scenario>`
2. **Use Descriptive Names**: Clear test purpose and expected outcome
3. **Add to Appropriate File**: Group related tests together
4. **Update Documentation**: Document new test scenarios

### Updating Existing Tests
1. **Maintain Backward Compatibility**: Don't break existing functionality
2. **Update Fixtures**: Ensure test data remains relevant
3. **Review Coverage**: Verify new code paths are tested
4. **Performance Impact**: Ensure tests remain fast and efficient

### Test Data Management
1. **Use Fixtures**: Reusable test data and objects
2. **Mock External Dependencies**: Avoid network calls and file I/O
3. **Clean Up Resources**: Properly dispose of temporary files
4. **Version Control**: Include test data in repository

## Troubleshooting

### Common Test Issues

#### Import Errors
```bash
# Ensure PYTHONPATH is set correctly
export PYTHONPATH=/path/to/project:$PYTHONPATH

# Or run from project root
cd /path/to/project
python -m pytest tests/
```

#### Missing Dependencies
```bash
# Install test dependencies
pip install -r requirements.txt

# Or use the test runner
python run_tests.py --install-deps
```

#### Docker Issues
```bash
# Rebuild test container
docker-compose -f docker-compose.test.yml build

# Check container logs
docker-compose -f docker-compose.test.yml logs test-runner
```

#### Test Failures
```bash
# Run with verbose output
python -m pytest tests/ -v -s

# Run specific failing test
python -m pytest tests/test_specific.py::test_function -v -s

# Check test coverage
python -m pytest tests/ --cov=app --cov-report=term-missing
```

## Performance Considerations

### Test Execution Time
- **Unit Tests**: Should complete in < 1 second each
- **Integration Tests**: Should complete in < 5 seconds each
- **End-to-End Tests**: Should complete in < 30 seconds each

### Resource Usage
- **Memory**: Tests should use < 100MB RAM
- **CPU**: Tests should not consume excessive CPU
- **Disk**: Tests should clean up temporary files

### Parallel Execution
- **Unit Tests**: Can run in parallel safely
- **Integration Tests**: May require sequential execution
- **Resource Tests**: Should run in isolation

## Best Practices

### Test Design
1. **Arrange-Act-Assert**: Clear test structure
2. **Single Responsibility**: Each test validates one behavior
3. **Descriptive Names**: Test names explain what is being tested
4. **Minimal Dependencies**: Tests should be independent

### Test Data
1. **Realistic Data**: Use data that resembles production
2. **Edge Cases**: Test boundary conditions and error scenarios
3. **Consistent Format**: Maintain consistent data structure
4. **Minimal Size**: Use smallest dataset that tests functionality

### Mocking Strategy
1. **External APIs**: Mock all external service calls
2. **File I/O**: Mock file operations for unit tests
3. **Time-dependent**: Mock time-sensitive operations
4. **Random Data**: Mock random number generation

### Error Testing
1. **Expected Errors**: Test known error conditions
2. **Unexpected Errors**: Test unexpected failure scenarios
3. **Recovery**: Test error recovery and fallback behavior
4. **Logging**: Verify proper error logging and reporting

## Conclusion

This comprehensive test suite ensures that the refactored BDQ Email Report Service maintains all functionality while providing the improved architecture described in the README. The tests cover:

- **Architecture Changes**: CLI approach vs. socket server
- **Functionality**: All BDQ test execution and email processing
- **Error Handling**: Graceful degradation and fallback behavior
- **Performance**: Efficient CLI execution and resource management
- **Integration**: Service coordination and data flow

Regular test execution helps maintain code quality and ensures the refactor continues to work correctly as the codebase evolves.
