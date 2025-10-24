# Target-Iceberg Tests

This directory contains tests for the target-iceberg connector.

## Test Structure

```
tests/
├── __init__.py
├── README.md                                    # This file
├── fixtures.py                                  # Test data and fixtures
├── unit/                                        # Unit tests (fast, no external dependencies)
│   ├── __init__.py
│   └── test_db_sync.py                         # Tests for db_sync module
└── integration/                                 # Integration tests (require AWS/Snowflake)
    ├── __init__.py
    └── test_target_iceberg_integration.py      # End-to-end integration tests
```

## Running Tests

### Unit Tests Only (Fast)

Unit tests use mocks and don't require external services:

```bash
# From target-iceberg directory
cd singer-connectors/target-iceberg

# Run all unit tests
pytest tests/unit/

# Run specific test file
pytest tests/unit/test_db_sync.py

# Run with coverage
pytest tests/unit/ --cov=target_iceberg --cov-report=html
```

### Integration Tests (Require AWS/Snowflake)

Integration tests connect to real AWS services and require credentials:

```bash
# Set environment variables
export ICEBERG_INTEGRATION_TESTS=1
export AWS_REGION=us-east-1
export ICEBERG_TEST_BUCKET=your-test-bucket
export AWS_ACCOUNT_ID=123456789012

# Optional: For Snowflake integration tests
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
export SNOWFLAKE_USER=test_user
export SNOWFLAKE_PASSWORD=test_password
export SNOWFLAKE_DATABASE=TEST_DB
export SNOWFLAKE_WAREHOUSE=TEST_WH
export SNOWFLAKE_EXTERNAL_VOLUME=test_external_volume
export SNOWFLAKE_CATALOG_INTEGRATION=test_catalog_integration

# Run integration tests
pytest tests/integration/
```

### Run All Tests

```bash
# Run everything
export ICEBERG_INTEGRATION_TESTS=1
pytest tests/

# With verbose output
pytest tests/ -v

# With coverage report
pytest tests/ --cov=target_iceberg --cov-report=term-missing
```

## Test Categories

### Unit Tests (`tests/unit/`)

Fast tests that use mocking and don't require external services:

- **`test_db_sync.py`**: Tests for DbSync class
  - Catalog initialization
  - Singer to Iceberg type conversions
  - Schema creation
  - Record processing and batching
  - Snowflake integration initialization
  - Copy-on-Write property validation

### Integration Tests (`tests/integration/`)

Slower tests that connect to real AWS and Snowflake services:

- **`test_target_iceberg_integration.py`**: End-to-end integration tests
  - Creating tables in AWS Glue Catalog
  - Writing data to S3 in Iceberg format
  - Verifying Copy-on-Write properties
  - Creating Snowflake external tables (if enabled)

**Note:** Integration tests are skipped by default. Set `ICEBERG_INTEGRATION_TESTS=1` to enable.

## Fixtures (`fixtures.py`)

Common test data and helper functions:

- `get_test_config()` - Minimal test configuration
- `get_test_config_with_snowflake()` - Config with Snowflake integration
- `get_simple_singer_schema()` - Basic Singer schema
- `get_complex_singer_schema()` - Schema with various data types
- `get_test_records()` - Sample record data
- `get_singer_messages()` - Complete Singer message stream
- `get_fastsync_columns()` - FastSync column definitions
- `get_postgres_type_mappings()` - PostgreSQL type mappings
- `get_mysql_type_mappings()` - MySQL type mappings

## Writing New Tests

### Unit Test Template

```python
import unittest
from unittest.mock import Mock, patch
from target_iceberg.db_sync import DbSync
from tests.fixtures import get_test_config

class TestMyFeature(unittest.TestCase):
    def setUp(self):
        self.config = get_test_config()

    @patch('target_iceberg.db_sync.load_catalog')
    def test_my_feature(self, mock_load_catalog):
        # Arrange
        db_sync = DbSync(self.config)

        # Act
        result = db_sync.some_method()

        # Assert
        self.assertEqual(result, expected_value)
```

### Integration Test Template

```python
import unittest
import os
from tests.fixtures import get_test_config

INTEGRATION_TESTS_ENABLED = os.getenv('ICEBERG_INTEGRATION_TESTS', '0') == '1'

@unittest.skipUnless(INTEGRATION_TESTS_ENABLED, 'Integration tests disabled')
class TestMyIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = get_test_config()
        # Override with real credentials from environment
        cls.config['s3_bucket'] = os.getenv('ICEBERG_TEST_BUCKET')

    def test_real_operation(self):
        # Test with real AWS services
        pass
```

## FastSync Tests

FastSync module tests are located in the main PipelineWise tests directory:

```
tests/units/fastsync/
├── test_postgres_to_iceberg.py           # PostgreSQL type mapping tests
├── test_mysql_to_iceberg.py              # MySQL type mapping tests
└── commons/
    └── test_fastsync_target_iceberg.py   # FastSync target tests
```

Run FastSync tests:

```bash
# From PipelineWise root directory
pytest tests/units/fastsync/test_postgres_to_iceberg.py
pytest tests/units/fastsync/test_mysql_to_iceberg.py
pytest tests/units/fastsync/commons/test_fastsync_target_iceberg.py

# Run all fastsync tests
pytest tests/units/fastsync/
```

## Continuous Integration

Tests are run automatically in CI/CD:

- **Unit tests**: Run on every commit
- **Integration tests**: Run on PRs to main branch (with proper credentials)

### GitHub Actions Example

```yaml
name: Test Target-Iceberg

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: |
          cd singer-connectors/target-iceberg
          pip install -e '.[test]'
      - name: Run unit tests
        run: pytest tests/unit/ -v

  integration-tests:
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request'
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: |
          cd singer-connectors/target-iceberg
          pip install -e '.[test]'
      - name: Run integration tests
        env:
          ICEBERG_INTEGRATION_TESTS: 1
          AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          ICEBERG_TEST_BUCKET: ${{ secrets.ICEBERG_TEST_BUCKET }}
        run: pytest tests/integration/ -v
```

## Test Coverage

Target minimum code coverage: **80%**

Generate coverage report:

```bash
# HTML report
pytest tests/ --cov=target_iceberg --cov-report=html
open htmlcov/index.html

# Terminal report with missing lines
pytest tests/ --cov=target_iceberg --cov-report=term-missing

# XML report (for CI tools)
pytest tests/ --cov=target_iceberg --cov-report=xml
```

## Debugging Tests

### Run with verbose output

```bash
pytest tests/ -v
```

### Run specific test

```bash
pytest tests/unit/test_db_sync.py::TestDbSync::test_singer_to_iceberg_type_conversions -v
```

### Show print statements

```bash
pytest tests/ -s
```

### Drop into debugger on failure

```bash
pytest tests/ --pdb
```

### Run tests in parallel (faster)

```bash
pytest tests/ -n auto
```

## Best Practices

1. **Mock external services** in unit tests (AWS, Snowflake)
2. **Use fixtures** from `fixtures.py` for common test data
3. **Name tests descriptively**: `test_<feature>_<scenario>_<expected_result>`
4. **Test edge cases**: null values, empty strings, large datasets
5. **Test error conditions**: invalid configs, network failures
6. **Keep tests independent**: Don't rely on test execution order
7. **Clean up resources**: Use `tearDown()` or context managers
8. **Document complex tests**: Add docstrings explaining test purpose

## Troubleshooting

### Import Errors

```bash
# Ensure package is installed in development mode
pip install -e .
```

### AWS Credential Errors

```bash
# Check AWS credentials
aws sts get-caller-identity

# Set credentials explicitly
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
```

### Snowflake Connection Errors

```bash
# Test Snowflake connection
python -c "import snowflake.connector; print(snowflake.connector.connect(account='...', user='...', password='...'))"
```

### S3 Permission Errors

Ensure your IAM user/role has these permissions:
- `s3:PutObject`
- `s3:GetObject`
- `s3:ListBucket`
- `glue:CreateDatabase`
- `glue:CreateTable`
- `glue:GetTable`

## Contributing

When adding new features:

1. Write unit tests first (TDD approach)
2. Ensure all existing tests pass
3. Add integration tests for new functionality
4. Update this README if adding new test categories
5. Maintain or improve code coverage

## Resources

- **pytest documentation**: https://docs.pytest.org/
- **unittest.mock**: https://docs.python.org/3/library/unittest.mock.html
- **PyIceberg**: https://py.iceberg.apache.org/
- **Singer spec**: https://github.com/singer-io/getting-started
