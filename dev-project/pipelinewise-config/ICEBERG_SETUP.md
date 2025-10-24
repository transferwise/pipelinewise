# Iceberg Target Configuration for Dev Project

This guide explains how to use the PostgreSQL to Iceberg pipeline configuration in the dev-project.

## Overview

The `tap_postgres_to_iceberg.yml` and `target_iceberg.yml` files demonstrate how to:
- Replicate data from PostgreSQL to Apache Iceberg tables on S3
- Use AWS Glue Catalog for metadata management
- Automatically create Snowflake external Iceberg tables
- Write data in Copy-on-Write (CoW) format for Snowflake compatibility

## Files

- **`target_iceberg.yml`** - Target configuration for Iceberg on S3 with Snowflake integration
- **`tap_postgres_to_iceberg.yml`** - Tap configuration for PostgreSQL source

## Prerequisites

### 1. AWS Setup

Before using this configuration, you need:

- **S3 Bucket**: Create an S3 bucket for Iceberg data
  ```bash
  aws s3 mb s3://my-data-lake-bucket
  ```

- **AWS Glue Catalog**: Your AWS account ID will be used as the Glue Catalog ID

- **IAM Permissions**: Ensure your AWS credentials have permissions for:
  - S3: `PutObject`, `GetObject`, `DeleteObject`, `ListBucket`
  - Glue: `CreateDatabase`, `GetDatabase`, `CreateTable`, `UpdateTable`, `GetTable`

### 2. Snowflake Setup (Optional)

If you want to enable Snowflake integration:

1. **Create External Volume** in Snowflake:
   ```sql
   CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
     STORAGE_LOCATIONS = (
       (
         NAME = 'my-s3-location'
         STORAGE_PROVIDER = 'S3'
         STORAGE_BASE_URL = 's3://my-data-lake-bucket/iceberg-warehouse/'
         STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-role'
       )
     );
   ```

2. **Create Catalog Integration** for AWS Glue:
   ```sql
   CREATE CATALOG INTEGRATION glue_catalog_integration
     CATALOG_SOURCE = GLUE
     CATALOG_NAMESPACE = '123456789012'
     TABLE_FORMAT = ICEBERG
     GLUE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-role'
     GLUE_CATALOG_ID = '123456789012'
     GLUE_REGION = 'us-east-1'
     ENABLED = TRUE;
   ```

3. **Create Snowflake Database**:
   ```sql
   CREATE DATABASE IF NOT EXISTS ANALYTICS_DB;
   ```

For detailed Snowflake setup instructions, see:
- `../../singer-connectors/target-iceberg/sample_config/SNOWFLAKE_SETUP.md`

## Configuration

### 1. Update Target Configuration

Edit `target_iceberg.yml` and update these values:

```yaml
db_conn:
  # AWS Configuration
  aws_region: "us-east-1"                    # Your AWS region
  s3_bucket: "my-data-lake-bucket"           # Your S3 bucket name
  glue_catalog_id: "123456789012"            # Your AWS account ID

  # Snowflake Integration (optional)
  snowflake_integration:
    enabled: true                            # Set to false to disable
    account: "xy12345.us-east-1"             # Your Snowflake account
    user: "ICEBERG_USER"                     # Your Snowflake user
    password: "your_password"                # Your Snowflake password
    database: "ANALYTICS_DB"                 # Your Snowflake database
    warehouse: "COMPUTE_WH"                  # Your Snowflake warehouse
    external_volume: "iceberg_external_volume"
    catalog_integration: "glue_catalog_integration"
```

### 2. Update Tap Configuration (Optional)

The `tap_postgres_to_iceberg.yml` file is pre-configured to work with the dev-project PostgreSQL source. You can customize:

- **Tables to replicate**: Add or remove tables in the `schemas` section
- **Replication methods**: Choose between `INCREMENTAL`, `FULL_TABLE`, or `LOG_BASED`
- **Partitioning**: Add `partition_columns` to tables for better query performance
- **Transformations**: Add data masking or transformations

## Usage

### 1. Import Configuration

```bash
# Navigate to PipelineWise directory
cd /path/to/pipelinewise

# Activate PipelineWise environment
source .virtualenvs/pipelinewise/bin/activate
export PIPELINEWISE_HOME=$(pwd)

# Import the dev-project configuration
pipelinewise import --dir dev-project/pipelinewise-config
```

### 2. Verify Configuration

```bash
# Check status
pipelinewise status

# You should see:
# - Target: iceberg_s3_snowflake
# - Tap: postgres_to_iceberg
```

### 3. Test Connection

```bash
# Test PostgreSQL source connection
pipelinewise test_tap_connection --tap postgres_to_iceberg
```

### 4. Discover Schema

```bash
# Discover PostgreSQL schema
pipelinewise discover_tap --tap postgres_to_iceberg
```

### 5. Run Pipeline

```bash
# Run the full pipeline
pipelinewise run_tap --tap postgres_to_iceberg --target iceberg_s3_snowflake

# Or sync specific tables
pipelinewise sync_tables --tap postgres_to_iceberg --tables city,country
```

### 6. Monitor Progress

```bash
# Check logs
tail -f ~/.pipelinewise/iceberg_s3_snowflake/postgres_to_iceberg/log/current.log

# View state
cat ~/.pipelinewise/iceberg_s3_snowflake/postgres_to_iceberg/state.json
```

## Verify Data

### In AWS

```bash
# Check S3 for data files
aws s3 ls s3://my-data-lake-bucket/iceberg-warehouse/ --recursive

# Check Glue Catalog
aws glue get-databases --catalog-id 123456789012

# List tables in analytics database
aws glue get-tables --database-name analytics --catalog-id 123456789012

# Get specific table metadata
aws glue get-table \
  --database-name analytics \
  --name postgres_analytics_city \
  --catalog-id 123456789012
```

### In AWS Athena

Query your Iceberg tables using Athena:

```sql
-- List databases
SHOW DATABASES;

-- List tables in analytics schema
SHOW TABLES IN analytics;

-- Query data
SELECT * FROM analytics.postgres_analytics_city LIMIT 10;
SELECT COUNT(*) FROM analytics.postgres_analytics_country;
```

### In Snowflake

If Snowflake integration is enabled:

```sql
-- Switch to analytics database
USE DATABASE ANALYTICS_DB;

-- List schemas
SHOW SCHEMAS;

-- List tables in postgres_analytics schema
SHOW ICEBERG TABLES IN SCHEMA postgres_analytics;

-- Query data
SELECT * FROM postgres_analytics.city LIMIT 10;
SELECT * FROM postgres_analytics.country;

-- Check table properties (verify CoW format)
DESCRIBE ICEBERG TABLE postgres_analytics.city;
```

## Copy-on-Write Format

All Iceberg tables are automatically created with Copy-on-Write (CoW) format:

```
write.format.default = parquet
write.delete.mode = copy-on-write
format-version = 2
```

This ensures optimal compatibility with Snowflake external tables.

## Partitioning

To enable partitioning for better query performance, add `partition_columns` to tables:

```yaml
tables:
  - table_name: "events"
    replication_method: "INCREMENTAL"
    replication_key: "updated_at"
    partition_columns:
      - "year"
      - "month"
```

Common partitioning strategies:
- **Time-series data**: Partition by date columns (`year`, `month`, `day`)
- **Categorical data**: Partition by region, category, or status
- **Large tables**: Combine time and categorical partitions

## Troubleshooting

### Pipeline Fails with "Access Denied"

Check AWS credentials and permissions:
```bash
aws sts get-caller-identity
aws s3 ls s3://my-data-lake-bucket/
```

### Tables Not Created in Glue

Check PipelineWise logs:
```bash
cat ~/.pipelinewise/iceberg_s3_snowflake/postgres_to_iceberg/log/current.log
```

Verify Glue permissions in IAM policy.

### Snowflake External Tables Not Appearing

Verify Snowflake setup:
```sql
SHOW EXTERNAL VOLUMES;
SHOW INTEGRATIONS;
```

Check Snowflake credentials in `target_iceberg.yml`.

### Performance Issues

- Increase `batch_size_rows` in tap configuration
- Add partitioning to large tables
- Adjust `parquet_row_group_size` in target configuration

## Development Workflow

### Without Snowflake Integration

If you want to test without Snowflake:

1. Edit `target_iceberg.yml`
2. Set `snowflake_integration.enabled: false`
3. Re-import configuration: `pipelinewise import --dir dev-project/pipelinewise-config`
4. Run pipeline

You can still query data via AWS Athena.

### Using Docker Dev Environment

The dev-project includes a Docker environment with PostgreSQL source:

```bash
# Start Docker environment
cd dev-project
docker compose up -d

# Shell into PipelineWise container
docker exec -it pipelinewise_dev bash

# Inside container, import and run
pipelinewise import --dir /opt/pipelinewise/dev-project/pipelinewise-config
pipelinewise run_tap --tap postgres_to_iceberg --target iceberg_s3_snowflake
```

## Best Practices

1. **Use IAM Roles**: In production, use IAM roles instead of access keys
2. **Encrypt Credentials**: Use PipelineWise vault encryption for passwords
3. **Partition Large Tables**: Always partition tables with millions of rows
4. **Monitor Costs**: Track S3, Glue, and Snowflake costs
5. **Test Incrementally**: Start with small tables before syncing large datasets
6. **Set Up Alerts**: Configure Slack/VictorOps alerts in `config.yml`

## Additional Resources

- **Target Iceberg README**: `../../singer-connectors/target-iceberg/README.md`
- **Sample Configurations**: `../../singer-connectors/target-iceberg/sample_config/`
- **Snowflake Setup Guide**: `../../singer-connectors/target-iceberg/sample_config/SNOWFLAKE_SETUP.md`
- **PipelineWise Docs**: https://transferwise.github.io/pipelinewise/
- **Apache Iceberg**: https://iceberg.apache.org/
