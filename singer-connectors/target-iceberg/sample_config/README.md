# Target-Iceberg Sample Configurations

This directory contains example configuration files for the PipelineWise target-iceberg connector.

## Directory Structure

```
sample_config/
├── README.md                               # This file
├── SNOWFLAKE_SETUP.md                      # Snowflake integration guide
│
├── Singer Target Configurations (JSON)
│   ├── config.json                         # Basic configuration
│   ├── config_with_snowflake.json          # With Snowflake integration
│   ├── config_with_partitioning.json       # With table partitioning
│   └── config_with_iam_role.json          # Using IAM role credentials
│
└── pipelinewise/                           # PipelineWise YAML configs
    ├── target_iceberg.yml                  # Basic target definition
    ├── target_iceberg_with_snowflake.yml   # Target with Snowflake
    ├── tap_mysql_to_iceberg.yml            # MySQL to Iceberg pipeline
    └── tap_postgres_to_iceberg.yml         # PostgreSQL to Iceberg pipeline
```

## Quick Start

### 1. Singer Target (Standalone)

For using target-iceberg as a standalone Singer target:

```bash
# Copy and customize a config file
cp config.json my_config.json
# Edit my_config.json with your credentials

# Run with a tap
tap-mysql --config tap_config.json | target-iceberg --config my_config.json
```

### 2. PipelineWise Integration

For using target-iceberg with PipelineWise:

```bash
# Create your PipelineWise configuration directory
mkdir -p ~/.pipelinewise/my_project

# Copy target configuration
cp pipelinewise/target_iceberg.yml ~/.pipelinewise/my_project/

# Copy a tap configuration
cp pipelinewise/tap_mysql_to_iceberg.yml ~/.pipelinewise/my_project/

# Edit configurations with your credentials
vim ~/.pipelinewise/my_project/target_iceberg.yml
vim ~/.pipelinewise/my_project/tap_mysql_to_iceberg.yml

# Import into PipelineWise
pipelinewise import --dir ~/.pipelinewise/my_project

# Run the pipeline
pipelinewise run_tap --tap mysql_to_iceberg --target iceberg_s3
```

## Configuration Files

### Basic Configuration (`config.json`)

Minimal configuration for writing Iceberg tables to S3 with AWS Glue Catalog:

- AWS credentials
- S3 bucket and prefix
- Glue Catalog ID
- Basic Parquet settings

**Use when:** You want a simple S3 data lake with Iceberg format.

### With Snowflake (`config_with_snowflake.json`)

Extends basic configuration with Snowflake external table creation:

- All basic configuration options
- Snowflake connection details
- External volume and catalog integration names

**Use when:** You want to query Iceberg data from Snowflake without loading it.

### With Partitioning (`config_with_partitioning.json`)

Adds table partitioning for better query performance:

- Partition columns configuration
- Ideal for time-series or categorical data

**Use when:** You have large tables and want to optimize query performance by partitioning data.

### With IAM Role (`config_with_iam_role.json`)

Uses IAM role instead of access keys:

- No explicit AWS credentials
- Relies on EC2 instance profile or ECS task role

**Use when:** Running on AWS infrastructure (EC2, ECS, Lambda) with IAM roles.

## PipelineWise YAML Configurations

### Target Definitions

#### `target_iceberg.yml`
Basic target configuration for PipelineWise. Reference this in your tap configurations using the target ID.

#### `target_iceberg_with_snowflake.yml`
Target with Snowflake integration enabled. Automatically creates external Iceberg tables in Snowflake.

### Full Pipeline Examples

#### `tap_mysql_to_iceberg.yml`
Complete MySQL to Iceberg pipeline showing:
- Log-based (CDC) replication
- Incremental replication
- Full table replication
- Table partitioning
- Data transformations (masking)

#### `tap_postgres_to_iceberg.yml`
Complete PostgreSQL to Iceberg pipeline showing:
- WAL-based (CDC) replication
- Time-based partitioning
- PII data masking
- Reference data sync

## Configuration Parameters

### Required Parameters

| Parameter | Description |
|-----------|-------------|
| `aws_region` | AWS region for S3 and Glue |
| `s3_bucket` | S3 bucket for Iceberg data files |
| `glue_catalog_id` | AWS Glue Catalog ID (usually AWS account ID) |
| `default_target_schema` | Default database/namespace |

### AWS Authentication

**Option 1: Access Keys** (for development)
```json
{
  "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
  "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
}
```

**Option 2: IAM Role** (recommended for production)
```json
{
  "aws_region": "us-east-1"
  // No aws_access_key_id - uses IAM role
}
```

**Option 3: Session Token** (for temporary credentials)
```json
{
  "aws_access_key_id": "...",
  "aws_secret_access_key": "...",
  "aws_session_token": "..."
}
```

### Performance Tuning

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batch_size_rows` | 100000 | Records per batch write |
| `parquet_row_group_size` | 100000 | Rows per Parquet row group |
| `compression_method` | snappy | Parquet compression (none/snappy/gzip/zstd) |
| `parallelism` | 0 | Parallel workers (0=auto) |
| `max_parallelism` | 16 | Max parallel workers |

### Partitioning

```json
{
  "partition_columns": ["year", "month", "day"]
}
```

Partitioning improves query performance by organizing data into directories. Common patterns:
- **Time-based**: `["year", "month"]`, `["date"]`
- **Categorical**: `["region"]`, `["category"]`
- **Hybrid**: `["region", "year", "month"]`

## AWS Permissions Required

### S3 Permissions
```json
{
  "Effect": "Allow",
  "Action": [
    "s3:PutObject",
    "s3:GetObject",
    "s3:DeleteObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::my-data-lake-bucket/*",
    "arn:aws:s3:::my-data-lake-bucket"
  ]
}
```

### Glue Permissions
```json
{
  "Effect": "Allow",
  "Action": [
    "glue:CreateDatabase",
    "glue:GetDatabase",
    "glue:CreateTable",
    "glue:UpdateTable",
    "glue:GetTable",
    "glue:GetTables"
  ],
  "Resource": [
    "arn:aws:glue:us-east-1:123456789012:catalog",
    "arn:aws:glue:us-east-1:123456789012:database/*",
    "arn:aws:glue:us-east-1:123456789012:table/*/*"
  ]
}
```

## Snowflake Setup

For detailed instructions on setting up Snowflake external Iceberg tables, see [SNOWFLAKE_SETUP.md](SNOWFLAKE_SETUP.md).

Quick checklist:
- [ ] Create Snowflake External Volume for S3
- [ ] Configure AWS IAM role with trust policy
- [ ] Create Snowflake Catalog Integration for Glue
- [ ] Enable `snowflake_integration` in target config
- [ ] Run PipelineWise pipeline
- [ ] Query external tables in Snowflake

## Copy-on-Write Format

All Iceberg tables created by this target use **Copy-on-Write (CoW)** format with these properties:

- `write.format.default=parquet` - Parquet file format
- `write.delete.mode=copy-on-write` - Rewrites files on updates/deletes
- `format-version=2` - Iceberg v2 format

This ensures optimal compatibility with Snowflake external tables, as Snowflake can directly read CoW tables without additional processing.

## Testing Your Configuration

### 1. Test AWS Credentials

```bash
# Using AWS CLI
aws s3 ls s3://my-data-lake-bucket/
aws glue get-databases --catalog-id 123456789012
```

### 2. Validate JSON Configuration

```bash
# Check JSON syntax
jq . config.json

# Validate with target-iceberg (if installed)
target-iceberg --config config.json --validate
```

### 3. Test PipelineWise Configuration

```bash
# Validate YAML syntax
pipelinewise validate --dir ~/.pipelinewise/my_project

# Test tap connection
pipelinewise test_tap_connection --tap mysql_to_iceberg
```

## Troubleshooting

### "Access Denied" S3 Errors
- Verify AWS credentials are correct
- Check IAM permissions include required S3 actions
- Ensure S3 bucket name and region are correct

### "Catalog not found" Glue Errors
- Verify `glue_catalog_id` matches your AWS account ID
- Check IAM permissions include Glue actions
- Ensure AWS region is correct

### Snowflake Connection Failures
- Verify Snowflake account identifier format
- Check user has required permissions
- Ensure external volume and catalog integration exist
- Review [SNOWFLAKE_SETUP.md](SNOWFLAKE_SETUP.md) for detailed setup

### Performance Issues
- Increase `batch_size_rows` for larger batches
- Adjust `parquet_row_group_size` based on query patterns
- Consider using `parallelism` for faster writes
- Add partitioning for large tables

## Support

For issues or questions:
- Check the main [README.md](../README.md)
- Review PipelineWise documentation
- Report issues on GitHub

## Additional Examples

Need more examples? Check:
- PipelineWise [dev-project](../../../dev-project/) for full working examples
- Singer specifications at [singer.io](https://www.singer.io/)
- Apache Iceberg docs at [iceberg.apache.org](https://iceberg.apache.org/)
