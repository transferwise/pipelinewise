# Snowflake External Iceberg Tables Setup

This guide explains how to set up Snowflake to read Iceberg tables created by PipelineWise target-iceberg.

## Overview

The target-iceberg connector can automatically create external Iceberg tables in Snowflake that reference the Iceberg tables stored in S3 and cataloged in AWS Glue. This allows you to query your data lake directly from Snowflake without loading data into Snowflake tables.

## Prerequisites

1. **Snowflake Account** with appropriate permissions
2. **AWS S3 Bucket** containing your Iceberg data
3. **AWS Glue Catalog** managing your Iceberg table metadata
4. **Snowflake Enterprise Edition or higher** (required for Iceberg support)

## Step 1: Create External Volume in Snowflake

An External Volume provides Snowflake access to your S3 bucket.

```sql
-- Create storage integration for S3 access
CREATE STORAGE INTEGRATION iceberg_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-iceberg-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://my-data-lake-bucket/iceberg-warehouse/');

-- Grant usage on storage integration
GRANT USAGE ON INTEGRATION iceberg_s3_integration TO ROLE ACCOUNTADMIN;

-- Describe integration to get AWS IAM user for trust relationship
DESC STORAGE INTEGRATION iceberg_s3_integration;
-- Note the STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID values

-- Create external volume
CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
  STORAGE_LOCATIONS = (
    (
      NAME = 'my-s3-location'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://my-data-lake-bucket/iceberg-warehouse/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-iceberg-role'
    )
  );

-- Grant usage on external volume
GRANT USAGE ON EXTERNAL VOLUME iceberg_external_volume TO ROLE ACCOUNTADMIN;
```

## Step 2: Configure AWS IAM Role

Create an IAM role that allows Snowflake to access your S3 bucket.

### Trust Policy for IAM Role

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789012:user/abc1-b-EXAMPLE"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "EXAMPLE_EXTERNAL_ID"
        }
      }
    }
  ]
}
```

**Note:** Replace the Principal AWS ARN and ExternalId with values from `DESC STORAGE INTEGRATION` output.

### IAM Policy for S3 Access

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::my-data-lake-bucket/iceberg-warehouse/*",
        "arn:aws:s3:::my-data-lake-bucket"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetPartitions"
      ],
      "Resource": [
        "arn:aws:glue:us-east-1:123456789012:catalog",
        "arn:aws:glue:us-east-1:123456789012:database/*",
        "arn:aws:glue:us-east-1:123456789012:table/*/*"
      ]
    }
  ]
}
```

## Step 3: Create Catalog Integration

A Catalog Integration connects Snowflake to your AWS Glue Catalog.

```sql
-- Create catalog integration for AWS Glue
CREATE CATALOG INTEGRATION glue_catalog_integration
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = '123456789012'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-iceberg-role'
  GLUE_CATALOG_ID = '123456789012'
  GLUE_REGION = 'us-east-1'
  ENABLED = TRUE;

-- Grant usage on catalog integration
GRANT USAGE ON INTEGRATION glue_catalog_integration TO ROLE ACCOUNTADMIN;

-- Describe integration to verify configuration
DESC CATALOG INTEGRATION glue_catalog_integration;
```

## Step 4: Configure PipelineWise Target

Update your target-iceberg configuration to enable Snowflake integration:

```yaml
---
id: 'iceberg_s3_snowflake'
name: 'Iceberg Data Lake with Snowflake'
type: 'target-iceberg'

db_conn:
  aws_access_key_id: 'YOUR_AWS_ACCESS_KEY'
  aws_secret_access_key: 'YOUR_AWS_SECRET_KEY'
  aws_region: 'us-east-1'
  s3_bucket: 'my-data-lake-bucket'
  s3_key_prefix: 'iceberg-warehouse/'
  glue_catalog_id: '123456789012'
  default_target_schema: 'analytics'

  # Enable Snowflake integration
  snowflake_integration:
    enabled: true
    account: 'xy12345.us-east-1'
    user: 'ICEBERG_USER'
    password: 'your_secure_password'
    database: 'ANALYTICS_DB'
    warehouse: 'COMPUTE_WH'
    role: 'ACCOUNTADMIN'
    external_volume: 'iceberg_external_volume'
    catalog_integration: 'glue_catalog_integration'
```

## Step 5: Run PipelineWise

When you run PipelineWise with Snowflake integration enabled, it will:

1. Create Iceberg tables in AWS Glue Catalog
2. Write data to S3 in Copy-on-Write (CoW) format
3. Automatically create corresponding external Iceberg tables in Snowflake

```bash
# Import your PipelineWise configuration
pipelinewise import --dir /path/to/config

# Run the tap to sync data
pipelinewise run_tap --tap mysql_to_iceberg --target iceberg_s3_snowflake

# Check status
pipelinewise status
```

## Step 6: Query Data in Snowflake

Once the pipeline runs, you can query the Iceberg tables directly in Snowflake:

```sql
-- Switch to your database
USE DATABASE ANALYTICS_DB;

-- Query the external Iceberg table
SELECT *
FROM analytics.user_events
WHERE event_date >= CURRENT_DATE - 7
LIMIT 100;

-- Check table metadata
SHOW ICEBERG TABLES IN SCHEMA analytics;

-- View table properties (including CoW configuration)
DESCRIBE ICEBERG TABLE analytics.user_events;
```

## Copy-on-Write Format

All Iceberg tables created by target-iceberg use Copy-on-Write (CoW) format with these properties:

- `write.format.default=parquet` - Uses Parquet format
- `write.delete.mode=copy-on-write` - Rewrites files on updates/deletes
- `format-version=2` - Uses Iceberg v2 format

This ensures optimal compatibility with Snowflake external tables, as Snowflake can directly read CoW tables without additional merge operations.

## Troubleshooting

### Error: "External volume not found"

Verify the external volume exists and you have USAGE privileges:

```sql
SHOW EXTERNAL VOLUMES;
GRANT USAGE ON EXTERNAL VOLUME iceberg_external_volume TO ROLE ACCOUNTADMIN;
```

### Error: "Catalog integration not found"

Verify the catalog integration exists and is enabled:

```sql
SHOW INTEGRATIONS;
DESC CATALOG INTEGRATION glue_catalog_integration;
```

### Error: "Access Denied" when querying S3

Check your IAM role permissions and trust policy. Ensure the Snowflake IAM user has the correct permissions.

```bash
# Test S3 access using AWS CLI with the IAM role
aws s3 ls s3://my-data-lake-bucket/iceberg-warehouse/ \
  --profile your-profile
```

### Tables not appearing in Snowflake

Check if the target-iceberg successfully created tables in Glue:

```bash
# List Glue databases
aws glue get-databases --catalog-id 123456789012

# List tables in a database
aws glue get-tables --database-name analytics --catalog-id 123456789012
```

Verify PipelineWise logs for any errors during Snowflake external table creation.

## Best Practices

1. **Use dedicated IAM role**: Create a separate IAM role for Snowflake with minimal required permissions
2. **Partition large tables**: Use time-based or categorical partitioning for better query performance
3. **Monitor costs**: External table queries scan S3 data, which incurs both S3 and Snowflake compute costs
4. **Regular maintenance**: Periodically optimize Iceberg tables using compaction
5. **Security**: Use Snowflake's row-level security and masking policies for sensitive data

## Additional Resources

- [Snowflake Iceberg Tables Documentation](https://docs.snowflake.com/en/user-guide/tables-iceberg)
- [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
