# Target-Iceberg Quick Start Guide

Get up and running with target-iceberg in minutes.

## Installation

```bash
# Install from source
cd singer-connectors/target-iceberg
pip install .

# Or install in PipelineWise
cd /path/to/pipelinewise
make pipelinewise connectors -e pw_connector=target-iceberg
```

## Minimal Configuration

Create `config.json`:

```json
{
  "aws_region": "us-east-1",
  "s3_bucket": "my-data-lake-bucket",
  "glue_catalog_id": "123456789012",
  "default_target_schema": "analytics"
}
```

**Note:** Uses IAM role for credentials. For access keys, add `aws_access_key_id` and `aws_secret_access_key`.

## Run Standalone

```bash
# Stream data from a tap to target-iceberg
tap-mysql --config tap_config.json | target-iceberg --config config.json
```

## Run with PipelineWise

### 1. Create Configuration Files

**Target:** `target_iceberg.yml`
```yaml
---
id: 'iceberg_s3'
name: 'Iceberg Data Lake'
type: 'target-iceberg'
db_conn:
  aws_region: 'us-east-1'
  s3_bucket: 'my-data-lake-bucket'
  glue_catalog_id: '123456789012'
  default_target_schema: 'analytics'
```

**Tap:** `tap_mysql.yml`
```yaml
---
id: 'mysql_source'
name: 'MySQL Database'
type: 'tap-mysql'
target: 'iceberg_s3'
db_conn:
  host: 'mysql.example.com'
  port: 3306
  user: 'replication_user'
  password: 'password'
  dbname: 'production_db'
schemas:
  - source_schema: 'public'
    target_schema: 'mysql_prod'
    tables:
      - table_name: 'users'
        replication_method: 'INCREMENTAL'
        replication_key: 'updated_at'
```

### 2. Import and Run

```bash
# Import configuration
pipelinewise import --dir /path/to/config

# Run the pipeline
pipelinewise run_tap --tap mysql_source --target iceberg_s3

# Check status
pipelinewise status
```

## Add Snowflake Integration

Update `target_iceberg.yml`:

```yaml
db_conn:
  # ... existing config ...
  snowflake_integration:
    enabled: true
    account: 'xy12345.us-east-1'
    user: 'ICEBERG_USER'
    password: 'password'
    database: 'ANALYTICS_DB'
    external_volume: 'iceberg_external_volume'
    catalog_integration: 'glue_catalog_integration'
```

Then query from Snowflake:

```sql
USE DATABASE ANALYTICS_DB;
SELECT * FROM mysql_prod.users LIMIT 10;
```

## Add Table Partitioning

In your tap configuration:

```yaml
tables:
  - table_name: 'events'
    replication_method: 'LOG_BASED'
    partition_columns:
      - 'year'
      - 'month'
      - 'day'
```

## Common Commands

```bash
# Test tap connection
pipelinewise test_tap_connection --tap mysql_source

# Discover tap schema
pipelinewise discover_tap --tap mysql_source

# Run specific tables
pipelinewise sync_tables --tap mysql_source --tables users,orders

# Check logs
tail -f ~/.pipelinewise/iceberg_s3/mysql_source/log/current.log

# View state
cat ~/.pipelinewise/iceberg_s3/mysql_source/state.json
```

## Verify Data in AWS

```bash
# Check S3 files
aws s3 ls s3://my-data-lake-bucket/iceberg-warehouse/

# Check Glue catalog
aws glue get-databases --catalog-id 123456789012
aws glue get-tables --database-name analytics --catalog-id 123456789012

# View table metadata
aws glue get-table --database-name analytics --name users --catalog-id 123456789012
```

## Query with AWS Athena

```sql
-- Athena automatically sees Glue catalog tables
SELECT * FROM analytics.users LIMIT 10;
```

## Troubleshooting

### Can't write to S3
```bash
# Check AWS credentials
aws sts get-caller-identity

# Test S3 access
aws s3 ls s3://my-data-lake-bucket/
```

### Tables not appearing
```bash
# Check PipelineWise logs
cat ~/.pipelinewise/iceberg_s3/mysql_source/log/current.log

# Verify Glue catalog
aws glue get-tables --database-name analytics --catalog-id 123456789012
```

### Snowflake external tables not working
```sql
-- In Snowflake, verify setup
SHOW EXTERNAL VOLUMES;
SHOW INTEGRATIONS;
DESC ICEBERG TABLE analytics.users;
```

## Next Steps

- Review [sample_config/README.md](README.md) for detailed configuration options
- See [SNOWFLAKE_SETUP.md](SNOWFLAKE_SETUP.md) for complete Snowflake setup
- Check full pipeline examples in `sample_config/pipelinewise/`
- Explore partitioning strategies for your use case
- Set up monitoring and alerting for production pipelines

## Production Checklist

- [ ] Use IAM roles instead of access keys
- [ ] Enable encryption at rest (S3 bucket encryption)
- [ ] Enable encryption in transit (SSL/TLS)
- [ ] Set up CloudWatch alarms for pipeline failures
- [ ] Configure PipelineWise alerts (Slack/VictorOps)
- [ ] Implement data quality checks
- [ ] Document schema changes and migrations
- [ ] Set up backup and disaster recovery
- [ ] Monitor costs (S3, Glue, Snowflake)
- [ ] Review and optimize partition strategies

## Resources

- **PipelineWise Docs**: [pipelinewise.transferwise.com](https://transferwise.github.io/pipelinewise/)
- **Singer Spec**: [singer.io](https://www.singer.io/)
- **Apache Iceberg**: [iceberg.apache.org](https://iceberg.apache.org/)
- **AWS Glue**: [docs.aws.amazon.com/glue](https://docs.aws.amazon.com/glue/)
- **Snowflake Iceberg**: [docs.snowflake.com/en/user-guide/tables-iceberg](https://docs.snowflake.com/en/user-guide/tables-iceberg)
