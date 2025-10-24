# pipelinewise-target-iceberg

[![License: Apache2](https://img.shields.io/badge/License-Apache2-yellow.svg)](https://opensource.org/licenses/Apache-2.0)

[Singer](https://www.singer.io/) target that loads data into Apache Iceberg tables on S3, with automatic catalog management via AWS Glue.

This is a [PipelineWise](https://transferwise.github.io/pipelinewise/) compatible target connector.

## Features

- Writes data to Apache Iceberg tables on S3
- Automatic table creation and schema evolution
- Integration with AWS Glue Data Catalog
- Support for partitioning strategies
- Optional Snowflake external table registration
- MERGE operations for updates and deletes
- Efficient parquet file writing with compression
- Copy-on-Write (CoW) format for Snowflake compatibility

## Installation

### From PyPI (when published)

```bash
pip install pipelinewise-target-iceberg
```

### From Source

```bash
cd singer-connectors/target-iceberg
pip install .
```

## Configuration

### Required Configuration

```json
{
  "aws_access_key_id": "YOUR_AWS_ACCESS_KEY",
  "aws_secret_access_key": "YOUR_AWS_SECRET_KEY",
  "aws_region": "us-east-1",
  "s3_bucket": "your-data-lake-bucket",
  "s3_key_prefix": "iceberg-warehouse/",
  "glue_catalog_id": "123456789012",
  "default_target_schema": "default_db"
}
```

### Optional Configuration

```json
{
  "compression_method": "snappy",
  "parquet_row_group_size": 100000,
  "partition_columns": ["year", "month"],
  "add_metadata_columns": true,
  "hard_delete": false,
  "data_flattening_max_level": 0,
  "snowflake_integration": {
    "enabled": true,
    "account": "your-account",
    "database": "your_database",
    "external_volume": "iceberg_external_volume",
    "catalog_integration": "glue_catalog"
  }
}
```

### Configuration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `aws_access_key_id` | String | Yes | - | AWS Access Key ID |
| `aws_secret_access_key` | String | Yes | - | AWS Secret Access Key |
| `aws_session_token` | String | No | - | AWS Session Token (for temporary credentials) |
| `aws_region` | String | Yes | us-east-1 | AWS Region |
| `s3_bucket` | String | Yes | - | S3 bucket for Iceberg data files |
| `s3_key_prefix` | String | No | iceberg/ | Prefix for S3 keys |
| `glue_catalog_id` | String | Yes | - | AWS Glue Catalog ID (usually your AWS account ID) |
| `default_target_schema` | String | Yes | - | Default database/namespace for tables |
| `compression_method` | String | No | snappy | Parquet compression: none, snappy, gzip, zstd |
| `parquet_row_group_size` | Integer | No | 100000 | Number of rows per row group |
| `partition_columns` | Array | No | [] | List of columns to partition by |
| `add_metadata_columns` | Boolean | No | true | Add _sdc metadata columns |
| `hard_delete` | Boolean | No | false | Perform hard deletes instead of soft |
| `data_flattening_max_level` | Integer | No | 0 | Max nesting level for JSON flattening |
| `batch_size_rows` | Integer | No | 100000 | Batch size for inserts |
| `flush_all_streams` | Boolean | No | false | Flush after every stream |
| `parallelism` | Integer | No | 0 | Number of parallel processes (0 = auto) |
| `max_parallelism` | Integer | No | 16 | Maximum parallel processes |

#### Snowflake Integration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `snowflake_integration.enabled` | Boolean | No | false | Enable Snowflake external table creation |
| `snowflake_integration.account` | String | Conditional | - | Snowflake account identifier |
| `snowflake_integration.user` | String | Conditional | - | Snowflake user |
| `snowflake_integration.password` | String | Conditional | - | Snowflake password |
| `snowflake_integration.database` | String | Conditional | - | Snowflake database name |
| `snowflake_integration.warehouse` | String | No | - | Snowflake warehouse |
| `snowflake_integration.role` | String | No | - | Snowflake role |
| `snowflake_integration.external_volume` | String | Conditional | - | Snowflake external volume name |
| `snowflake_integration.catalog_integration` | String | Conditional | - | Snowflake catalog integration name |

## Quick Start

See [sample_config/QUICK_START.md](sample_config/QUICK_START.md) for a quick start guide.

For detailed examples and full configuration options, see the [sample_config/](sample_config/) directory.

## Usage with PipelineWise

Add target configuration in your PipelineWise target YAML:

```yaml
---
id: "iceberg_data_lake"
name: "Apache Iceberg on S3"
type: "target-iceberg"

db_conn:
  aws_access_key_id: "YOUR_AWS_ACCESS_KEY"
  aws_secret_access_key: "YOUR_AWS_SECRET_KEY"
  aws_region: "us-east-1"
  s3_bucket: "your-data-lake-bucket"
  s3_key_prefix: "iceberg-warehouse/"
  glue_catalog_id: "123456789012"
  default_target_schema: "analytics"
  compression_method: "snappy"
  add_metadata_columns: true
```

**More examples:**
- [Basic target configuration](sample_config/pipelinewise/target_iceberg.yml)
- [Target with Snowflake integration](sample_config/pipelinewise/target_iceberg_with_snowflake.yml)
- [MySQL to Iceberg pipeline](sample_config/pipelinewise/tap_mysql_to_iceberg.yml)
- [PostgreSQL to Iceberg pipeline](sample_config/pipelinewise/tap_postgres_to_iceberg.yml)

## Partitioning

Iceberg tables can be partitioned for better query performance:

```yaml
schemas:
  - source_schema: "public"
    target_schema: "analytics"
    tables:
      - table_name: "events"
        partition_columns:
          - "year"
          - "month"
```

## AWS Glue Catalog Integration

The target automatically:
1. Creates databases in Glue Catalog if they don't exist
2. Creates Iceberg table definitions in Glue
3. Updates table schemas when source schemas evolve
4. Maintains table metadata and statistics

Ensure your AWS credentials have these permissions:
- `glue:CreateDatabase`
- `glue:GetDatabase`
- `glue:CreateTable`
- `glue:UpdateTable`
- `glue:GetTable`
- `s3:PutObject`
- `s3:GetObject`
- `s3:DeleteObject`
- `s3:ListBucket`

## Snowflake External Tables

When `snowflake_integration.enabled` is true, the target will:
1. Create external Iceberg tables in Snowflake
2. Link them to the Glue Catalog
3. Enable querying Iceberg data directly from Snowflake

All Iceberg tables are created with Copy-on-Write (CoW) format for optimal Snowflake compatibility:
- `write.format.default=parquet` - Uses Parquet file format
- `write.delete.mode=copy-on-write` - Rewrites data files on updates/deletes
- `format-version=2` - Uses Iceberg table format v2

Prerequisites:
- Snowflake External Volume configured for S3
- Catalog Integration configured for AWS Glue
- Appropriate Snowflake permissions

**See [sample_config/SNOWFLAKE_SETUP.md](sample_config/SNOWFLAKE_SETUP.md) for complete setup instructions.**

## Schema Evolution

The target supports automatic schema evolution:
- Adding new columns (backward compatible)
- Updating column comments
- Promoting data types (e.g., int to bigint)

Note: Removing columns or incompatible type changes require manual intervention.

## License

Apache License Version 2.0

See [LICENSE](../../LICENSE) to see the full text.
