"""Test fixtures and mock data for target-iceberg tests"""


def get_test_config():
    """Get minimal test configuration"""
    return {
        'aws_region': 'us-east-1',
        's3_bucket': 'test-bucket',
        's3_key_prefix': 'iceberg/',
        'glue_catalog_id': '123456789012',
        'default_target_schema': 'test_schema',
        'batch_size_rows': 1000,
        'hard_delete': False
    }


def get_test_config_with_snowflake():
    """Get test configuration with Snowflake integration"""
    config = get_test_config()
    config['snowflake_integration'] = {
        'enabled': True,
        'account': 'test_account',
        'user': 'test_user',
        'password': 'test_password',
        'database': 'TEST_DB',
        'warehouse': 'TEST_WH',
        'external_volume': 'test_external_volume',
        'catalog_integration': 'test_catalog_integration'
    }
    return config


def get_simple_singer_schema():
    """Get simple Singer schema for testing"""
    return {
        'type': 'SCHEMA',
        'stream': 'test_stream',
        'schema': {
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': 'string'},
                'email': {'type': ['string', 'null']}
            }
        },
        'key_properties': ['id']
    }


def get_complex_singer_schema():
    """Get complex Singer schema with various types"""
    return {
        'type': 'SCHEMA',
        'stream': 'complex_stream',
        'schema': {
            'properties': {
                'id': {'type': 'integer'},
                'name': {'type': 'string'},
                'age': {'type': ['integer', 'null']},
                'salary': {'type': 'number'},
                'is_active': {'type': 'boolean'},
                'created_at': {'type': 'string', 'format': 'date-time'},
                'birth_date': {'type': 'string', 'format': 'date'},
                'metadata': {'type': ['object', 'null']},
                'tags': {'type': ['array', 'null']}
            }
        },
        'key_properties': ['id']
    }


def get_test_records():
    """Get sample test records"""
    return [
        {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'},
        {'id': 2, 'name': 'Bob', 'email': 'bob@example.com'},
        {'id': 3, 'name': 'Charlie', 'email': None}
    ]


def get_singer_messages():
    """Get Singer formatted messages"""
    import json

    schema = get_simple_singer_schema()
    records = get_test_records()

    messages = [json.dumps(schema)]

    for record in records:
        messages.append(json.dumps({
            'type': 'RECORD',
            'stream': 'test_stream',
            'record': record
        }))

    messages.append(json.dumps({
        'type': 'STATE',
        'value': {'bookmarks': {'test_stream': {'id': 3}}}
    }))

    return messages


def get_fastsync_columns():
    """Get FastSync column definitions"""
    return [
        {'name': 'id', 'type': 'BIGINT', 'not_null': True},
        {'name': 'name', 'type': 'STRING', 'not_null': True},
        {'name': 'email', 'type': 'STRING'},
        {'name': 'age', 'type': 'INTEGER'},
        {'name': 'salary', 'type': 'DOUBLE'},
        {'name': 'is_active', 'type': 'BOOLEAN'},
        {'name': 'created_at', 'type': 'TIMESTAMP'},
        {'name': 'birth_date', 'type': 'DATE'}
    ]


def get_postgres_type_mappings():
    """Get PostgreSQL to Iceberg type mappings for testing"""
    return {
        'serial': 'BIGINT',
        'bigserial': 'BIGINT',
        'integer': 'INTEGER',
        'bigint': 'BIGINT',
        'real': 'DOUBLE',
        'double precision': 'DOUBLE',
        'numeric': 'NUMERIC',
        'decimal': 'NUMERIC',
        'varchar': 'STRING',
        'text': 'STRING',
        'date': 'DATE',
        'timestamp': 'TIMESTAMP',
        'timestamp without time zone': 'TIMESTAMP',
        'timestamp with time zone': 'TIMESTAMP',
        'boolean': 'BOOLEAN',
        'bytea': 'BINARY',
        'json': 'STRING',
        'jsonb': 'STRING',
        'uuid': 'STRING',
        'ARRAY': 'STRING'
    }


def get_mysql_type_mappings():
    """Get MySQL to Iceberg type mappings for testing"""
    return {
        'tinyint': 'INTEGER',
        'smallint': 'INTEGER',
        'mediumint': 'INTEGER',
        'int': 'INTEGER',
        'bigint': 'BIGINT',
        'float': 'DOUBLE',
        'double': 'DOUBLE',
        'decimal': 'NUMERIC',
        'char': 'STRING',
        'varchar': 'STRING',
        'text': 'STRING',
        'date': 'DATE',
        'datetime': 'TIMESTAMP',
        'timestamp': 'TIMESTAMP',
        'time': 'STRING',
        'year': 'INTEGER',
        'binary': 'BINARY',
        'blob': 'BINARY',
        'json': 'STRING',
        'enum': 'STRING',
        'set': 'STRING',
        'bit': 'BOOLEAN'
    }
