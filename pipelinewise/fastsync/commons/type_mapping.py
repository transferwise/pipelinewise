"""
 Support types in sources and their mapping to various targets
"""
from typing import Optional

# Please keep this list up to date with Singer taps
# https://github.com/transferwise/pipelinewise-tap-mysql/blob/master/tap_mysql/discover_utils.py

__STRING_TYPES = {'char', 'varchar', 'text', 'tinytext', 'mediumtext', 'longtext', 'enum'}

__BINARY_TYPES = {'binary', 'varbinary'}

__SPATIAL_TYPES = {
    'geometry', 'point', 'linestring', 'polygon', 'multipoint',
    'multilinestring', 'multipolygon', 'geometrycollection'
}

__FLOAT_TYPES = {'float', 'double', 'decimal'}

__JSON_TYPES = {'json'}

__BOOL_TYPES = {'bit', 'boolean', 'bool'}

__DATETIME_TYPES = {'datetime', 'timestamp', 'time', 'date'}

__INTEGER_TYPES = {'tinyint', 'int', 'smallint', 'mediumint', 'bigint'}

SUPPORTED_MYSQL_DATATYPES = (__STRING_TYPES
                             .union(__FLOAT_TYPES)
                             .union(__DATETIME_TYPES)
                             .union(__BINARY_TYPES)
                             .union(__SPATIAL_TYPES)
                             .union(__BOOL_TYPES)
                             .union(__JSON_TYPES)
                             .union(__INTEGER_TYPES))


REDSHIFT_DEFAULT_VARCHAR_LENGTH = 10000
REDSHIFT_SHORT_VARCHAR_LENGTH = 256
REDSHIFT_LONG_VARCHAR_LENGTH = 65535


def __mysql_to_sf_mapper(mysql_column_datatype: str, mysql_column_type: str) -> Optional[str]:
    """
    Mapping Mysql/Mariadb DB column types to the equivalent in Snowflake
    Args:
        mysql_column_datatype: column datatype
        mysql_column_type: column type

    Returns: a string if column is supported, None otherwise
    """
    target_type = None

    if mysql_column_datatype in __STRING_TYPES:
        target_type = 'VARCHAR'

    if mysql_column_datatype in __BINARY_TYPES:
        target_type = 'BINARY'

    if mysql_column_datatype in __INTEGER_TYPES:
        target_type = 'NUMBER'

        if mysql_column_datatype == 'tinyint' and mysql_column_type and mysql_column_type.startswith('tinyint(1)'):
            target_type = 'BOOLEAN'

    if mysql_column_datatype in __BOOL_TYPES:
        target_type = 'BOOLEAN'

    if mysql_column_datatype in __FLOAT_TYPES:
        target_type = 'FLOAT'

    if mysql_column_datatype in __DATETIME_TYPES:
        target_type = 'TIMESTAMP_NTZ'

        if mysql_column_datatype == 'time':
            target_type = 'TIME'

    if mysql_column_datatype in __JSON_TYPES or mysql_column_datatype in __SPATIAL_TYPES:
        target_type = 'VARIANT'

    return target_type


MYSQL_TO_SNOWFLAKE_MAPPER = __mysql_to_sf_mapper


def __mysql_to_postgres_mapper(mysql_column_datatype: str, mysql_column_type: str) -> Optional[str]:
    """
    Mapping Mysql/Mariadb DB column types to the equivalent in Postgres
    Args:
        mysql_column_datatype: column datatype
        mysql_column_type: column type

    Returns: a string if column is supported, None otherwise
    """
    return_type = None

    if mysql_column_datatype in __STRING_TYPES or mysql_column_datatype in __BINARY_TYPES:
        return_type = 'CHARACTER VARYING'

    if mysql_column_datatype in __JSON_TYPES or mysql_column_datatype in __SPATIAL_TYPES:
        return_type = 'JSONB'

    if mysql_column_datatype in __BOOL_TYPES:
        return_type = 'BOOLEAN'

    if mysql_column_datatype in __FLOAT_TYPES:
        return_type = 'DOUBLE PRECISION'

    if mysql_column_datatype in __INTEGER_TYPES:

        if mysql_column_datatype == 'tinyint':
            return_type = 'BOOLEAN' if mysql_column_type and \
                                       mysql_column_type.startswith('tinyint(1)') else 'SMALLINT NULL'

        elif mysql_column_datatype == 'smallint':
            return_type = 'SMALLINT NULL'

        elif mysql_column_datatype == 'bigint':
            return_type = 'BIGINT NULL'

        elif mysql_column_datatype in ('int', 'mediumint'):
            return_type = 'INTEGER NULL'

        else:
            return_type = 'NUMERIC'

    if mysql_column_datatype in __DATETIME_TYPES:
        return_type = 'TIME WITHOUT TIME ZONE' if mysql_column_datatype == 'time' else 'TIMESTAMP WITHOUT TIME ZONE'

    return return_type


MYSQL_TO_POSTGRES_MAPPER = __mysql_to_postgres_mapper


def __mysql_to_bigquery_mapper(mysql_column_datatype: str, mysql_column_type: str) -> Optional[str]:
    """
    Mapping Mysql/Mariadb DB column types to the equivalent in BigQuery
    Args:
        mysql_column_datatype: column datatype
        mysql_column_type: column type

    Returns: a string if column is supported, None otherwise
    """
    return_type = None

    if mysql_column_datatype in __STRING_TYPES or \
            mysql_column_datatype in __BINARY_TYPES \
            or mysql_column_datatype in __SPATIAL_TYPES or \
            mysql_column_datatype in __JSON_TYPES:
        return_type = 'STRING'

    if mysql_column_datatype in __BOOL_TYPES:
        return_type = 'BOOL'

    if mysql_column_datatype in __FLOAT_TYPES:
        return_type = 'NUMERIC'

    if mysql_column_datatype in __INTEGER_TYPES:
        return_type = 'INT64'

        if mysql_column_datatype == 'tinyint' and mysql_column_type and mysql_column_type.startswith('tinyint(1)'):
            return_type = 'BOOLEAN'

    if mysql_column_datatype in __DATETIME_TYPES:
        if mysql_column_datatype == 'time':
            return_type = 'TIME'
        else:
            return_type = 'TIMESTAMP'

    return return_type


MYSQL_TO_BIGQUERY_MAPPER = __mysql_to_bigquery_mapper


def __mysql_to_redshift_mapper(mysql_column_datatype: str, mysql_column_type: str) -> Optional[str]:
    """
    Mapping Mysql/Mariadb DB column types to the equivalent in Redshift
    Args:
        mysql_column_datatype: column datatype
        mysql_column_type: column type

    Returns: a string if column is supported, None otherwise
    """

    return_type = None

    if mysql_column_datatype in __STRING_TYPES:

        return_type = f'CHARACTER VARYING({REDSHIFT_LONG_VARCHAR_LENGTH})'

        if mysql_column_datatype in {'char', 'varchar', 'tinytext'}:
            return_type = f'CHARACTER VARYING({REDSHIFT_SHORT_VARCHAR_LENGTH})'

        elif mysql_column_datatype == 'enum':
            return_type = f'CHARACTER VARYING({REDSHIFT_DEFAULT_VARCHAR_LENGTH})'

    if mysql_column_datatype in __BINARY_TYPES or mysql_column_datatype in __JSON_TYPES:
        return_type = f'CHARACTER VARYING({REDSHIFT_LONG_VARCHAR_LENGTH})'

    if mysql_column_datatype in __SPATIAL_TYPES:
        return_type = f'CHARACTER VARYING({REDSHIFT_DEFAULT_VARCHAR_LENGTH})'

    if mysql_column_datatype in __BOOL_TYPES:
        return_type = 'BOOLEAN'

    if mysql_column_datatype in __FLOAT_TYPES:
        return_type = 'FLOAT'

    if mysql_column_datatype in __INTEGER_TYPES:

        return_type = 'NUMERIC NULL'
        if mysql_column_datatype == 'tinyint' and mysql_column_type and mysql_column_type.startswith('tinyint(1)'):
            return_type = 'BOOLEAN'

    if mysql_column_datatype in __DATETIME_TYPES:
        if mysql_column_datatype == 'time':
            return_type = 'TIME WITHOUT TIME ZONE'
        else:
            return_type = 'TIMESTAMP WITHOUT TIME ZONE'

    return return_type


MYSQL_TO_REDSHIFT_MAPPER = __mysql_to_redshift_mapper
