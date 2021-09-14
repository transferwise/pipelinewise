"""
 Support types in sources and their mapping to various targets
"""
from typing import Optional

# Please keep this list up to date with Singer taps
# https://github.com/transferwise/pipelinewise-tap-mysql/blob/master/tap_mysql/discover_utils.py

__MYSQL_STRING_TYPES = {'char', 'varchar', 'text', 'tinytext', 'mediumtext', 'longtext', 'enum'}

__MYSQL_BINARY_TYPES = {'binary', 'varbinary'}

__MYSQL_SPATIAL_TYPES = {
    'geometry', 'point', 'linestring', 'polygon', 'multipoint',
    'multilinestring', 'multipolygon', 'geometrycollection'
}

__MYSQL_FLOAT_TYPES = {'float', 'double', 'decimal'}

__MYSQL_JSON_TYPES = {'json'}

__MYSQL_BOOL_TYPES = {'bit', 'boolean', 'bool'}

__MYSQL_DATETIME_TYPES = {'datetime', 'timestamp', 'time', 'date'}

__MYSQL_INTEGER_TYPES = {'tinyint', 'int', 'smallint', 'mediumint', 'bigint'}

SUPPORTED_MYSQL_DATATYPES = (__MYSQL_STRING_TYPES
                             .union(__MYSQL_FLOAT_TYPES)
                             .union(__MYSQL_DATETIME_TYPES)
                             .union(__MYSQL_BINARY_TYPES)
                             .union(__MYSQL_SPATIAL_TYPES)
                             .union(__MYSQL_BOOL_TYPES)
                             .union(__MYSQL_JSON_TYPES)
                             .union(__MYSQL_INTEGER_TYPES))


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

    if mysql_column_datatype in __MYSQL_STRING_TYPES:
        target_type = 'VARCHAR'

    elif mysql_column_datatype in __MYSQL_BINARY_TYPES:
        target_type = 'BINARY'

    elif mysql_column_datatype in __MYSQL_INTEGER_TYPES:
        target_type = 'NUMBER'

        if mysql_column_datatype == 'tinyint' and mysql_column_type and mysql_column_type.startswith('tinyint(1)'):
            target_type = 'BOOLEAN'

    elif mysql_column_datatype in __MYSQL_BOOL_TYPES:
        target_type = 'BOOLEAN'

    elif mysql_column_datatype in __MYSQL_FLOAT_TYPES:
        target_type = 'FLOAT'

    elif mysql_column_datatype in __MYSQL_DATETIME_TYPES:
        target_type = 'TIMESTAMP_NTZ'

        if mysql_column_datatype == 'time':
            target_type = 'TIME'

    elif mysql_column_datatype in __MYSQL_JSON_TYPES or mysql_column_datatype in __MYSQL_SPATIAL_TYPES:
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

    if mysql_column_datatype in __MYSQL_STRING_TYPES or mysql_column_datatype in __MYSQL_BINARY_TYPES:
        return_type = 'CHARACTER VARYING'

    elif mysql_column_datatype in __MYSQL_JSON_TYPES or mysql_column_datatype in __MYSQL_SPATIAL_TYPES:
        return_type = 'JSONB'

    elif mysql_column_datatype in __MYSQL_BOOL_TYPES:
        return_type = 'BOOLEAN'

    elif mysql_column_datatype in __MYSQL_FLOAT_TYPES:
        return_type = 'DOUBLE PRECISION'

    elif mysql_column_datatype in __MYSQL_INTEGER_TYPES:

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

    elif mysql_column_datatype in __MYSQL_DATETIME_TYPES:
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

    if mysql_column_datatype in __MYSQL_STRING_TYPES or \
            mysql_column_datatype in __MYSQL_BINARY_TYPES \
            or mysql_column_datatype in __MYSQL_SPATIAL_TYPES or \
            mysql_column_datatype in __MYSQL_JSON_TYPES:
        return_type = 'STRING'

    elif mysql_column_datatype in __MYSQL_BOOL_TYPES:
        return_type = 'BOOL'

    elif mysql_column_datatype in __MYSQL_FLOAT_TYPES:
        return_type = 'NUMERIC'

    elif mysql_column_datatype in __MYSQL_INTEGER_TYPES:
        return_type = 'INT64'

        if mysql_column_datatype == 'tinyint' and mysql_column_type and mysql_column_type.startswith('tinyint(1)'):
            return_type = 'BOOLEAN'

    elif mysql_column_datatype in __MYSQL_DATETIME_TYPES:
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

    if mysql_column_datatype in __MYSQL_STRING_TYPES:

        return_type = f'CHARACTER VARYING({REDSHIFT_LONG_VARCHAR_LENGTH})'

        if mysql_column_datatype in {'char', 'varchar', 'tinytext'}:
            return_type = f'CHARACTER VARYING({REDSHIFT_SHORT_VARCHAR_LENGTH})'

        elif mysql_column_datatype == 'enum':
            return_type = f'CHARACTER VARYING({REDSHIFT_DEFAULT_VARCHAR_LENGTH})'

    elif mysql_column_datatype in __MYSQL_BINARY_TYPES or mysql_column_datatype in __MYSQL_JSON_TYPES:
        return_type = f'CHARACTER VARYING({REDSHIFT_LONG_VARCHAR_LENGTH})'

    elif mysql_column_datatype in __MYSQL_SPATIAL_TYPES:
        return_type = f'CHARACTER VARYING({REDSHIFT_DEFAULT_VARCHAR_LENGTH})'

    elif mysql_column_datatype in __MYSQL_BOOL_TYPES:
        return_type = 'BOOLEAN'

    elif mysql_column_datatype in __MYSQL_FLOAT_TYPES:
        return_type = 'FLOAT'

    elif mysql_column_datatype in __MYSQL_INTEGER_TYPES:

        return_type = 'NUMERIC NULL'
        if mysql_column_datatype == 'tinyint' and mysql_column_type and mysql_column_type.startswith('tinyint(1)'):
            return_type = 'BOOLEAN'

    elif mysql_column_datatype in __MYSQL_DATETIME_TYPES:
        if mysql_column_datatype == 'time':
            return_type = 'TIME WITHOUT TIME ZONE'
        else:
            return_type = 'TIMESTAMP WITHOUT TIME ZONE'

    return return_type


MYSQL_TO_REDSHIFT_MAPPER = __mysql_to_redshift_mapper
