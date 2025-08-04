"""
Pipelinewise common utils between cli and fastsync
"""
from typing import Optional
from cryptography.hazmat.primitives import serialization


def safe_column_name(
    name: Optional[str], quote_character: Optional[str] = None
) -> Optional[str]:
    """
    Makes column name safe by capitalizing and wrapping it in double quotes
    Args:
        name: column name
        quote_character: character the database uses for quoting identifiers

    Returns:
        str: safe column name
    """
    if quote_character is None:
        quote_character = '"'
    if name:
        return f'{quote_character}{name.upper()}{quote_character}'
    return name


def get_tables_size(schema: str, tap) -> dict:
    """get tables size"""
    result = []
    if 'FastSyncTapMySql' in str(type(tap)):
        tap.open_connections()
        result_list = tap.query(
            'select TABLE_NAME as table_name,'
            ' (DATA_LENGTH + INDEX_LENGTH)/ 1024 / 1024 as table_size'
            f' from information_schema.TABLES where TABLE_SCHEMA = \'{schema}\';')
        tap.close_connections()
        for res in result_list:
            result.append({
                'table_name': f'{schema}.{res["table_name"]}',
                'table_size': res['table_size']
            })

    if 'FastSyncTapPostgres' in str(type(tap)):
        tap.open_connection()
        result_list = tap.query(
            'SELECT TABLE_NAME as table_name,'
            ' pg_total_relation_size('
            '\'"\'||table_schema||\'"."\'||table_name||\'"\')::NUMERIC/1024::NUMERIC/1024 as table_size '
            'FROM (SELECT table_schema, TABLE_NAME FROM information_schema.tables '
            f'WHERE TABLE_NAME not like \'pg_%\' AND table_schema in (\'{schema}\')) as tb'
            )
        for res in result_list:
            result.append({
                'table_name': f'{schema}.{res[0]}',
                'table_size': res[1]
            })
        tap.close_connection()
    return result


def get_maximum_value_from_list_of_dicts(list_of_dicts: list, key: str) -> Optional[int]:
    """get maximum value from list of dicts"""
    try:
        return max(list_of_dicts, key=lambda x: x.get(key))
    except Exception:
        return None


def filter_out_selected_tables(all_schema_tables: list, selected_tables: set) -> list:
    """filter out selected tables"""
    filtered_tables = []
    for table in selected_tables:
        found_table = next((item for item in all_schema_tables if item['table_name'] == table), None)
        if found_table:
            filtered_tables.append(found_table)
    return filtered_tables


def get_schemas_of_tables_set(set_of_tables: set) -> set:
    """get schema from set of  tables"""
    set_of_schemas = set()
    for table in set_of_tables:
        schema = table.split('.')[0]
        set_of_schemas.add(schema)
    return set_of_schemas

def pem2der(pem_file: str, password: str = None) -> bytes:
    """Convert Key PEM format to DER format"""
    with open(pem_file, 'rb') as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=password,
        )
    der_key = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return der_key
