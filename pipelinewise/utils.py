"""
Pipelinewise common utils between cli and fastsync
"""
from typing import Optional


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
            "select TABLE_NAME as table_name,"
            " TABLE_ROWS as table_rows,"
            " DATA_LENGTH + INDEX_LENGTH as table_size"
            f" from information_schema.TABLES where TABLE_SCHEMA = '{schema}';")
        tap.close_connections()
        for res in result_list:
            result.append({
                'table_name': f'{schema}.{res["table_name"]}',
                'table_rows': res['table_rows'],
                'table_size': res['table_size']
            })

    if 'FastSyncTapPostgres' in str(type(tap)):
        tap.open_connection()
        result_list = tap.query(
            "SELECT TABLE_NAME as table_name,"
            " (xpath('/row/c/text()',"
            " query_to_xml(format('select count(*) as c from %I.%I', table_schema, TABLE_NAME), FALSE, TRUE, ''))"
            ")[1]::text::int AS table_rows,"
            " pg_total_relation_size('\"'||table_schema||'\".\"'||table_name||'\"') as table_size "
            "FROM (SELECT table_schema, TABLE_NAME FROM information_schema.tables "
            f"WHERE TABLE_NAME not like 'pg_%' AND table_schema in ('{schema}')) as tb"
            )
        for res in result_list:
            result.append({
                'table_name': f'{schema}.{res[0]}',
                'table_rows': res[1],
                'table_size': res[2]
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
