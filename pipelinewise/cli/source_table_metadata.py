"""Read selected source tables' indexes, keys, and storage through one connection."""
from contextlib import closing

from .source_table_sizes import open_source_connection, read_source_table_size


MYSQL_KEYS_SQL = """
SELECT tc.CONSTRAINT_NAME AS name, tc.CONSTRAINT_TYPE AS kind,
       k.COLUMN_NAME AS column_name, k.REFERENCED_TABLE_SCHEMA AS referenced_schema,
       k.REFERENCED_TABLE_NAME AS referenced_table, k.REFERENCED_COLUMN_NAME AS referenced_column,
       r.UPDATE_RULE AS on_update, r.DELETE_RULE AS on_delete
FROM information_schema.TABLE_CONSTRAINTS tc
JOIN information_schema.KEY_COLUMN_USAGE k
  ON k.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND k.TABLE_NAME = tc.TABLE_NAME
 AND k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS r
  ON r.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA AND r.TABLE_NAME = tc.TABLE_NAME
 AND r.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
WHERE tc.TABLE_SCHEMA = %s AND tc.TABLE_NAME = %s
  AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
ORDER BY tc.CONSTRAINT_NAME, k.ORDINAL_POSITION
"""
POSTGRES_INDEXES_SQL = """
SELECT idx.relname AS name, am.amname AS type, i.indisunique AS is_unique,
       i.indisprimary AS is_primary, i.indisvalid AS is_valid,
       pg_catalog.pg_get_indexdef(i.indexrelid) AS definition,
       pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS predicate,
       a.attname AS column_name, k.position AS position,
       pg_catalog.pg_get_indexdef(i.indexrelid, k.position::integer, true) AS column_definition,
       k.position > i.indnkeyatts AS is_included,
       CASE WHEN k.position <= i.indnkeyatts AND am.amname = 'btree'
            THEN CASE WHEN (i.indoption[k.position::integer - 1] & 1) = 1 THEN 'DESC' ELSE 'ASC' END
            ELSE NULL END AS sort_order
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
JOIN pg_catalog.pg_class idx ON idx.oid = i.indexrelid
JOIN pg_catalog.pg_am am ON am.oid = idx.relam
CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, position)
LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
WHERE n.nspname = %s AND t.relname = %s
ORDER BY idx.relname, k.position
"""
POSTGRES_KEYS_SQL = """
SELECT con.conname AS name, con.contype AS kind,
       ARRAY(SELECT a.attname FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, position)
             JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
             ORDER BY k.position) AS columns,
       rn.nspname AS referenced_schema, rt.relname AS referenced_table,
       ARRAY(SELECT a.attname FROM unnest(con.confkey) WITH ORDINALITY AS k(attnum, position)
             JOIN pg_catalog.pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.attnum
             ORDER BY k.position) AS referenced_columns,
       con.confupdtype AS on_update, con.confdeltype AS on_delete,
       con.condeferrable AS is_deferrable, con.condeferred AS initially_deferred,
       con.convalidated AS is_validated
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class t ON t.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
LEFT JOIN pg_catalog.pg_class rt ON rt.oid = con.confrelid
LEFT JOIN pg_catalog.pg_namespace rn ON rn.oid = rt.relnamespace
WHERE n.nspname = %s AND t.relname = %s AND con.contype IN ('p', 'u', 'f')
ORDER BY con.conname
"""
FOREIGN_KEY_ACTIONS = {'a': 'NO ACTION', 'r': 'RESTRICT', 'c': 'CASCADE', 'n': 'SET NULL', 'd': 'SET DEFAULT'}


def _query_rows(cursor, query, params=None):
    """Return named metadata rows from either driver's tuple cursor."""
    cursor.execute(query, params)
    names = [column[0].lower() for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _mysql_indexes(cursor, schema, table):
    """Preserve index key order, prefix lengths, and optional functional expressions."""
    quoted_schema = '`' + schema.replace('`', '``') + '`'
    quoted_table = '`' + table.replace('`', '``') + '`'
    rows = _query_rows(cursor, f'SHOW INDEX FROM {quoted_table} FROM {quoted_schema}')
    indexes = {}
    for row in sorted(rows, key=lambda item: (item['key_name'], item['seq_in_index'])):
        index = indexes.setdefault(row['key_name'], {
            'name': row['key_name'], 'type': row['index_type'],
            'is_unique': not bool(row['non_unique']), 'is_primary': row['key_name'] == 'PRIMARY',
            'is_valid': None, 'predicate': None, 'definition': None, 'columns': [],
        })
        index['columns'].append({
            'name': row['column_name'], 'expression': row.get('expression'),
            'order': {'A': 'ASC', 'D': 'DESC'}.get(row.get('collation')),
            'prefix_length': row.get('sub_part'), 'is_included': False,
        })
    return list(indexes.values())


def _postgres_indexes(cursor, schema, table):
    """Report expressions, INCLUDE columns, partial predicates, and validity."""
    indexes = {}
    for row in _query_rows(cursor, POSTGRES_INDEXES_SQL, (schema, table)):
        index = indexes.setdefault(row['name'], {
            key: row[key] for key in ['name', 'type', 'is_unique', 'is_primary', 'is_valid', 'predicate', 'definition']
        })
        index.setdefault('columns', []).append({
            'name': row['column_name'],
            'expression': row['column_definition'] if row['column_name'] is None else None,
            'order': row['sort_order'], 'prefix_length': None, 'is_included': row['is_included'],
        })
    return list(indexes.values())


def _keys_result():
    """Use null for a missing primary key and empty lists for other missing keys."""
    return {'primary_key': None, 'unique_constraints': [], 'foreign_keys': []}


def _mysql_keys(cursor, schema, table):
    """Group ordered composite key columns without joining unrelated constraints."""
    constraints = {}
    for row in _query_rows(cursor, MYSQL_KEYS_SQL, (schema, table)):
        key = constraints.setdefault(row['name'], {
            'name': row['name'], 'kind': row['kind'], 'columns': [],
            'is_deferrable': None, 'initially_deferred': None, 'is_validated': None,
        })
        key['columns'].append(row['column_name'])
        if row['kind'] == 'FOREIGN KEY':
            key.update({field: row[field] for field in [
                'referenced_schema', 'referenced_table', 'on_update', 'on_delete']
                        })
            key.setdefault('referenced_columns', []).append(row['referenced_column'])
    result = _keys_result()
    for key in constraints.values():
        kind = key.pop('kind')
        if kind == 'PRIMARY KEY':
            result['primary_key'] = key
        elif kind == 'UNIQUE':
            result['unique_constraints'].append(key)
        else:
            result['foreign_keys'].append(key)
    return result


def _postgres_keys(cursor, schema, table):
    """Keep ordered local and referenced columns together for composite keys."""
    result = _keys_result()
    for row in _query_rows(cursor, POSTGRES_KEYS_SQL, (schema, table)):
        key = {field: row[field] for field in [
            'name', 'columns', 'is_deferrable', 'initially_deferred', 'is_validated'
        ]}
        if row['kind'] == 'p':
            result['primary_key'] = key
        elif row['kind'] == 'u':
            result['unique_constraints'].append(key)
        else:
            key.update({field: row[field] for field in ['referenced_schema', 'referenced_table', 'referenced_columns']})
            key.update({field: FOREIGN_KEY_ACTIONS[row[field]] for field in ['on_update', 'on_delete']})
            result['foreign_keys'].append(key)
    return result


def get_source_table_details(tap_type, config, tables):
    """Return storage, indexes, and keys per (schema, table) using one source connection."""
    if not tables:
        return {}
    # YugabyteDB's YSQL exposes the same pg_catalog index and constraint views as PostgreSQL.
    index_reader, key_reader = (
        (_mysql_indexes, _mysql_keys) if tap_type == 'tap-mysql' else (_postgres_indexes, _postgres_keys)
    )
    details = {}
    with closing(open_source_connection(tap_type, config)) as connection, connection.cursor() as cursor:
        for schema, table in tables:
            size_bytes = read_source_table_size(cursor, tap_type, schema, table)
            indexes = [] if tap_type == 'tap-mysql' and size_bytes is None else index_reader(cursor, schema, table)
            details[(schema, table)] = {
                'size_bytes': size_bytes,
                'indexes': indexes,
                **key_reader(cursor, schema, table),
            }
    return details
