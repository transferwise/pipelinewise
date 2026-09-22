#!/usr/bin/env python3
"""Check a tap-yugabyte YAML against the source, and report the indexes it needs.

Service owners choose a replication method and key per table. Those choices imply
index requirements that YugabyteDB does not enforce and that nothing in a normal
run reports: a missing one is not an error, it is a full table scan on every
sync. This reads the config the owner already wrote, checks each table against
the live source, and prints the DDL to close the gap.

    yb_index_check.py tap_yugabyte.yml --host h --port 5433 --user u --dbname d

Exit status is 0 when nothing needs doing, 1 when any table needs an index or has
a configuration that cannot work.
"""

import argparse
import os
import sys

import psycopg2
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from tap_yugabyte import keyset  # noqa: E402  pylint: disable=wrong-import-position

OK, ACTION, BLOCK = 'OK', 'ACTION', 'BLOCK'


def _repair_ddl(schema_name, found, create_statement):
    """The statements that actually turn this line into OK.

    A CREATE on its own only works when nothing is there. When the index exists
    but is the wrong shape -- wrong bucket count, hashed discriminator, missing
    trailing key, not unique, one tablet, or a backfill that never completed --
    the CREATE fails with `relation "..." already exists`, and the operator is
    left holding a statement that cannot run. It has to be dropped first.

    Dropping is safe here precisely because the index is wrong: nothing the tap
    issues can use it.
    """
    statements = []
    if found is not None:
        statements.append(f'DROP INDEX "{schema_name}"."{found["name"]}";')
    statements.append(create_statement + ';')
    return statements


def _primary_key(cur, schema_name, table_name):
    """Key columns in index order, with their types."""
    cur.execute("""
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_index x
        JOIN pg_class c ON c.oid = x.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN LATERAL unnest(x.indkey) WITH ORDINALITY AS kc(attnum, ord)
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = kc.attnum
        WHERE n.nspname = %s AND c.relname = %s AND x.indisprimary
        ORDER BY kc.ord
    """, (schema_name, table_name))
    rows = cur.fetchall()
    return [r[0] for r in rows], [r[1] for r in rows]


def _pk_index(cur, schema_name, table_name, pk_columns, pk_types, buckets):
    """Shape 1/2: ((yb_hash_code(pk) % N) ASC, pk ASC) UNIQUE.

    Required by FULL_TABLE and by the full-table bootstrap stage of LOG_BASED,
    which runs the same code.
    """
    plan = keyset.plan_keyset_strategy(cur, schema_name, table_name,
                                       pk_columns, pk_types, buckets)
    if plan['strategy'] == keyset.STRATEGY_PK_RANGE:
        return OK, ['Primary key is range-sharded, so it already pages in key '
                    'order. Adding an index here would only cost writes.'], []
    if plan['strategy'] != keyset.STRATEGY_BUCKET_INDEX:
        return BLOCK, plan['blockers'], []

    name = keyset.index_name(table_name)
    found, problems = keyset.check_index(cur, schema_name, table_name, name,
                                         pk_columns, pk_columns, buckets)
    if found is not None and not problems:
        return OK, [f'{name} supplies the order the hash-sharded primary key '
                    f'lacks, {buckets} buckets over {found["tablets"]} tablets.'], []
    notes = ['Primary key is hash-sharded, so it has no order to page along. '
             'Without this index every resume re-reads the whole table.']
    notes.extend(problems)
    return ACTION, notes, _repair_ddl(schema_name, found, plan['index_ddl'])


def check_full_table(cur, schema_name, table_name, buckets):
    """FULL_TABLE pages by primary key, so it needs the key to be scannable in
    order."""
    pk_columns, pk_types = _primary_key(cur, schema_name, table_name)
    return _pk_index(cur, schema_name, table_name, pk_columns, pk_types, buckets)


def check_log_based(cur, schema_name, table_name, buckets):
    """LOG_BASED reads the change stream -- but its first stage is a full-table
    bootstrap that runs full_table.sync_table, so it needs the same index."""
    pk_columns, pk_types = _primary_key(cur, schema_name, table_name)
    status, notes, ddl = _pk_index(cur, schema_name, table_name,
                                   pk_columns, pk_types, buckets)
    notes.append('The change stream itself needs no index. The initial snapshot '
                 'does: LOG_BASED bootstraps through the same full-table scan, '
                 'and resumes it after an interruption.')
    notes.append('It does need a replica identity that can name a deleted row.')
    return status, notes, ddl


def check_incremental(cur, schema_name, table_name, replication_key, buckets):
    """INCREMENTAL pages by the replication key, so the key -- not the primary
    key -- is what has to be indexed, non-null and actually increasing."""
    if not replication_key:
        return BLOCK, ['replication_method is INCREMENTAL but no replication_key '
                       'is set.'], []

    pk_columns, pk_types = _primary_key(cur, schema_name, table_name)
    if not pk_columns:
        return BLOCK, ['Table has no primary key, so there is nothing to bucket '
                       'on and no tiebreaker for rows sharing a replication-key '
                       'value.'], []

    notes, ddl, status = [], [], OK
    key = keyset.require_monotonic_key(cur, schema_name, table_name,
                                       replication_key, pk_columns)
    if key['hard_failures']:
        status = BLOCK
        notes.extend(key['hard_failures'])
    notes.extend(key['risks'])

    # when the replication key IS the primary key the shape-1 index already is
    # the shape-2 index; asking for a second one would ask for a duplicate
    if list(pk_columns) == [replication_key]:
        pk_status, pk_notes, pk_ddl = _pk_index(cur, schema_name, table_name,
                                                pk_columns, pk_types, buckets)
        notes.append('The replication key is the primary key, so the primary-key '
                     'keyset index serves both. No second index.')
        notes.extend(pk_notes)
        ddl.extend(pk_ddl)
        if pk_status == BLOCK or (pk_status == ACTION and status == OK):
            status = pk_status
        return status, notes, ddl

    trailing = [replication_key] + list(pk_columns)
    name = keyset.replication_key_index_name(table_name, replication_key)
    found, problems = keyset.check_index(cur, schema_name, table_name, name,
                                         pk_columns, trailing, buckets)
    if found is not None and not problems:
        notes.append(f'{name} orders {replication_key} within each of {buckets} '
                     f'buckets and ends in the primary key, so the watermark scan '
                     f'streams in order and resumes inside a tie.')
    else:
        notes.extend(problems)
        fq_table_name = f'"{schema_name}"."{table_name}"'
        ddl.extend(_repair_ddl(schema_name, found, keyset.replication_key_index_ddl(
            fq_table_name, table_name, replication_key, pk_columns, buckets)))
        if status != BLOCK:
            status = ACTION

    if key.get('tiebreaker'):
        notes.append(f"{replication_key} is not unique; the index ends in "
                     f"{', '.join(key['tiebreaker'])} so rows sharing a value keep "
                     f"a stable order across runs.")
    return status, notes, ddl


def check_table(cur, schema_name, table, buckets):
    method = (table.get('replication_method') or '').upper()
    table_name = table['table_name']
    if method == 'FULL_TABLE':
        return check_full_table(cur, schema_name, table_name, buckets)
    if method == 'INCREMENTAL':
        return check_incremental(cur, schema_name, table_name,
                                 table.get('replication_key'), buckets)
    if method == 'LOG_BASED':
        return check_log_based(cur, schema_name, table_name, buckets)
    return BLOCK, [f'Unknown replication_method {method!r}.'], []


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config', help='tap YAML the service owner maintains')
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', type=int, default=5433)
    parser.add_argument('--user', required=True)
    parser.add_argument('--dbname', required=True)
    parser.add_argument('--password', default=os.environ.get('PGPASSWORD'))
    parser.add_argument('--buckets', type=int, default=keyset.BUCKETS_DEFAULT,
                        help='keyset_buckets the tap is configured for')
    args = parser.parse_args()

    with open(args.config, encoding='utf-8') as handle:
        config = yaml.safe_load(handle)

    conn = psycopg2.connect(host=args.host, port=args.port, user=args.user,
                            dbname=args.dbname, password=args.password)
    conn.autocommit = True
    all_ddl, worst = [], OK

    with conn.cursor() as cur:
        for schema in config.get('schemas') or []:
            schema_name = schema['source_schema']
            for table in schema.get('tables') or []:
                status, notes, ddl = check_table(cur, schema_name, table, args.buckets)
                method = table.get('replication_method')
                key = table.get('replication_key')
                heading = f"{schema_name}.{table['table_name']}  [{method}"
                heading += f" on {key}]" if key else "]"
                print(f'\n{status:6s} {heading}')
                for note in notes:
                    print(f'       - {note}')
                for statement in ddl:
                    print(f'       $ {statement}')
                all_ddl.extend(ddl)
                if status == BLOCK or (status == ACTION and worst != BLOCK):
                    worst = status
    conn.close()

    if all_ddl:
        print('\n' + '=' * 72 + '\nAll DDL, to hand to the service owner:\n')
        for statement in all_ddl:
            print(f'  {statement}')
    print(f'\nWorst status: {worst}')
    return 0 if worst == OK else 1


if __name__ == '__main__':
    sys.exit(main())
