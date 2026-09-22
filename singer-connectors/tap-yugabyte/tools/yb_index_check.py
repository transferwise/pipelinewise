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


def check_full_table(cur, schema_name, table_name, buckets):
    """FULL_TABLE pages by primary key, so it needs the key to be scannable in order."""
    pk_columns, pk_types = _primary_key(cur, schema_name, table_name)
    plan = keyset.plan_keyset_strategy(cur, schema_name, table_name,
                                       pk_columns, pk_types, buckets)
    if plan['strategy'] == keyset.STRATEGY_PK_RANGE:
        return OK, ['Primary key is range-sharded, so it already pages in key '
                    'order. Adding an index here would only cost writes.'], []
    if plan['strategy'] == keyset.STRATEGY_BUCKET_INDEX:
        present, reason = keyset.validate_index(cur, schema_name, table_name,
                                                pk_columns, buckets)
        if present:
            return OK, [f'Primary key is hash-sharded, and '
                        f'{keyset.index_name(table_name)} already supplies the '
                        f'order it lacks.'], []
        return ACTION, [
            'Primary key is hash-sharded, so it has no order to page along. '
            'Without this index every resume re-reads the whole table.',
            reason.splitlines()[0] if reason else '',
        ], [plan['index_ddl'] + ';']
    return BLOCK, plan['blockers'], []


def check_incremental(cur, schema_name, table_name, replication_key, buckets):
    """INCREMENTAL pages by the replication key, so the key -- not the primary
    key -- is what has to be indexed, non-null and actually increasing."""
    if not replication_key:
        return BLOCK, ['replication_method is INCREMENTAL but no replication_key '
                       'is set.'], []

    pk_columns, _ = _primary_key(cur, schema_name, table_name)
    notes, ddl, status = [], [], OK

    key = keyset.require_monotonic_key(cur, schema_name, table_name,
                                       replication_key, pk_columns)
    if key['hard_failures']:
        status = BLOCK
        notes.extend(key['hard_failures'])
    notes.extend(key['risks'])

    # the bucketed index is what INCREMENTAL wants, and it does not lead with the
    # key -- the discriminator does -- so no column-name lookup will find it
    cur.execute(
        "SELECT pg_get_indexdef(i.oid) FROM pg_index x "
        'JOIN pg_class i ON i.oid = x.indexrelid '
        'JOIN pg_class c ON c.oid = x.indrelid '
        'JOIN pg_namespace n ON n.oid = c.relnamespace '
        'WHERE n.nspname = %s AND c.relname = %s AND i.relname = %s',
        (schema_name, table_name,
         keyset.replication_key_index_name(table_name, replication_key)))
    bucketed = cur.fetchone()

    if bucketed:
        found = keyset.parse_index_buckets(bucketed[0])
        notes.append(f'{replication_key} is bucketed by '
                     f'{keyset.replication_key_index_name(table_name, replication_key)} '
                     f'({found} buckets), so the watermark query bounds itself and the '
                     f'write tail is spread across {found} tablets.')
    else:
        served = keyset.plan_partial_sync(cur, schema_name, table_name, replication_key)
        if served['indexed']:
            notes.append(f"{replication_key} is ordered by {served['index']}, which bounds "
                         f'the watermark query but puts every insert on one tablet: a '
                         f'replication key only ever increases, so it lands at the tail.')
        else:
            notes.append(f'No index bounds {replication_key}, so every run scans the whole '
                         f'table to find its rows.')
        if status != BLOCK:
            status = ACTION
        fq_table_name = f'"{schema_name}"."{table_name}"'
        ddl.append(keyset.replication_key_index_ddl(
            fq_table_name, table_name, replication_key, buckets) + ';')

    if key.get('tiebreaker'):
        notes.append(f"{replication_key} is not unique; rows sharing a value need "
                     f"{', '.join(key['tiebreaker'])} to break the tie deterministically.")
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
        return OK, ['LOG_BASED reads the change stream, so it needs no index. '
                    'It does need the table to have a replica identity that can '
                    'name a deleted row.'], []
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
