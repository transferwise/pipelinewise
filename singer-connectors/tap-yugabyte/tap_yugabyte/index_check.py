"""Check a tap-yugabyte YAML against the source, and report the indexes it needs.

Service owners choose a replication method and key per table. Those choices imply
index requirements that YugabyteDB does not enforce and that nothing in a normal
run reports: a missing one is not an error, it is a full table scan on every
sync. This reads the config the owner already wrote, checks each table against
the live source, and prints the DDL to close the gap.

    tap-yugabyte-index-check tap_yugabyte.yml --host h --port 5433 --user u --dbname d

PostgreSQL and Snowflake targets load FULL_TABLE tables and LOG_BASED initial
snapshots through FastSync, which reads no keyset index. Pass
`--target-type target-s3-csv` for a target without FastSync, where the tap scans
FULL_TABLE tables itself.

Exit status is 0 when nothing needs doing, 1 when any table needs an index or has
a configuration that cannot work.
"""

import argparse
import os

import psycopg2
import yaml

from tap_yugabyte import keyset
from tap_yugabyte import parse_keyset_buckets

OK, ACTION, BLOCK = 'OK', 'ACTION', 'BLOCK'

# PipelineWise's own default for tap-yugabyte tables that name no method
# (pipelinewise/cli/tap_properties.py); the tap YAML has no key to override it.
DEFAULT_REPLICATION_METHOD = 'LOG_BASED'

# Targets with a tap-yugabyte FastSync route (pipelinewise/cli/fastsync_capabilities.py).
FASTSYNC_TARGET_TYPES = ('target-postgres', 'target-snowflake')
TARGET_TYPES = FASTSYNC_TARGET_TYPES + ('target-s3-csv',)


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

    Required by FULL_TABLE when the tap scans the table itself, on a target
    without FastSync, for every table with a primary key, whatever the
    sharding. INCREMENTAL on the primary key reuses it. A range-sharded key
    used to be reported OK with nothing to do; the tap has no code path that
    exploits it, so that OK predicted a parallel resumable sync the operator
    never got. plan_keyset_strategy says which sharding it is and why the
    index is needed either way.
    """
    plan = keyset.plan_keyset_strategy(cur, schema_name, table_name,
                                       pk_columns, pk_types, buckets)
    if not plan['index_required']:
        return BLOCK, plan['blockers'], []

    name = keyset.index_name(table_name)
    found, problems = keyset.check_index(cur, schema_name, table_name, name,
                                         pk_columns, pk_columns, buckets)
    if found is not None and not problems:
        return OK, [f'{name} supplies the bucket order the parallel scan pages '
                    f'along, {buckets} buckets over {found["tablets"]} '
                    f'tablets.'], []
    notes = [plan['note']]
    notes.extend(problems)
    return ACTION, notes, _repair_ddl(schema_name, found, plan['index_ddl'])


def check_full_table(cur, schema_name, table_name, buckets):
    """A Singer FULL_TABLE scan pages by primary key, so it needs the key to be
    scannable in order."""
    pk_columns, pk_types = _primary_key(cur, schema_name, table_name)
    return _pk_index(cur, schema_name, table_name, pk_columns, pk_types, buckets)


def check_fastsync_table(method):
    """FastSync copies FULL_TABLE tables and LOG_BASED initial snapshots with a plain COPY."""
    if method == 'FULL_TABLE':
        return OK, ['FastSync copies this table on every run with a plain COPY, which reads no keyset index.'], []
    return OK, ['FastSync loads the initial snapshot with a plain COPY, and the change stream reads no index.',
                'The table needs a replica identity other than NOTHING; PostgreSQL targets need DEFAULT or FULL.'], []


def check_incremental(cur, schema_name, table_name, replication_key, buckets,
                      run_limit=None):
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

    # No index fixes a tie group -- the bookmark is one scalar and the resume is
    # `>=`, so a group larger than one run can drain never lets the bookmark
    # move. With `limit` known that is provable rather than likely, and a
    # configuration that provably cannot make progress is a BLOCK even though
    # there is no DDL to print for it.
    ties = keyset.measure_tie_groups(cur, schema_name, table_name,
                                     replication_key, run_limit)
    notes.extend(ties['risks'])
    if ties['severity'] == 'livelock':
        status = BLOCK

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
                     f'streams in order. A resume still rereads every row that shares '
                     f'the bookmarked value.')
    else:
        notes.extend(problems)
        fq_table_name = f'"{schema_name}"."{table_name}"'
        colocated = keyset.table_is_colocated(cur, schema_name, table_name)
        ddl.extend(_repair_ddl(schema_name, found, keyset.replication_key_index_ddl(
            fq_table_name, table_name, replication_key, pk_columns, buckets, colocated)))
        if status != BLOCK:
            status = ACTION

    if key.get('tiebreaker'):
        notes.append(f"{replication_key} is not unique; the index ends in "
                     f"{', '.join(key['tiebreaker'])} so rows sharing a value keep "
                     f"a stable order across runs.")
    return status, notes, ddl


def table_method(table):
    return (table.get('replication_method') or DEFAULT_REPLICATION_METHOD).upper()


def check_table(cur, schema_name, table, buckets, run_limit=None, fastsync=False):
    """Check one table for the route it runs on: `fastsync` when the target has a FastSync route."""
    method = table_method(table)
    table_name = table['table_name']
    if method in ('FULL_TABLE', 'LOG_BASED') and fastsync:
        return check_fastsync_table(method)
    if method == 'FULL_TABLE':
        return check_full_table(cur, schema_name, table_name, buckets)
    if method == 'INCREMENTAL':
        return check_incremental(cur, schema_name, table_name,
                                 table.get('replication_key'), buckets, run_limit)
    if method == 'LOG_BASED':
        return BLOCK, ['LOG_BASED needs FastSync to create its replication slot, and this target has no '
                       'FastSync route. PipelineWise rejects it. Use FULL_TABLE or INCREMENTAL.'], []
    return BLOCK, [f'Unknown replication_method {method!r}.'], []


def tap_settings(config, buckets=None, limit=None):
    """The bucket count and row limit the tap will run with: the flag when given,
    otherwise what the tap YAML passes the tap under db_conn.

    Checking against a bucket count the tap is not configured for reports every
    correct index as wrong, so the YAML is the default rather than a constant.
    """
    db_conn = config.get('db_conn') or {}
    if buckets is None:
        configured = db_conn.get('keyset_buckets')
        buckets = (parse_keyset_buckets(configured) if configured is not None
                   else keyset.BUCKETS_DEFAULT)
    if limit is None and db_conn.get('limit'):
        limit = int(db_conn['limit'])
    return buckets, limit


def worse(current, status):
    if BLOCK in (current, status):
        return BLOCK
    if ACTION in (current, status):
        return ACTION
    return OK


def run(config, cur, buckets, limit, fastsync=True):
    """Check every table in a tap YAML and print the report; return the worst status."""
    all_ddl, worst = [], OK
    iceberg = config.get('target_table_format') == 'iceberg'
    for schema in config.get('schemas') or []:
        schema_name = schema['source_schema']
        for table in schema.get('tables') or []:
            if iceberg:
                status, notes, ddl = BLOCK, ['PipelineWise does not support YugabyteDB with managed Iceberg '
                                             'targets yet. Remove target_table_format or set it to "native".'], []
            else:
                status, notes, ddl = check_table(cur, schema_name, table, buckets, limit, fastsync)
            key = table.get('replication_key')
            heading = f"{schema_name}.{table['table_name']}  [{table_method(table)}"
            heading += f" on {key}]" if key else "]"
            print(f'\n{status:6s} {heading}')
            for note in notes:
                print(f'       - {note}')
            for statement in ddl:
                print(f'       $ {statement}')
            all_ddl.extend(ddl)
            worst = worse(worst, status)

    if all_ddl:
        print('\n' + '=' * 72 + '\nAll DDL, to hand to the service owner:\n')
        for statement in all_ddl:
            print(f'  {statement}')
    print(f'\nWorst status: {worst}')
    return worst


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config', help='tap YAML the service owner maintains')
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', type=int, default=5433)
    parser.add_argument('--user', required=True)
    parser.add_argument('--dbname', required=True)
    parser.add_argument('--password', default=os.environ.get('PGPASSWORD'))
    parser.add_argument('--buckets', type=int, default=None,
                        help='keyset_buckets the tap is configured for; defaults to '
                             'db_conn.keyset_buckets in the YAML, then 3')
    parser.add_argument('--limit', type=int, default=None,
                        help='`limit` the tap is configured for, which caps the '
                             'rows one INCREMENTAL run reads; defaults to '
                             'db_conn.limit in the YAML. Given it, the tie-group '
                             'check is exact rather than a heuristic: a group of '
                             'at least this many rows can never advance the bookmark')
    parser.add_argument('--target-type', choices=TARGET_TYPES, default=None,
                        help='type of the target the tap loads into. PostgreSQL and Snowflake, '
                             'the default, load FULL_TABLE and LOG_BASED tables through FastSync, '
                             'which needs no keyset index; target-s3-csv has no FastSync route')
    args = parser.parse_args(argv)

    with open(args.config, encoding='utf-8') as handle:
        config = yaml.safe_load(handle) or {}
    buckets, limit = tap_settings(config, args.buckets, args.limit)

    conn = psycopg2.connect(host=args.host, port=args.port, user=args.user,
                            dbname=args.dbname, password=args.password)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            worst = run(config, cur, buckets, limit, args.target_type != 'target-s3-csv')
    finally:
        conn.close()
    return 0 if worst == OK else 1
