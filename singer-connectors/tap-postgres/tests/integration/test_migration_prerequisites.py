"""Exercise migration prerequisites against a real source without shared ACL changes."""

import uuid

from psycopg2 import sql
import pytest

from tap_postgres import db
from tap_postgres.sync_strategies import full_table, logical_replication
from tests.utils import get_test_connection, get_test_connection_config


def test_revoked_message_permission_requires_a_grant_before_any_migration_work():
    name = f'ppw_permission_{uuid.uuid4().hex[:12]}'
    config = get_test_connection_config(target_db=name)
    config.update(user=name, password=name, tap_id=name)
    admin = get_test_connection(superuser=True)
    isolated = None
    try:
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL('CREATE ROLE {} LOGIN REPLICATION PASSWORD %s').format(
                sql.Identifier(name)), (name,))
            cursor.execute(sql.SQL('CREATE DATABASE {} OWNER {}').format(sql.Identifier(name), sql.Identifier(name)))
        isolated = get_test_connection(target_db=name, superuser=True)
        with isolated.cursor() as cursor:
            cursor.execute("""
                SELECT COALESCE(
                    to_regprocedure('pg_catalog.pg_logical_emit_message(boolean,text,text,boolean)'),
                    to_regprocedure('pg_catalog.pg_logical_emit_message(boolean,text,text)')
                )::text
            """)
            signature = cursor.fetchone()[0]
            cursor.execute(sql.SQL('REVOKE EXECUTE ON FUNCTION {} FROM PUBLIC').format(sql.SQL(signature)))

        stream = {'tap_stream_id': 'public-unused', 'table_name': 'unused', 'metadata': [
            {'breadcrumb': [], 'metadata': {'schema-name': 'public'}},
        ]}
        with pytest.raises(RuntimeError, match='requires EXECUTE'):
            logical_replication.prepare_publication(config, [stream])
        with pytest.raises(RuntimeError, match='requires EXECUTE'):
            db.capture_snapshot_boundary(config)
        with isolated.cursor() as cursor:
            cursor.execute('SELECT count(*) FROM pg_publication')
            assert cursor.fetchone()[0] == 0
            cursor.execute('SELECT count(*) FROM pg_replication_slots WHERE database = %s', (name,))
            assert cursor.fetchone()[0] == 0
            cursor.execute(sql.SQL('GRANT EXECUTE ON FUNCTION {} TO {}').format(
                sql.SQL(signature), sql.Identifier(name)))
        assert db.capture_snapshot_boundary(config) > 0
    finally:
        if isolated is not None:
            isolated.close()
        with admin.cursor() as cursor:
            cursor.execute(sql.SQL('DROP DATABASE IF EXISTS {}').format(sql.Identifier(name)))
            cursor.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(name)))
        admin.close()


def test_primary_endpoint_cannot_masquerade_as_a_snapshot_secondary():
    config = get_test_connection_config()
    config.update(use_secondary=True, secondary_host=config['host'], secondary_port=config['port'])
    state = {}
    with pytest.raises(logical_replication.ReplicationSlotMigrationError, match='secondary is not in recovery'):
        full_table.sync_table(config, {'tap_stream_id': 'public-unused'}, state, [], {}, snapshot_lsn=1)
    assert state == {}
