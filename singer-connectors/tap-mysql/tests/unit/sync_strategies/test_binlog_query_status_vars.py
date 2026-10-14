"""MariaDB Query event status variables unknown to the pinned binlog decoder."""

import struct
from types import SimpleNamespace

import pytest
from pymysql.protocol import MysqlPacket
from pymysqlreplication.constants import QUERY_EVENT
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.packet import BinLogPacketWrapper

from tap_mysql.sync_strategies import binlog

FLAGS2_SQL_MODE_CATALOG = '00 00000001 01 0000205400000000 06 03 737464'
CHARSET_DATABASE_XID = '04 000900090009 08 0009 81 6b01000000000000'
QUERY = b'CREATE TABLE t (id INT) DEFAULT CHARSET=utf8mb4'


def query_event(status_vars, schema=b'app', query=QUERY):
    """Decode one Query event through the pinned mysql-replication packet wrapper."""
    body = struct.pack('<IIBHH', 7, 0, len(schema), 0, len(status_vars)) + status_vars + schema + b'\x00' + query
    header = struct.pack('<cIBIIIH', b'\x00', 0, QUERY_EVENT, 1, 19 + len(body), 0, 0)
    return BinLogPacketWrapper(
        MysqlPacket(header + body, 'utf8'), table_map={}, ctl_connection=SimpleNamespace(charset='utf8'),
        mysql_version=(11, 4, 0), use_checksum=False, allowed_events=frozenset([QueryEvent]),
        only_tables=None, ignored_tables=None, only_schemas=None, ignored_schemas=None, freeze_schema=False,
        fail_on_table_metadata_unavailable=False, ignore_decode_errors=False, verify_checksum=False,
    ).event


@pytest.mark.parametrize('collations', [
    # Captured from MariaDB 11.4 and 11.8 default @@character_set_collations.
    '83 01 2d000009',
    '83 05 21000008 2300000a 2d000009 3600000b 3c00000c',
])
def test_mariadb_character_set_collations_keep_query_aligned(collations):
    event = query_event(bytes.fromhex(f'{FLAGS2_SQL_MODE_CATALOG} {collations} {CHARSET_DATABASE_XID}'))

    assert (event.schema, event.query) == (b'app', QUERY.decode())
    assert (event.sql_mode, event.catalog_nz_code) == (0x54200000, b'std')


@pytest.mark.parametrize('tail', ['82 04 0900000000000000', '82 02', 'ff ff ff'])
def test_unknown_trailing_status_vars_are_skipped(tail):
    event = query_event(bytes.fromhex(f'{FLAGS2_SQL_MODE_CATALOG} {CHARSET_DATABASE_XID} {tail}'))

    assert (event.schema, event.query, event.xid) == (b'app', QUERY.decode(), 0x16b)


def test_known_status_vars_still_use_pinned_decoder():
    event = query_event(bytes.fromhex(f'{FLAGS2_SQL_MODE_CATALOG} {CHARSET_DATABASE_XID}'), schema=b'', query=b'BEGIN')

    assert (event.schema, event.query, event.charset_database_number, event.xid) == (b'', 'BEGIN', 0x900, 0x16b)


def test_status_var_patch_is_applied_once():
    patched = QueryEvent._read_status_vars_value_for_key

    binlog.skip_unknown_query_status_vars()

    assert QueryEvent._read_status_vars_value_for_key is patched
