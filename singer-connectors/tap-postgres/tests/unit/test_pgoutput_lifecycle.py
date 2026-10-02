import copy
import datetime
import json
from collections import namedtuple
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from tap_postgres.sync_strategies import logical_replication


WalMessage = namedtuple('WalMessage', ['payload', 'data_start'])


def _slot(plugin, database='source', active=False, lsn='0/64'):
    return {
        'plugin': plugin,
        'slot_type': 'logical',
        'database': database,
        'active': active,
        'confirmed_flush_lsn': lsn,
    }


def _stream():
    return {
        'tap_stream_id': 'public-payments',
        'stream': 'payments',
        'table_name': 'payments',
        'schema': {'properties': {'id': {'type': ['null', 'integer']}}},
        'metadata': [{
            'breadcrumb': [],
            'metadata': {'schema-name': 'public', 'database-name': 'source'},
        }],
    }


def _keyed_stream():
    return {
        'tap_stream_id': 'public-payments',
        'stream': 'payments',
        'table_name': 'payments',
        'schema': {
            'type': 'object',
            'properties': {
                'id': {'type': ['null', 'integer']},
                'description': {'type': ['null', 'string']},
            },
        },
        'metadata': [
            {
                'breadcrumb': [],
                'metadata': {
                    'schema-name': 'public',
                    'table-key-properties': ['id'],
                },
            },
            {
                'breadcrumb': ['properties', 'id'],
                'metadata': {
                    'inclusion': 'automatic',
                    'sql-datatype': 'integer',
                },
            },
            {
                'breadcrumb': ['properties', 'description'],
                'metadata': {
                    'selected': True,
                    'sql-datatype': 'text',
                },
            },
        ],
    }


def _config(**overrides):
    config = {
        'host': 'source',
        'port': 5432,
        'user': 'tap',
        'password': 'secret',
        'dbname': 'source',
        'tap_id': 'orders',
        'use_secondary': False,
        'max_run_seconds': 60,
        'logical_poll_total_seconds': 60,
        'break_at_end_lsn': True,
    }
    config.update(overrides)
    return config


def _state(lsn=100, migration=None):
    state = {
        'currently_syncing': None,
        'bookmarks': {'public-payments': {'lsn': lsn, 'version': 1}},
    }
    if migration is not None:
        state[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY] = migration
    return state


def test_slot_copy_uses_canonical_pgoutput_name_and_preserves_source():
    cursor = Mock()
    cursor.fetchone.return_value = ('pipelinewise_orders', '0/64')
    rows = {
        'pipelinewise_source': _slot('wal2json'),
        'pipelinewise_source_orders': _slot('wal2json'),
    }

    with patch.object(logical_replication, '_slot_rows', return_value=rows):
        result = logical_replication.locate_replication_slot_by_cur(
            cursor, 'source', 'orders')

    assert result == 'pipelinewise_orders'
    assert result.migration_source == 'pipelinewise_source_orders'
    assert result.migration_lsn == 100
    assert result.confirmed_flush_lsn == 100
    assert cursor.execute.call_args.args[1] == (
        'pipelinewise_source_orders', 'pipelinewise_orders', 'pgoutput')


def test_slot_resolution_rejects_database_wide_legacy_slot_before_copy():
    rows = {'pipelinewise_source': _slot('wal2json')}
    with patch.object(logical_replication, '_slot_rows', return_value=rows), \
            pytest.raises(
                logical_replication.ReplicationSlotMigrationError,
                match='database-wide slot pipelinewise_source may be shared'):
        cursor = Mock()
        logical_replication.locate_replication_slot_by_cur(cursor, 'source', 'orders')

    cursor.execute.assert_not_called()


def test_existing_canonical_slot_ignores_unrelated_database_wide_slot():
    rows = {
        'pipelinewise_orders': _slot('pgoutput', lsn='0/7B'),
        'pipelinewise_source': _slot('wal2json'),
    }

    with patch.object(logical_replication, '_slot_rows', return_value=rows):
        cursor = Mock()
        result = logical_replication.locate_replication_slot_by_cur(
            cursor, 'source', 'orders')

    assert result == 'pipelinewise_orders'
    assert result.migration_source is None
    assert result.confirmed_flush_lsn == 123
    cursor.execute.assert_not_called()


def test_partition_root_rejects_tap_specific_wal2json_migration_before_copy():
    rows = {'pipelinewise_source_orders': _slot('wal2json')}

    with patch.object(logical_replication, '_slot_rows', return_value=rows), \
            pytest.raises(
                logical_replication.ReplicationSlotMigrationError,
                match='partition root.*full resync'):
        cursor = Mock()
        logical_replication.locate_replication_slot_by_cur(
            cursor,
            'source',
            'orders',
            allow_wal2json_migration=False,
        )

    cursor.execute.assert_not_called()


def test_postgres14_partition_parent_uses_root_publication_and_leaf_wal2json_aliases():
    cursor = Mock()
    cursor.fetchall.side_effect = [
        [('public', 'events', 'p', 'public', 'events')],
        [
            ('public', 'events', 'public', 'events_2025', 'r'),
            ('public', 'events', 'archive', 'events_2024', 'r'),
        ],
    ]

    result = logical_replication._validate_publication_tables(
        cursor,
        [('public', 'events')],
        140000,
    )

    publication_tables, has_roots, wal_tables, wal_aliases, pgoutput_aliases = result
    expected_leaves = [('archive', 'events_2024'), ('public', 'events_2025')]
    assert publication_tables == [('public', 'events')]
    assert wal_tables == expected_leaves
    assert has_roots is True
    assert wal_aliases == {leaf: ('public', 'events') for leaf in expected_leaves}
    assert pgoutput_aliases == {}


def test_postgres14_allows_a_standalone_leaf_partition():
    cursor = Mock()
    cursor.fetchall.side_effect = [
        [('public', 'events_2025', 'r', 'public', 'events')],
        [],
    ]

    publication_tables, has_roots, *_ = logical_replication._validate_publication_tables(
        cursor, [('public', 'events_2025')], 140000)

    assert publication_tables == [('public', 'events_2025')]
    assert has_roots is False


def test_partition_root_rejects_unsupported_descendant_relation_kind():
    cursor = Mock()
    cursor.fetchall.side_effect = [
        [('public', 'events', 'p', 'public', 'events')],
        [('public', 'events', 'foreign', 'events_external', 'f')],
    ]

    with pytest.raises(
            logical_replication.ReplicationSlotMigrationError,
            match='persistent and partitioned descendants'):
        logical_replication._validate_publication_tables(
            cursor, [('public', 'events')], 140000)


def test_ordinary_table_with_inheritance_descendant_is_rejected():
    cursor = Mock()
    cursor.fetchall.side_effect = [
        [('public', 'events', 'r', 'public', 'events')],
        [('public', 'events', 'public', 'events_child', 'r')],
    ]

    with pytest.raises(
            logical_replication.ReplicationSlotMigrationError,
            match='ordinary tables with inheritance descendants'):
        logical_replication._validate_publication_tables(
            cursor, [('public', 'events')], 140000)


def test_partition_root_preflight_rejects_tap_specific_wal2json_slot():
    source = 'pipelinewise_source_orders'
    with patch.object(
            logical_replication,
            '_slot_rows',
            return_value={source: _slot('wal2json')}), \
            pytest.raises(
                logical_replication.ReplicationSlotMigrationError,
                match='partition root.*full resync'):
        logical_replication._validate_replication_slot_candidates(
            Mock(), 'source', 'orders', allow_wal2json_migration=False)


def test_publication_preflight_rejects_database_wide_slot_when_it_is_only_candidate():
    with patch.object(
            logical_replication,
            '_slot_rows',
            return_value={'pipelinewise_source': _slot('wal2json')}), \
            pytest.raises(
                logical_replication.ReplicationSlotMigrationError,
                match='database-wide slot pipelinewise_source may be shared'):
        logical_replication._validate_replication_slot_candidates(
            Mock(), 'source', 'orders')


@pytest.mark.parametrize('usable_slot,plugin', [
    ('pipelinewise_orders', 'pgoutput'),
    ('pipelinewise_source_orders', 'wal2json'),
])
def test_publication_preflight_ignores_unrelated_database_wide_slot(
        usable_slot, plugin):
    rows = {
        'pipelinewise_source': _slot('wal2json'),
        usable_slot: _slot(plugin),
    }
    with patch.object(logical_replication, '_slot_rows', return_value=rows):
        logical_replication._validate_replication_slot_candidates(
            Mock(), 'source', 'orders')


@pytest.mark.parametrize(('slot_name', 'slot', 'error'), [
    (
        'pipelinewise_orders',
        _slot('wal2json'),
        'uses wal2json, expected pgoutput',
    ),
    (
        'pipelinewise_orders',
        _slot('pgoutput', database='other'),
        'logical slot for database source',
    ),
    (
        'pipelinewise_orders',
        _slot('pgoutput', active=True),
        'already active',
    ),
    (
        'pipelinewise_source_orders',
        _slot('pgoutput'),
        'uses pgoutput, expected wal2json',
    ),
    (
        'pipelinewise_source_orders',
        _slot('wal2json', database='other'),
        'logical slot for database source',
    ),
    (
        'pipelinewise_source_orders',
        _slot('wal2json', active=True),
        'already active',
    ),
])
def test_publication_preflight_validates_owned_slot_candidates(
        slot_name, slot, error):
    with patch.object(logical_replication, '_slot_rows', return_value={slot_name: slot}), \
            pytest.raises(logical_replication.ReplicationSlotMigrationError, match=error):
        logical_replication._validate_replication_slot_candidates(
            Mock(), 'source', 'orders')


def test_prepare_publication_rejects_invalid_slot_before_publication_ddl():
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.server_version = 140018
    cursor = connection.cursor.return_value.__enter__.return_value

    with patch.object(
            logical_replication.post_db,
            'open_connection',
            return_value=connection), \
            patch.object(logical_replication, '_reject_selected_generated_columns'), \
            patch.object(
                logical_replication,
                '_validate_publication_tables',
                return_value=(
                    [('public', 'payments')], False,
                    [('public', 'payments')], {}, {},
                )), \
            patch.object(
                logical_replication,
                '_slot_rows',
                return_value={'pipelinewise_orders': _slot('wal2json')}), \
            patch.object(logical_replication, '_validate_replica_identity') as identity, \
            pytest.raises(
                logical_replication.ReplicationSlotMigrationError,
                match='uses wal2json, expected pgoutput'):
        logical_replication.prepare_publication(_config(), [_stream()])

    identity.assert_not_called()
    assert all('PUBLICATION' not in str(item.args[0]) for item in cursor.execute.call_args_list)


def test_selected_generated_column_is_rejected_before_slot_work():
    cursor = Mock()
    cursor.fetchall.return_value = [('public', 'payments', 'total')]
    stream = _stream()
    stream['schema']['properties']['total'] = {'type': ['null', 'number']}

    with patch.object(logical_replication.sync_common, 'should_sync_column', return_value=True), \
            pytest.raises(logical_replication.ReplicationSlotMigrationError, match='generated columns'):
        logical_replication._reject_selected_generated_columns(cursor, [stream])


def test_replica_identity_accepts_default_with_matching_primary_key():
    cursor = Mock()
    cursor.fetchall.return_value = [
        ('public', 'payments', 'd', True, ['account_id', 'payment_id']),
    ]

    logical_replication._validate_replica_identity(
        cursor,
        [('public', 'payments')],
        {('public', 'payments'): ('payment_id', 'account_id')},
    )


@pytest.mark.parametrize('replica_identity,has_primary_key', [
    ('n', True),
    ('i', True),
    ('f', True),
    ('d', False),
])
def test_replica_identity_rejects_non_default_or_invalid_primary_key(
        replica_identity, has_primary_key):
    cursor = Mock()
    cursor.fetchall.return_value = [
        ('public', 'payments', replica_identity, has_primary_key, ['id']),
    ]

    with pytest.raises(
            logical_replication.ReplicationSlotMigrationError,
            match='REPLICA IDENTITY DEFAULT backed by a valid non-deferrable primary key'):
        logical_replication._validate_replica_identity(
            cursor,
            [('public', 'payments')],
            {('public', 'payments'): ('id',)},
        )


def test_replica_identity_rejects_primary_key_that_differs_from_catalog_merge_key():
    cursor = Mock()
    cursor.fetchall.return_value = [
        ('public', 'payments', 'd', True, ['source_id']),
    ]

    with pytest.raises(
            logical_replication.ReplicationSlotMigrationError,
            match='source row identity matches the Singer target merge key'):
        logical_replication._validate_replica_identity(
            cursor,
            [('public', 'payments')],
            {('public', 'payments'): ('catalog_id',)},
        )


def test_pgoutput_primary_key_update_emits_old_key_delete_before_new_row():
    payload = {
        'action': 'U',
        'schema': 'public',
        'table': 'payments',
        'columns': [
            {'name': 'id', 'value': '2'},
            {'name': 'description', 'value': 'moved'},
        ],
        'identity': [{'name': 'id', 'value': '1'}],
        'relation_columns': [
            {'name': 'id', 'is_key': True},
            {'name': 'description', 'is_key': False},
        ],
        'transaction_lsn': 101,
        '_pgoutput': True,
    }
    state = {'bookmarks': {'public-payments': {'version': 1, 'lsn': 100}}}

    with patch.object(logical_replication.singer, 'write_message') as write_message:
        logical_replication.consume_message(
            [_keyed_stream()],
            state,
            WalMessage(payload=b'', data_start=101),
            datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            {'debug_lsn': False},
            message_payload=payload,
        )

    assert [message.args[0].record for message in write_message.call_args_list] == [
        {'id': 1, '_sdc_deleted_at': '2026-01-01T00:00:00+00:00'},
        {'id': 2, 'description': 'moved', '_sdc_deleted_at': None},
    ]


def test_pgoutput_relation_identity_must_match_singer_merge_key():
    payload = {
        'action': 'D',
        'schema': 'public',
        'table': 'payments',
        'identity': [{'name': 'alternate_id', 'value': '1'}],
        'relation_columns': [
            {'name': 'id', 'is_key': False},
            {'name': 'alternate_id', 'is_key': True},
        ],
        'schema_changed': True,
        '_pgoutput': True,
    }

    with patch.object(logical_replication.singer, 'write_message') as write_message, \
            patch.object(logical_replication, 'refresh_streams_schema') as refresh, \
            pytest.raises(
                logical_replication.ReplicationSlotMigrationError,
                match='replica identity does not match the Singer target merge key'):
        logical_replication.consume_message(
            [_keyed_stream()],
            {'bookmarks': {'public-payments': {'version': 1, 'lsn': 100}}},
            WalMessage(payload=b'', data_start=101),
            datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            {'debug_lsn': False},
            message_payload=payload,
        )

    write_message.assert_not_called()
    refresh.assert_not_called()


def test_primary_key_update_with_unchanged_toast_fails_before_delete():
    payload = {
        'action': 'U',
        'columns': [{'name': 'id', 'value': '2'}],
        'identity': [{'name': 'id', 'value': '1'}],
        '_pgoutput': True,
    }

    with pytest.raises(
            logical_replication.ReplicationSlotMigrationError,
            match='omitted unchanged selected columns'):
        logical_replication._old_primary_key_for_changed_update(
            payload, ['id'], {'id', 'description'})


def test_publication_fence_comment_round_trips_original_comment():
    for original in (None, 'DBA-owned publication comment'):
        encoded = logical_replication._encode_publication_fence_comment('ready', original)
        assert encoded.startswith(logical_replication.PUBLICATION_FENCE_COMMENT_PREFIX)
        assert logical_replication._decode_publication_fence_comment(encoded) == ('ready', original)


def test_malformed_publication_fence_comment_fails_closed():
    malformed = logical_replication.PUBLICATION_FENCE_COMMENT_PREFIX + 'bnVsbA=='
    with pytest.raises(
            logical_replication.ReplicationSlotMigrationError,
            match='invalid pending transaction-fence comment'):
        logical_replication._decode_publication_fence_comment(malformed)


class _ReplicationConnection:
    server_version = 160000

    def __init__(self, messages):
        self.cursor_instance = Mock()
        self.cursor_instance.read_message.side_effect = messages

    def cursor(self):
        return self.cursor_instance

    def close(self):
        return None


def _downstream_delay(current_time):
    delayed = False

    def consume(_streams, state, *_args, **_kwargs):
        nonlocal delayed
        if not delayed:
            current_time[0] += datetime.timedelta(seconds=10)
            delayed = True
        return state

    return consume


def test_wal2json_bridge_idle_timeout_excludes_downstream_blocking_time():
    token = 'bridge-token'
    messages = [
        WalMessage(json.dumps({'action': 'I'}), 101),
        WalMessage(json.dumps({'action': 'B'}), 110),
        WalMessage(json.dumps({
            'action': 'M',
            'transactional': True,
            'prefix': 'pipelinewise_orders',
            'content': token,
        }), 111),
        WalMessage(json.dumps({'action': 'C'}), 120),
    ]
    connection = _ReplicationConnection(messages)
    publication = logical_replication.PreparedPublication(
        'pw_pub_orders',
        wal2json_tables=[('public', 'payments')],
    )
    current_time = [datetime.datetime(2026, 1, 1)]
    clock = Mock()
    clock.utcnow.side_effect = lambda: current_time[0]

    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, 'emit_boundary_message', return_value=token), \
            patch.object(logical_replication, 'consume_message', side_effect=_downstream_delay(current_time)), \
            patch.object(logical_replication.singer, 'write_message'), \
            patch.object(logical_replication.datetime, 'datetime', clock):
        result = logical_replication._bridge_wal2json_slot(
            _config(logical_poll_total_seconds=3),
            [_stream()],
            _state(),
            'state.json',
            publication,
            'pipelinewise_source_orders',
            'pipelinewise_orders',
            100,
            100,
            None,
        )

    assert connection.cursor_instance.read_message.call_count == len(messages)
    assert result[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY]['bridge_lsn'] == 120


def test_pgoutput_idle_timeout_excludes_downstream_blocking_time():
    token = 'boundary-token'
    messages = [
        WalMessage(json.dumps({'action': 'I'}), 101),
        WalMessage(json.dumps({'action': 'B'}), 110),
        WalMessage(json.dumps({
            'action': 'M',
            'transactional': True,
            'prefix': 'pipelinewise_orders',
            'content': token,
        }), 111),
        WalMessage(json.dumps({'action': 'C', 'end_lsn': 120}), 120),
    ]
    connection = _ReplicationConnection(messages)
    current_time = [datetime.datetime(2026, 1, 1)]
    clock = Mock()
    clock.utcnow.side_effect = lambda: current_time[0]

    with patch.object(
            logical_replication,
            'prepare_publication',
            return_value=logical_replication.PreparedPublication('pw_pub_orders')), \
            patch.object(
                logical_replication,
                'locate_replication_slot',
                return_value=logical_replication.PreparedReplicationSlot(
                    'pipelinewise_orders', confirmed_flush_lsn=100)), \
            patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, 'emit_boundary_message', return_value=token), \
            patch.object(logical_replication, 'consume_message', side_effect=_downstream_delay(current_time)), \
            patch.object(logical_replication.sync_common, 'send_schema_message'), \
            patch.object(logical_replication.singer, 'write_message'), \
            patch.object(logical_replication.datetime, 'datetime', clock):
        result = logical_replication.sync_tables(
            _config(logical_poll_total_seconds=3),
            [_stream()],
            _state(),
            90,
            'state.json',
        )

    assert connection.cursor_instance.read_message.call_count == len(messages)
    assert result['bookmarks']['public-payments']['lsn'] == 120


def test_wal2json_bridge_emits_phase_and_exits_without_waiting_for_target():
    token = 'bridge-token'
    messages = [
        WalMessage(json.dumps({'action': 'B'}), 101),
        WalMessage(json.dumps({
            'action': 'M',
            'transactional': True,
            'prefix': 'pipelinewise_orders',
            'content': token,
        }), 102),
        WalMessage(json.dumps({'action': 'C'}), 110),
    ]
    connection = _ReplicationConnection(messages)
    publication = logical_replication.PreparedPublication(
        'pw_pub_orders',
        wal2json_tables=[('public', 'payments')],
    )
    state = _state()

    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, 'emit_boundary_message', return_value=token), \
            patch.object(logical_replication, 'consume_message', side_effect=lambda _s, state, *_a, **_k: state), \
            patch.object(logical_replication.singer, 'write_message'):
        result = logical_replication._bridge_wal2json_slot(
            _config(),
            [_stream()],
            state,
            'state.json',
            publication,
            'pipelinewise_source_orders',
            'pipelinewise_orders',
            100,
            100,
            None,
        )

    assert result['bookmarks']['public-payments']['lsn'] == 110
    assert result[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY] == {
        'version': 1,
        'phase': 'bridge',
        'source_slot': 'pipelinewise_source_orders',
        'destination_slot': 'pipelinewise_orders',
        'copy_lsn': 100,
        'bridge_lsn': 110,
    }


def test_bridge_rejects_bookmark_before_legacy_confirmed_flush_lsn():
    publication = logical_replication.PreparedPublication(
        'pw_pub_orders', wal2json_tables=[('public', 'payments')])
    slot = logical_replication.PreparedReplicationSlot(
        'pipelinewise_orders', 'pipelinewise_source_orders', 100, 90)

    with patch.object(logical_replication, 'prepare_publication', return_value=publication), \
            patch.object(logical_replication, 'locate_replication_slot', return_value=slot), \
            patch.object(logical_replication, '_bridge_wal2json_slot') as bridge, \
            patch.object(logical_replication, '_validate_replica_identity_tables') as validate, \
            patch.object(logical_replication.sync_common, 'send_schema_message'), \
            pytest.raises(
                logical_replication.ReplicationSlotMigrationError,
                match='predates legacy slot.*full resync'):
        logical_replication.sync_tables(
            _config(), [_stream()], _state(lsn=99), 200, 'state.json')

    validate.assert_not_called()
    bridge.assert_not_called()


def test_pgoutput_rejects_bookmark_before_canonical_confirmed_flush_lsn():
    """A restored state file cannot ask PostgreSQL to replay discarded WAL."""
    publication = logical_replication.PreparedPublication('pw_pub_orders')
    slot = logical_replication.PreparedReplicationSlot(
        'pipelinewise_orders', confirmed_flush_lsn=101)

    with patch.object(
            logical_replication,
            'prepare_publication',
            return_value=publication), patch.object(
            logical_replication,
            'locate_replication_slot',
            return_value=slot), patch.object(
            logical_replication.post_db,
            'open_connection') as open_connection, patch.object(
            logical_replication.sync_common,
            'send_schema_message') as send_schema, pytest.raises(
            logical_replication.ReplicationSlotMigrationError,
            match='predates canonical pgoutput slot.*unfiltered whole-tap FastSync'):
        logical_replication.sync_tables(
            _config(), [_stream()], _state(lsn=100), 200, 'state.json')

    open_connection.assert_not_called()
    send_schema.assert_not_called()


def test_bridge_validates_physical_leaf_identity_before_consuming_wal2json():
    publication = logical_replication.PreparedPublication(
        'pw_pub_orders',
        wal2json_tables=[('public', 'payments_2026')],
        expected_primary_keys={('public', 'payments_2026'): ('id',)},
    )
    slot = logical_replication.PreparedReplicationSlot(
        'pipelinewise_orders', 'pipelinewise_source_orders', 100, 100)
    expected_state = _state()

    with patch.object(logical_replication, 'prepare_publication', return_value=publication), \
            patch.object(logical_replication, 'locate_replication_slot', return_value=slot), \
            patch.object(
                logical_replication,
                '_bridge_wal2json_slot',
                return_value=expected_state) as bridge, \
            patch.object(logical_replication, '_validate_replica_identity_tables') as validate, \
            patch.object(logical_replication.sync_common, 'send_schema_message'):
        result = logical_replication.sync_tables(
            _config(), [_stream()], _state(), 200, 'state.json')

    assert result == expected_state
    validate.assert_called_once_with(
        _config(),
        (('public', 'payments_2026'),),
        {('public', 'payments_2026'): ('id',)},
    )
    bridge.assert_called_once()


def test_bridge_rejects_primary_key_changed_after_publication_preflight():
    publication = logical_replication.PreparedPublication(
        'pw_pub_orders',
        wal2json_tables=[('public', 'payments')],
        expected_primary_keys={('public', 'payments'): ('id',)},
    )
    slot = logical_replication.PreparedReplicationSlot(
        'pipelinewise_orders', 'pipelinewise_source_orders', 100, 100)

    with patch.object(
            logical_replication, 'prepare_publication', return_value=publication), \
            patch.object(logical_replication, 'locate_replication_slot', return_value=slot), \
            patch.object(logical_replication, '_bridge_wal2json_slot') as bridge, \
            patch.object(
                logical_replication,
                '_validate_replica_identity_tables',
                side_effect=logical_replication.ReplicationSlotMigrationError(
                    'primary key changed')) as validate, \
            patch.object(logical_replication.sync_common, 'send_schema_message'), \
            pytest.raises(
                logical_replication.ReplicationSlotMigrationError,
                match='primary key changed'):
        logical_replication.sync_tables(
            _config(), [_stream()], _state(), 200, 'state.json')

    validate.assert_called_once_with(
        _config(),
        (('public', 'payments'),),
        {('public', 'payments'): ('id',)},
    )
    bridge.assert_not_called()


def test_pgoutput_boundary_after_bridge_marks_retire_without_business_dml():
    token = 'boundary-token'
    migration = {
        'version': 1,
        'phase': 'pgoutput',
        'source_slot': 'pipelinewise_source_orders',
        'destination_slot': 'pipelinewise_orders',
        'copy_lsn': 100,
        'bridge_lsn': 200,
    }
    messages = [
        WalMessage(json.dumps({'action': 'B'}), 201),
        WalMessage(json.dumps({
            'action': 'M',
            'transactional': True,
            'prefix': 'pipelinewise_orders',
            'content': token,
        }), 252),
        WalMessage(json.dumps({'action': 'C', 'end_lsn': 260}), 260),
    ]
    connection = _ReplicationConnection(messages)
    slot = logical_replication.PreparedReplicationSlot(
        'pipelinewise_orders', 'pipelinewise_source_orders', 100, 100)
    publication = logical_replication.PreparedPublication('pw_pub_orders')

    with patch.object(logical_replication, 'prepare_publication', return_value=publication), \
            patch.object(logical_replication, 'locate_replication_slot', return_value=slot), \
            patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, 'emit_boundary_message', return_value=token), \
            patch.object(logical_replication, 'consume_message', side_effect=lambda _s, state, *_a, **_k: state), \
            patch.object(logical_replication.sync_common, 'send_schema_message'), \
            patch.object(logical_replication.singer, 'write_message'):
        result = logical_replication.sync_tables(
            _config(), [_stream()], _state(migration=copy.deepcopy(migration)), 190, 'state.json')

    assert result[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY]['phase'] == 'retire'
    assert result[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY]['retire_lsn'] == 260
    assert result['bookmarks']['public-payments']['lsn'] == 260


def test_boundary_at_bridge_lsn_does_not_mark_migration_retire():
    token = 'boundary-token'
    migration = {
        'version': 1,
        'phase': 'pgoutput',
        'source_slot': 'pipelinewise_source_orders',
        'destination_slot': 'pipelinewise_orders',
        'copy_lsn': 100,
        'bridge_lsn': 200,
    }
    messages = [
        WalMessage(json.dumps({'action': 'B'}), 199),
        WalMessage(json.dumps({
            'action': 'M',
            'transactional': True,
            'prefix': 'pipelinewise_orders',
            'content': token,
        }), 199),
        WalMessage(json.dumps({'action': 'C', 'end_lsn': 200}), 200),
    ]
    connection = _ReplicationConnection(messages)
    slot = logical_replication.PreparedReplicationSlot(
        'pipelinewise_orders', 'pipelinewise_source_orders', 100, 100)

    with patch.object(
            logical_replication,
            'prepare_publication',
            return_value=logical_replication.PreparedPublication('pw_pub_orders')), \
            patch.object(logical_replication, 'locate_replication_slot', return_value=slot), \
            patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, 'emit_boundary_message', return_value=token), \
            patch.object(
                logical_replication,
                'consume_message',
                side_effect=lambda _s, state, *_a, **_k: state), \
            patch.object(logical_replication.sync_common, 'send_schema_message'), \
            patch.object(logical_replication.singer, 'write_message'):
        result = logical_replication.sync_tables(
            _config(), [_stream()], _state(migration=copy.deepcopy(migration)), 190, 'state.json')

    assert result[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY]['phase'] == 'pgoutput'
    assert 'retire_lsn' not in result[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY]


@pytest.mark.parametrize('migration', [
    None,
    {
        'version': 1,
        'phase': 'pgoutput',
        'source_slot': 'pipelinewise_source_orders',
        'destination_slot': 'pipelinewise_orders',
        'copy_lsn': True,
        'bridge_lsn': 200,
    },
    {
        'version': 1,
        'phase': 'retire',
        'source_slot': 'pipelinewise_source_orders',
        'destination_slot': 'pipelinewise_orders',
        'copy_lsn': 100,
        'bridge_lsn': 200,
        'retire_lsn': 200,
    },
    {
        'version': 1,
        'phase': 'pgoutput',
        'source_slot': 'foreign_slot',
        'destination_slot': 'pipelinewise_orders',
        'copy_lsn': 100,
        'bridge_lsn': 200,
    },
])
def test_malformed_migration_state_fails_before_source_preflight(migration):
    state = _state()
    state[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY] = migration

    with patch.object(logical_replication, 'prepare_publication') as prepare, \
            pytest.raises(logical_replication.ReplicationSlotMigrationError, match='Invalid'):
        logical_replication.sync_tables(
            _config(), [_stream()], state, 250, 'state.json')

    prepare.assert_not_called()


def test_persisted_bridge_phase_requires_parent_promotion_before_resume():
    migration = {
        'version': 1,
        'phase': 'bridge',
        'source_slot': 'pipelinewise_source_orders',
        'destination_slot': 'pipelinewise_orders',
        'copy_lsn': 100,
        'bridge_lsn': 200,
    }

    with patch.object(logical_replication, 'prepare_publication') as prepare, \
            pytest.raises(logical_replication.ReplicationSlotMigrationError, match='promoted by PipelineWise'):
        logical_replication.sync_tables(
            _config(), [_stream()], _state(migration=migration), 250, 'state.json')

    prepare.assert_not_called()


def _two_logical_streams():
    payments = _stream()
    refunds = copy.deepcopy(payments)
    refunds.update({
        'tap_stream_id': 'public-refunds',
        'stream': 'refunds',
        'table_name': 'refunds',
    })
    return [payments, refunds]


def test_historical_primary_key_update_does_not_require_columns_added_later():
    payload = {
        '_pgoutput': True, 'action': 'U',
        'identity': [{'name': 'id', 'value': '1'}],
        'columns': [{'name': 'id', 'value': '2'}, {'name': 'description', 'value': 'old value'}],
        'relation_columns': [{'name': 'id'}, {'name': 'description'}],
    }
    assert logical_replication._old_primary_key_for_changed_update(
        payload, ['id'], {'id', 'description', 'added_later'}) == ['1']
    payload['columns'].pop()
    with pytest.raises(logical_replication.ReplicationSlotMigrationError, match='description'):
        logical_replication._old_primary_key_for_changed_update(
            payload, ['id'], {'id', 'description', 'added_later'})


def test_legacy_names_follow_postgres_name_truncation_and_alias():
    database = 'source_' + 'd' * 30
    old_id = 'Old-Tap-' + 't' * 30
    expected = ('pipelinewise_' + database + '_' + old_id).lower().replace('-', '_')[:63]
    assert logical_replication.legacy_replication_slot_names(database, old_id)[1] == expected
    rows = {expected: _slot('wal2json', database=database)}
    cursor = Mock()
    cursor.fetchone.return_value = ('pipelinewise_new_tap', '0/64')
    with patch.object(logical_replication, '_slot_rows', return_value=rows):
        slot = logical_replication.locate_replication_slot_by_cur(
            cursor, database, 'new_tap', previous_tap_id=old_id)
    assert slot.migration_source == expected


def test_truncated_legacy_names_with_ambiguous_ownership_are_rejected():
    database = 'a' * 60
    shared = logical_replication.legacy_replication_slot_names(database, 'tap')[0]
    with patch.object(logical_replication, '_slot_rows', return_value={shared: _slot('wal2json')}), \
            pytest.raises(logical_replication.ReplicationSlotMigrationError, match='collide after'):
        logical_replication._validate_replication_slot_candidates(Mock(), database, 'tap')


@pytest.mark.parametrize('fresh_start,canonical_exists', [(True, False), (True, True), (False, True)])
def test_fresh_or_existing_pgoutput_tap_preserves_colliding_shared_history(fresh_start, canonical_exists):
    database = 'a' * 60
    shared = logical_replication.legacy_replication_slot_names(database, 'tap')[0]
    rows = {shared: _slot('wal2json', database=database, active=True)}
    if canonical_exists:
        rows['pipelinewise_tap'] = _slot('pgoutput', database=database)
    cursor = Mock()
    with patch.object(logical_replication, '_slot_rows', return_value=rows):
        destination, migration_source, result_rows = logical_replication._validate_replication_slot_candidates(
            cursor, database, 'tap', fresh_start=fresh_start,
        )
    assert destination == 'pipelinewise_tap'
    assert migration_source is None
    assert result_rows[shared]['active'] is True
    cursor.execute.assert_not_called()


def test_fresh_tap_does_not_claim_unrelated_shared_slot():
    with patch.object(logical_replication, '_slot_rows', return_value={'pipelinewise_source': _slot('wal2json')}):
        destination, source, _ = logical_replication._validate_replication_slot_candidates(
            Mock(), 'source', 'orders', fresh_start=True)
    assert destination == 'pipelinewise_orders'
    assert source is None


@pytest.mark.parametrize('existing_tables', [
    [('public', 'payments'), ('public', 'other')], [('public', 'other')],
])
def test_publication_preparation_preserves_tables_outside_the_current_run(existing_tables):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.server_version = 140018
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (
        False, True, True, True, False, True,
        logical_replication._encode_publication_fence_comment('ready', None))
    cursor.fetchall.return_value = existing_tables
    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, '_reject_selected_generated_columns'), \
            patch.object(logical_replication, '_validate_publication_tables', return_value=(
                [('public', 'payments')], False, [('public', 'payments')], {}, {})), \
            patch.object(logical_replication, '_validate_replica_identity'), \
            patch.object(logical_replication, '_slot_rows', return_value={}), \
            patch.object(logical_replication, '_wait_for_prepublication_transactions') as fence:
        logical_replication.prepare_publication(_config(), [_stream()])
    queries = [str(item.args[0]) for item in cursor.execute.call_args_list]
    assert not any('SET TABLE' in query or 'DROP TABLE' in query for query in queries)
    expected_add = ('public', 'payments') not in existing_tables
    assert any('ADD TABLE' in query for query in queries) == expected_add
    assert fence.called == expected_add


def test_publication_fence_reuses_connection_and_waits_only_for_preexisting_writers():
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.side_effect = [[('1/2',)], [('1/2',)], [], [], []]
    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection) as connect, \
            patch.object(logical_replication.time, 'sleep'):
        logical_replication._wait_for_prepublication_transactions(_config())
    connect.assert_called_once()
    connection.close.assert_called_once()
    assert 'activity.backend_xid IS NOT NULL' in cursor.execute.call_args_list[0].args[0]


@pytest.mark.parametrize('phase,fresh_start,allowed', [
    ('pgoutput', False, True), ('retire', False, True),
    ('bridge', False, False), (None, True, True), (None, False, False),
])
def test_partition_preflight_respects_durable_migration_phase(phase, fresh_start, allowed):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.server_version = 140018
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (
        False, True, True, True, False, True,
        logical_replication._encode_publication_fence_comment('ready', None))
    cursor.fetchall.return_value = [('public', 'payments')]
    migration = {'version': 1, 'phase': phase, 'source_slot': 'pipelinewise_source_orders',
                 'destination_slot': 'pipelinewise_orders', 'copy_lsn': 100, 'bridge_lsn': 200,
                 'retire_lsn': 300}
    state = _state(migration=migration if phase is not None else None)
    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, '_reject_selected_generated_columns'), \
            patch.object(logical_replication, '_validate_publication_tables', return_value=(
                [('public', 'payments')], True, [('public', 'payments')], {}, {})), \
            patch.object(logical_replication, '_validate_replica_identity'), \
            patch.object(logical_replication, '_slot_rows', return_value={
                'pipelinewise_source_orders': _slot('wal2json'), 'pipelinewise_orders': _slot('pgoutput')}):
        if allowed:
            assert logical_replication.prepare_publication(
                _config(), [_stream()], state=state, fresh_start=fresh_start) == 'pw_pub_orders'
        else:
            with pytest.raises(logical_replication.ReplicationSlotMigrationError, match='partition root'):
                logical_replication.prepare_publication(_config(), [_stream()], state=state)


def test_target_acknowledgement_uses_minimum_lsn_across_all_logical_streams(tmp_path):
    state_file = tmp_path / 'state.json'
    state_file.write_text(json.dumps({
        'bookmarks': {
            'public-payments': {'lsn': 220},
            'public-refunds': {'lsn': 180},
        },
    }))

    assert logical_replication._read_target_acknowledged_lsn(
        state_file, _two_logical_streams(), 100) == 180


def test_target_acknowledgement_never_regresses(tmp_path):
    state_file = tmp_path / 'state.json'
    state_file.write_text(json.dumps({
        'bookmarks': {
            'public-payments': {'lsn': 90},
            'public-refunds': {'lsn': 95},
        },
    }))

    assert logical_replication._read_target_acknowledged_lsn(
        state_file, _two_logical_streams(), 100) == 100


@pytest.mark.parametrize('state_contents', [
    '{',
    '{}',
    json.dumps({'bookmarks': {'public-payments': {'lsn': 200}}}),
    json.dumps({
        'bookmarks': {
            'public-payments': {'lsn': 200},
            'public-refunds': {'lsn': True},
        },
    }),
])
def test_invalid_or_truncated_target_state_does_not_advance_feedback(
        tmp_path, state_contents):
    state_file = tmp_path / 'state.json'
    state_file.write_text(state_contents)

    assert logical_replication._read_target_acknowledged_lsn(
        state_file, _two_logical_streams(), 100) == 100


def test_missing_or_unreadable_target_state_does_not_advance_feedback(tmp_path):
    missing_state_file = tmp_path / 'missing-state.json'
    assert logical_replication._read_target_acknowledged_lsn(
        missing_state_file, _two_logical_streams(), 100) == 100

    with patch('builtins.open', side_effect=OSError('read failed')):
        assert logical_replication._read_target_acknowledged_lsn(
            'state.json', _two_logical_streams(), 100) == 100


def _feedback_messages(token):
    return [
        WalMessage(json.dumps({'action': 'B'}), 101),
        WalMessage(json.dumps({'action': 'C', 'end_lsn': 150}), 150),
        WalMessage(json.dumps({'action': 'B'}), 151),
        WalMessage(json.dumps({
            'action': 'M',
            'transactional': True,
            'prefix': 'pipelinewise_orders',
            'content': token,
        }), 152),
        WalMessage(json.dumps({'action': 'C', 'end_lsn': 160}), 160),
    ]


def _run_pgoutput_feedback(messages, acknowledged_lsn):
    connection = _ReplicationConnection(messages)
    with patch.object(
            logical_replication,
            'prepare_publication',
            return_value=logical_replication.PreparedPublication('pw_pub_orders')), \
            patch.object(
                logical_replication,
                'locate_replication_slot',
                return_value=logical_replication.PreparedReplicationSlot(
                    'pipelinewise_orders', confirmed_flush_lsn=100)), \
            patch.object(
                logical_replication.post_db,
                'open_connection',
                return_value=connection), \
            patch.object(
                logical_replication,
                'emit_boundary_message',
                return_value='boundary-token'), \
            patch.object(
                logical_replication,
                'consume_message',
                side_effect=lambda _s, state, *_a, **_k: state), \
            patch.object(
                logical_replication,
                '_read_target_acknowledged_lsn',
                side_effect=acknowledged_lsn) as read_acknowledgement, \
            patch.object(logical_replication.sync_common, 'send_schema_message'), \
            patch.object(
                logical_replication.singer,
                'write_message') as write_message, \
            patch.object(logical_replication, 'FEEDBACK_POLL_INTERVAL', 0), \
            patch.object(logical_replication, 'UPDATE_BOOKMARK_PERIOD', 1):
        logical_replication.sync_tables(
            _config(), [_stream()], _state(), 90, 'state.json')
    return connection.cursor_instance, read_acknowledgement, write_message


def test_pgoutput_feedback_advances_only_to_target_persisted_state():
    cursor, read_acknowledgement, _ = _run_pgoutput_feedback(
        _feedback_messages('boundary-token'),
        [100, 150, 150, 150],
    )

    assert read_acknowledgement.call_count == 4
    assert cursor.send_feedback.call_args_list == [
        call(
            write_lsn=100,
            flush_lsn=0,
            reply=True,
            force=True,
        ),
        call(
            write_lsn=100,
            flush_lsn=100,
            reply=True,
            force=True,
        ),
        call(
            write_lsn=150,
            flush_lsn=150,
            reply=True,
            force=True,
        ),
    ]


def test_consumed_and_periodically_emitted_positions_are_not_feedback_until_target_ack():
    cursor, read_acknowledgement, write_message = _run_pgoutput_feedback(
        _feedback_messages('boundary-token'),
        [0, 0, 0, 0],
    )

    assert read_acknowledgement.call_count == 4
    assert any(
        message.value['bookmarks']['public-payments']['lsn'] == 150
        for message, in (write.args for write in write_message.call_args_list)
    )
    cursor.send_feedback.assert_called_once_with(
        write_lsn=100,
        flush_lsn=0,
        reply=True,
        force=True,
    )
