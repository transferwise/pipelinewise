import base64
import copy
import datetime
import json
from collections import namedtuple
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from tap_postgres.sync_strategies import logical_replication


WalMessage = namedtuple('WalMessage', ['payload', 'data_start'])


@pytest.fixture
def permitted_boundary_messages():
    with patch.object(logical_replication.post_db, 'require_logical_message_privilege') as permission:
        yield permission


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


def test_fresh_slot_uses_canonical_pgoutput_name_and_preserves_source_position():
    cursor = Mock()
    cursor.fetchone.return_value = ('ppw_slot_orders', '0/7B')
    rows = {
        'pipelinewise_source': _slot('wal2json'),
        'pipelinewise_source_orders': _slot('wal2json'),
    }

    with patch.object(logical_replication, '_slot_rows', return_value=rows):
        result = logical_replication.locate_replication_slot_by_cur(
            cursor, 'source', 'orders')

    assert result == 'ppw_slot_orders'
    assert result.migration_source == 'pipelinewise_source_orders'
    assert result.source_confirmed_lsn == 100
    assert result.confirmed_flush_lsn == 123
    cursor.execute.assert_called_once_with(
        'SELECT slot_name, lsn::text FROM pg_catalog.pg_create_logical_replication_slot(%s, %s)',
        ('ppw_slot_orders', 'pgoutput'),
    )


def test_slot_resolution_rejects_database_wide_legacy_slot_before_creation():
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
        'ppw_slot_orders': _slot('pgoutput', lsn='0/7B'),
        'pipelinewise_source': _slot('wal2json'),
    }

    with patch.object(logical_replication, '_slot_rows', return_value=rows):
        cursor = Mock()
        result = logical_replication.locate_replication_slot_by_cur(
            cursor, 'source', 'orders')

    assert result == 'ppw_slot_orders'
    assert result.migration_source is None
    assert result.confirmed_flush_lsn == 123
    cursor.execute.assert_not_called()


def test_partition_root_rejects_tap_specific_wal2json_migration_before_creation():
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
    ('ppw_slot_orders', 'pgoutput'),
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
        'ppw_slot_orders',
        _slot('wal2json'),
        'uses wal2json, expected pgoutput',
    ),
    (
        'ppw_slot_orders',
        _slot('pgoutput', database='other'),
        'logical slot for database source',
    ),
    (
        'ppw_slot_orders',
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


def test_prepare_publication_rejects_invalid_slot_before_publication_ddl(permitted_boundary_messages):
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
                return_value={'ppw_slot_orders': _slot('wal2json')}), \
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


def test_publication_fence_comment_round_trips_original_comment_and_managed_tables():
    for original in (None, 'DBA-owned publication comment'):
        encoded = logical_replication._encode_publication_fence_comment(
            'ready', original, {('public', 'refunds'), ('public', 'payments')})
        assert encoded.startswith(logical_replication.PUBLICATION_FENCE_COMMENT_PREFIX)
        assert logical_replication._decode_publication_fence_comment(encoded) == (
            'ready', original, {('public', 'payments'), ('public', 'refunds')})


def test_legacy_publication_fence_comment_decodes_with_no_managed_tables():
    payload = json.dumps({'state': 'ready', 'original_comment': 'legacy'}).encode()
    encoded = (
        logical_replication.PUBLICATION_FENCE_COMMENT_PREFIX
        + base64.urlsafe_b64encode(payload).decode()
    )

    assert logical_replication._decode_publication_fence_comment(encoded) == (
        'ready', 'legacy', set())


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
        'ppw_slot_orders',
        wal2json_tables=[('public', 'payments')],
    )
    current_time = [datetime.datetime(2026, 1, 1)]
    clock = Mock()
    clock.utcnow.side_effect = lambda: current_time[0]

    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, 'emit_wal_progress_message', return_value=106), \
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
            'ppw_slot_orders',
            105,
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
            return_value=logical_replication.PreparedPublication('ppw_slot_orders')), \
            patch.object(
                logical_replication,
                'locate_replication_slot',
                return_value=logical_replication.PreparedReplicationSlot(
                    'ppw_slot_orders', confirmed_flush_lsn=100)), \
            patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, 'emit_wal_progress_message', return_value=106), \
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
        'ppw_slot_orders',
        wal2json_tables=[('public', 'payments')],
    )
    state = _state()

    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, 'emit_wal_progress_message', return_value=106), \
            patch.object(logical_replication, 'consume_message', side_effect=lambda _s, state, *_a, **_k: state), \
            patch.object(logical_replication.singer, 'write_message'):
        result = logical_replication._bridge_wal2json_slot(
            _config(),
            [_stream()],
            state,
            'state.json',
            publication,
            'pipelinewise_source_orders',
            'ppw_slot_orders',
            105,
            100,
            None,
        )

    assert result['bookmarks']['public-payments']['lsn'] == 110
    assert result[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY] == {
        'version': 2,
        'phase': 'bridge',
        'source_slot': 'pipelinewise_source_orders',
        'destination_slot': 'ppw_slot_orders',
        'slot_lsn': 105,
        'bridge_lsn': 110,
        'boundary_lsn': 106,
    }


def test_wal2json_bridge_retry_reuses_first_boundary():
    config = _config()
    publication = logical_replication.PreparedPublication(
        'ppw_slot_orders', wal2json_tables=[('public', 'payments')])
    token = 'ignored-message'
    interrupted_connection = _ReplicationConnection([])
    retry_connection = _ReplicationConnection([
        WalMessage(json.dumps({'action': 'B'}), 101),
        WalMessage(json.dumps({
            'action': 'M',
            'transactional': True,
            'prefix': 'pipelinewise_orders',
            'content': token,
        }), 102),
        WalMessage(json.dumps({'action': 'C'}), 110),
    ])

    with patch.object(
            logical_replication.post_db,
            'open_connection',
            side_effect=[interrupted_connection, retry_connection]), patch.object(
            logical_replication,
            'emit_wal_progress_message',
            return_value=106) as emit, patch.object(
            logical_replication,
            'consume_message',
            side_effect=lambda _streams, state, *_args, **_kwargs: state), patch.object(
            logical_replication.singer,
            'write_message'):
        interrupted = logical_replication._bridge_wal2json_slot(
            _config(max_run_seconds=0),
            [_stream()],
            _state(),
            'state.json',
            publication,
            'pipelinewise_source_orders',
            'ppw_slot_orders',
            105,
            100,
            None,
        )
        result = logical_replication._bridge_wal2json_slot(
            config,
            [_stream()],
            copy.deepcopy(interrupted),
            'state.json',
            publication,
            'pipelinewise_source_orders',
            'ppw_slot_orders',
            105,
            100,
            None,
        )

    assert interrupted[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY] == {
        'version': 2,
        'phase': 'bridge_pending',
        'source_slot': 'pipelinewise_source_orders',
        'destination_slot': 'ppw_slot_orders',
        'slot_lsn': 105,
        'boundary_lsn': 106,
    }
    emit.assert_called_once_with(_config(max_run_seconds=0))
    assert result[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY]['bridge_lsn'] == 110


def test_bridge_rejects_bookmark_before_legacy_confirmed_flush_lsn():
    publication = logical_replication.PreparedPublication(
        'ppw_slot_orders', wal2json_tables=[('public', 'payments')])
    slot = logical_replication.PreparedReplicationSlot(
        'ppw_slot_orders', 'pipelinewise_source_orders', 100, 123)

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
    publication = logical_replication.PreparedPublication('ppw_slot_orders')
    slot = logical_replication.PreparedReplicationSlot(
        'ppw_slot_orders', confirmed_flush_lsn=101)

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
        'ppw_slot_orders',
        wal2json_tables=[('public', 'payments_2026')],
        expected_primary_keys={('public', 'payments_2026'): ('id',)},
    )
    slot = logical_replication.PreparedReplicationSlot(
        'ppw_slot_orders', 'pipelinewise_source_orders', 100, 123)
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
    assert bridge.call_args.args[5:9] == (
        'pipelinewise_source_orders',
        'ppw_slot_orders',
        123,
        100,
    )


def test_bridge_rejects_primary_key_changed_after_publication_preflight():
    publication = logical_replication.PreparedPublication(
        'ppw_slot_orders',
        wal2json_tables=[('public', 'payments')],
        expected_primary_keys={('public', 'payments'): ('id',)},
    )
    slot = logical_replication.PreparedReplicationSlot(
        'ppw_slot_orders', 'pipelinewise_source_orders', 100, 123)

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


def test_pgoutput_overlap_reuses_boundary_starts_at_original_lsn_and_is_monotonic():
    migration = {
        'version': 2,
        'phase': 'pgoutput_overlap',
        'source_slot': 'pipelinewise_source_orders',
        'destination_slot': 'ppw_slot_orders',
        'slot_lsn': 123,
        'bridge_lsn': 200,
        'boundary_lsn': 124,
    }
    messages = [
        WalMessage(json.dumps({'action': 'B'}), 124),
        WalMessage(json.dumps({'action': 'C', 'end_lsn': 160}), 160),
        WalMessage(json.dumps({'action': 'B'}), 161),
        WalMessage(json.dumps({'action': 'C', 'end_lsn': 220}), 220),
        WalMessage(json.dumps({'action': 'I'}), 221),
    ]
    connection = _ReplicationConnection(messages)
    slot = logical_replication.PreparedReplicationSlot(
        'ppw_slot_orders', confirmed_flush_lsn=123)
    publication = logical_replication.PreparedPublication('ppw_slot_orders')

    with patch.object(logical_replication, 'prepare_publication', return_value=publication), \
            patch.object(logical_replication, 'locate_replication_slot', return_value=slot), \
            patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, 'emit_wal_progress_message') as emit, \
            patch.object(logical_replication, 'consume_message', side_effect=lambda _s, state, *_a, **_k: state), \
            patch.object(logical_replication.sync_common, 'send_schema_message'), \
            patch.object(logical_replication, 'FEEDBACK_POLL_INTERVAL', 0), \
            patch.object(logical_replication.singer, 'write_message') as write_state:
        result = logical_replication.sync_tables(
            _config(break_at_end_lsn=False), [_stream()],
            _state(lsn=200, migration=copy.deepcopy(migration)), 190, 'state.json')

    emit.assert_not_called()
    assert connection.cursor_instance.start_replication.call_args.kwargs['start_lsn'] == 123
    connection.cursor_instance.send_feedback.assert_called_once_with(
        write_lsn=123,
        flush_lsn=0,
        reply=True,
        force=True,
    )
    assert connection.cursor_instance.read_message.call_count == 4
    assert result[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY]['phase'] == 'overlap_complete'
    assert result[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY]['crossover_lsn'] == 220
    assert result['bookmarks']['public-payments']['lsn'] == 220
    assert all(
        message.args[0].value['bookmarks']['public-payments']['lsn'] >= 200
        for message in write_state.call_args_list
    )


def test_pgoutput_overlap_retry_reuses_shared_boundary():
    migration = {
        'version': 2,
        'phase': 'pgoutput_overlap',
        'source_slot': 'pipelinewise_source_orders',
        'destination_slot': 'ppw_slot_orders',
        'slot_lsn': 123,
        'bridge_lsn': 200,
        'boundary_lsn': 124,
    }
    interrupted_connection = _ReplicationConnection([])
    retry_connection = _ReplicationConnection([
        WalMessage(json.dumps({'action': 'B'}), 201),
        WalMessage(json.dumps({'action': 'C', 'end_lsn': 260}), 260),
    ])
    publication = logical_replication.PreparedPublication('ppw_slot_orders')
    slot = logical_replication.PreparedReplicationSlot(
        'ppw_slot_orders', confirmed_flush_lsn=123)

    with patch.object(
            logical_replication,
            'prepare_publication',
            return_value=publication), patch.object(
            logical_replication,
            'locate_replication_slot',
            return_value=slot), patch.object(
            logical_replication.post_db,
            'open_connection',
            side_effect=[interrupted_connection, retry_connection]), patch.object(
            logical_replication, 'emit_wal_progress_message') as emit, patch.object(
            logical_replication,
            'consume_message',
            side_effect=lambda _streams, state, *_args, **_kwargs: state), patch.object(
            logical_replication.sync_common,
            'send_schema_message'), patch.object(
            logical_replication.singer,
            'write_message'):
        interrupted = logical_replication.sync_tables(
            _config(max_run_seconds=0),
            [_stream()],
            _state(lsn=200, migration=copy.deepcopy(migration)),
            190,
            'state.json',
        )
        result = logical_replication.sync_tables(
            _config(),
            [_stream()],
            copy.deepcopy(interrupted),
            190,
            'state.json',
        )

    emit.assert_not_called()
    assert interrupted[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY]['phase'] == 'pgoutput_overlap'
    assert result[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY]['phase'] == 'overlap_complete'
    assert result[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY]['crossover_lsn'] == 260


@pytest.mark.parametrize('migration', [
    None,
    {
        'version': 2,
        'phase': 'bridge_pending',
        'source_slot': 'pipelinewise_source_orders',
        'destination_slot': 'ppw_slot_orders',
        'slot_lsn': 123,
        'boundary_lsn': 'invalid',
    },
    {
        'version': 2,
        'phase': 'pgoutput_overlap',
        'source_slot': 'pipelinewise_source_orders',
        'destination_slot': 'ppw_slot_orders',
        'slot_lsn': True,
        'bridge_lsn': 200,
        'boundary_lsn': 2,
    },
    {
        'version': 2,
        'phase': 'overlap_complete',
        'source_slot': 'pipelinewise_source_orders',
        'destination_slot': 'ppw_slot_orders',
        'slot_lsn': 123,
        'bridge_lsn': 200,
        'crossover_lsn': 100,
        'boundary_lsn': 124,
    },
    {
        'version': 2,
        'phase': 'pgoutput_overlap',
        'source_slot': 'foreign_slot',
        'destination_slot': 'ppw_slot_orders',
        'slot_lsn': 123,
        'bridge_lsn': 200,
        'boundary_lsn': 124,
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
        'version': 2,
        'phase': 'bridge',
        'source_slot': 'pipelinewise_source_orders',
        'destination_slot': 'ppw_slot_orders',
        'slot_lsn': 123,
        'bridge_lsn': 200,
        'boundary_lsn': 124,
    }

    with patch.object(logical_replication, 'prepare_publication') as prepare, \
            pytest.raises(logical_replication.ReplicationSlotMigrationError, match='processed by PipelineWise'):
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
    cursor.fetchone.return_value = ('ppw_slot_new_tap', '0/7B')
    with patch.object(logical_replication, '_slot_rows', return_value=rows):
        slot = logical_replication.locate_replication_slot_by_cur(
            cursor, database, 'new_tap', previous_tap_id=old_id)
    assert slot.migration_source == expected


def test_implicitly_truncated_tap_specific_slot_requires_explicit_previous_id():
    database = 'source_' + 'd' * 30
    tap_id = 'orders_' + 't' * 30
    source = logical_replication.legacy_replication_slot_names(database, tap_id)[1]
    rows = {source: _slot('wal2json', database=database)}
    cursor = Mock()

    with patch.object(logical_replication, '_slot_rows', return_value=rows), \
            pytest.raises(
                logical_replication.ReplicationSlotMigrationError,
                match='implicitly truncated.*previous_tap_id',
            ):
        logical_replication.locate_replication_slot_by_cur(cursor, database, tap_id)

    cursor.execute.assert_not_called()


@pytest.mark.parametrize('proof', ['canonical', 'fresh-start', 'final-deselection'])
def test_implicitly_truncated_slot_is_preserved_when_history_is_not_claimed(proof):
    database = 'source_' + 'd' * 30
    tap_id = 'orders_' + 't' * 30
    destination = logical_replication.generate_replication_slot_name(tap_id)
    source = logical_replication.legacy_replication_slot_names(database, tap_id)[1]
    rows = {source: _slot('wal2json', database=database, active=True)}
    kwargs = {}
    if proof == 'canonical':
        rows[destination] = _slot('pgoutput', database=database)
    elif proof == 'fresh-start':
        kwargs['fresh_start'] = True
    else:
        kwargs['final_log_deselection'] = True

    with patch.object(logical_replication, '_slot_rows', return_value=rows):
        resolved_destination, migration_source, result_rows = (
            logical_replication._validate_replication_slot_candidates(
                Mock(), database, tap_id, **kwargs))

    assert resolved_destination == destination
    assert migration_source is None
    assert result_rows[source]['active'] is True


def test_implicit_truncated_migration_marker_requires_explicit_previous_id():
    database = 'source_' + 'd' * 30
    tap_id = 'orders_' + 't' * 30
    destination = logical_replication.generate_replication_slot_name(tap_id)
    source = logical_replication.legacy_replication_slot_names(database, tap_id)[1]
    marker = {
        'version': 2, 'phase': 'bridge', 'source_slot': source,
        'destination_slot': destination, 'slot_lsn': 100, 'bridge_lsn': 200,
        'boundary_lsn': 101,
    }

    with pytest.raises(
            logical_replication.ReplicationSlotMigrationError,
            match='implicitly truncated.*previous_tap_id'):
        logical_replication._validate_migration_state({
            'dbname': database, 'tap_id': tap_id,
        }, marker)


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
        rows['ppw_slot_tap'] = _slot('pgoutput', database=database)
    cursor = Mock()
    with patch.object(logical_replication, '_slot_rows', return_value=rows):
        destination, migration_source, result_rows = logical_replication._validate_replication_slot_candidates(
            cursor, database, 'tap', fresh_start=fresh_start,
        )
    assert destination == 'ppw_slot_tap'
    assert migration_source is None
    assert result_rows[shared]['active'] is True
    cursor.execute.assert_not_called()


def test_fresh_tap_does_not_claim_unrelated_shared_slot():
    with patch.object(logical_replication, '_slot_rows', return_value={'pipelinewise_source': _slot('wal2json')}):
        destination, source, _ = logical_replication._validate_replication_slot_candidates(
            Mock(), 'source', 'orders', fresh_start=True)
    assert destination == 'ppw_slot_orders'
    assert source is None


def test_filtered_publication_preparation_is_additive_and_tracks_managed_tables(permitted_boundary_messages):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.server_version = 140018
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (
        False, True, True, True, False, True,
        logical_replication._encode_publication_fence_comment(
            'ready', 'DBA comment', {('public', 'refunds')}))
    cursor.fetchall.return_value = [('public', 'refunds'), ('public', 'dba_table')]
    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, '_reject_selected_generated_columns'), \
            patch.object(logical_replication, '_validate_publication_tables', return_value=(
                [('public', 'payments')], False, [('public', 'payments')], {}, {})), \
            patch.object(logical_replication, '_validate_replica_identity'), \
            patch.object(logical_replication, '_slot_rows', return_value={}), \
            patch.object(logical_replication, '_set_publication_comment') as set_comment, \
            patch.object(logical_replication, '_wait_for_prepublication_transactions') as fence:
        logical_replication.prepare_publication(_config(), [_stream()])

    queries = [str(item.args[0]) for item in cursor.execute.call_args_list]
    assert any('ADD TABLE' in query for query in queries)
    assert not any('DROP TABLE' in query for query in queries)
    assert fence.call_count == 1
    assert [
        logical_replication._decode_publication_fence_comment(item.args[2])
        for item in set_comment.call_args_list
    ] == [
        ('pending', 'DBA comment', {('public', 'payments'), ('public', 'refunds')}),
        ('ready', 'DBA comment', {('public', 'payments'), ('public', 'refunds')}),
    ]


@pytest.mark.parametrize('with_pending_state', [False, True])
def test_selection_change_is_rejected_between_bridge_attempts(with_pending_state, permitted_boundary_messages):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.server_version = 140018
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (
        False, True, True, True, False, True,
        logical_replication._encode_publication_fence_comment(
            'ready', None, {('public', 'payments')}),
    )
    cursor.fetchall.return_value = [('public', 'payments')]
    migration = None
    if with_pending_state:
        migration = {
            'version': 2,
            'phase': 'bridge_pending',
            'source_slot': 'pipelinewise_source_orders',
            'destination_slot': 'ppw_slot_orders',
            'slot_lsn': 105,
            'boundary_lsn': 106,
        }

    with patch.object(
            logical_replication.post_db,
            'open_connection',
            return_value=connection), patch.object(
            logical_replication,
            '_reject_selected_generated_columns'), patch.object(
            logical_replication,
            '_validate_publication_tables',
            return_value=(
                [('public', 'payments'), ('public', 'refunds')],
                False,
                [('public', 'payments'), ('public', 'refunds')],
                {},
                {},
            )), patch.object(
            logical_replication,
            '_validate_replica_identity'), patch.object(
            logical_replication,
            '_slot_rows',
            return_value={
                'pipelinewise_source_orders': _slot('wal2json'),
                'ppw_slot_orders': _slot('pgoutput'),
            }), patch.object(
            logical_replication,
            '_set_publication_comment') as set_comment, patch.object(
            logical_replication,
            '_wait_for_prepublication_transactions') as fence, pytest.raises(
            logical_replication.ReplicationSlotMigrationError,
            match='selection or options cannot change.*re-run import_config'):
        logical_replication.prepare_publication(
            _config(),
            _two_logical_streams(),
            state=_state(migration=migration),
        )

    queries = [str(item.args[0]) for item in cursor.execute.call_args_list]
    assert not any('ALTER PUBLICATION' in query for query in queries)
    set_comment.assert_not_called()
    fence.assert_not_called()


def test_publication_option_change_is_rejected_between_bridge_attempts(permitted_boundary_messages):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.server_version = 140018
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (
        False, True, True, True, False, False,
        logical_replication._encode_publication_fence_comment(
            'ready', None, {('public', 'payments')}),
    )
    cursor.fetchall.return_value = [('public', 'payments')]

    with patch.object(
            logical_replication.post_db,
            'open_connection',
            return_value=connection), patch.object(
            logical_replication,
            '_reject_selected_generated_columns'), patch.object(
            logical_replication,
            '_validate_publication_tables',
            return_value=(
                [('public', 'payments')],
                False,
                [('public', 'payments')],
                {},
                {},
            )), patch.object(
            logical_replication,
            '_validate_replica_identity'), patch.object(
            logical_replication,
            '_slot_rows',
            return_value={
                'pipelinewise_source_orders': _slot('wal2json'),
                'ppw_slot_orders': _slot('pgoutput'),
            }), patch.object(
            logical_replication,
            '_set_publication_comment') as set_comment, patch.object(
            logical_replication,
            '_wait_for_prepublication_transactions') as fence, pytest.raises(
            logical_replication.ReplicationSlotMigrationError,
            match='selection or options cannot change'):
        logical_replication.prepare_publication(_config(), [_stream()])

    queries = [str(item.args[0]) for item in cursor.execute.call_args_list]
    assert not any('ALTER PUBLICATION' in query for query in queries)
    set_comment.assert_not_called()
    fence.assert_not_called()


def test_explicit_publication_reconcile_removes_only_stale_managed_tables(permitted_boundary_messages):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.server_version = 140018
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect = [
        (1,),
        (
            False, True, True, True, False, True,
            logical_replication._encode_publication_fence_comment(
                'ready', None, {('public', 'payments'), ('public', 'refunds')}),
        ),
    ]
    cursor.fetchall.return_value = [
        ('public', 'payments'),
        ('public', 'refunds'),
        ('public', 'dba_table'),
    ]

    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, '_reject_selected_generated_columns'), \
            patch.object(logical_replication, '_validate_publication_tables', return_value=(
                [('public', 'payments')], False, [('public', 'payments')], {}, {})), \
            patch.object(logical_replication, '_validate_replica_identity'), \
            patch.object(logical_replication, '_slot_rows', return_value={}), \
            patch.object(logical_replication, '_set_publication_comment') as set_comment, \
            patch.object(logical_replication, '_wait_for_prepublication_transactions') as fence:
        logical_replication.prepare_publication(
            _config(), [_stream()], reconcile=True)

    queries = [str(item.args[0]) for item in cursor.execute.call_args_list]
    assert any('DROP TABLE' in query for query in queries)
    assert not any('ADD TABLE' in query for query in queries)
    assert fence.call_count == 1
    assert logical_replication._decode_publication_fence_comment(
        set_comment.call_args_list[-1].args[2]
    ) == ('ready', None, {('public', 'payments')})


def test_empty_publication_reconcile_removes_all_managed_tables_without_dropping_publication(
        permitted_boundary_messages):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.server_version = 140018
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect = [
        (1,),
        (
            False, True, True, True, False, True,
            logical_replication._encode_publication_fence_comment(
                'ready', None, {('public', 'payments'), ('public', 'refunds')}),
        ),
    ]
    cursor.fetchall.return_value = [
        ('public', 'payments'),
        ('public', 'refunds'),
        ('public', 'dba_table'),
    ]

    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, '_slot_rows', return_value={}), \
            patch.object(logical_replication, '_set_publication_comment') as set_comment, \
            patch.object(logical_replication, '_wait_for_prepublication_transactions') as fence:
        result = logical_replication.prepare_publication(
            _config(), [], reconcile=True)

    queries = [str(item.args[0]) for item in cursor.execute.call_args_list]
    assert result == 'ppw_slot_orders'
    assert any('DROP TABLE' in query for query in queries)
    assert not any('DROP PUBLICATION' in query or 'CREATE PUBLICATION' in query for query in queries)
    assert fence.call_count == 1
    assert logical_replication._decode_publication_fence_comment(
        set_comment.call_args_list[-1].args[2]
    ) == ('ready', None, set())


def test_final_log_deselection_reconciles_managed_tables_while_migration_slots_coexist(permitted_boundary_messages):
    """Durably abandoned LOG state may empty the publication before slot retirement."""
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.server_version = 140018
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect = [
        (1,),
        (
            False, True, True, True, False, True,
            logical_replication._encode_publication_fence_comment(
                'ready', None, {('public', 'payments')}),
        ),
    ]
    cursor.fetchall.return_value = [('public', 'payments'), ('public', 'dba_table')]
    slots = {
        'pipelinewise_source_orders': _slot('wal2json'),
        'ppw_slot_orders': _slot('pgoutput'),
    }

    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, '_slot_rows', return_value=slots), \
            patch.object(logical_replication, '_set_publication_comment') as set_comment, \
            patch.object(logical_replication, '_wait_for_prepublication_transactions') as fence:
        result = logical_replication.prepare_publication(
            _config(), [], state={'bookmarks': {'full': {'xmin': 10}}},
            reconcile=True, final_log_deselection=True)

    queries = [str(item.args[0]) for item in cursor.execute.call_args_list]
    assert result == 'ppw_slot_orders'
    assert any('DROP TABLE' in query for query in queries)
    assert fence.call_count == 1
    assert logical_replication._decode_publication_fence_comment(
        set_comment.call_args_list[-1].args[2]
    ) == ('ready', None, set())


def test_empty_reconcile_remains_frozen_without_final_log_deselection(permitted_boundary_messages):
    """An ordinary import cannot discard a live migration's entire topology."""
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.server_version = 140018
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect = [
        (1,),
        (
            False, True, True, True, False, True,
            logical_replication._encode_publication_fence_comment(
                'ready', None, {('public', 'payments')}),
        ),
    ]
    cursor.fetchall.return_value = [('public', 'payments')]
    slots = {
        'pipelinewise_source_orders': _slot('wal2json'),
        'ppw_slot_orders': _slot('pgoutput'),
    }

    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, '_slot_rows', return_value=slots), pytest.raises(
                logical_replication.ReplicationSlotMigrationError,
                match='selection or options cannot change'):
        logical_replication.prepare_publication(
            _config(), [], state={'bookmarks': {}}, reconcile=True)

    assert not any(
        'ALTER PUBLICATION' in str(item.args[0]) for item in cursor.execute.call_args_list
    )


@pytest.mark.parametrize('state', [
    {'bookmarks': {'public-payments': {'lsn': 100}}},
    {'bookmarks': {}, logical_replication.PGOUTPUT_MIGRATION_STATE_KEY: {'phase': 'bridge'}},
    {'bookmarks': {}, '_pipelinewise_pgoutput_fresh_start': {'version': 1}},
])
def test_final_log_deselection_requires_durable_state_invalidation(state, permitted_boundary_messages):
    """The exceptional frozen reconcile is unavailable while LOG state is reusable."""
    with patch.object(logical_replication.post_db, 'open_connection') as connect, pytest.raises(
            logical_replication.ReplicationSlotMigrationError,
            match='persist removal of all logical bookmarks'):
        logical_replication.prepare_publication(
            _config(), [], state=state, reconcile=True, final_log_deselection=True)

    connect.assert_not_called()


@pytest.mark.parametrize('streams', [[_stream()], []], ids=['selected', 'empty'])
def test_reconcile_does_not_create_an_absent_publication(streams, permitted_boundary_messages):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.server_version = 140018
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = None

    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, '_reject_selected_generated_columns') as reject_columns, \
            patch.object(logical_replication, '_validate_publication_tables', return_value=(
                [('public', 'payments')], False, [('public', 'payments')], {}, {})) as validate_tables, \
            patch.object(logical_replication, '_validate_replica_identity') as validate_identity, \
            patch.object(logical_replication, '_slot_rows', return_value={}) as slot_rows, \
            patch.object(logical_replication, '_set_publication_comment') as set_comment, \
            patch.object(logical_replication, '_wait_for_prepublication_transactions') as fence:
        result = logical_replication.prepare_publication(
            _config(), streams, reconcile=True)

    queries = [str(item.args[0]) for item in cursor.execute.call_args_list]
    assert result == 'ppw_slot_orders'
    assert any('SELECT 1 FROM pg_catalog.pg_publication' in query for query in queries)
    assert not any('CREATE PUBLICATION' in query or 'ALTER PUBLICATION' in query for query in queries)
    reject_columns.assert_not_called()
    validate_tables.assert_not_called()
    validate_identity.assert_not_called()
    slot_rows.assert_not_called()
    set_comment.assert_not_called()
    fence.assert_not_called()


def test_reconcile_does_not_recreate_a_publication_removed_during_preflight(permitted_boundary_messages):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.server_version = 140018
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect = [(1,), None]

    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, '_reject_selected_generated_columns'), \
            patch.object(logical_replication, '_validate_publication_tables', return_value=(
                [('public', 'payments')], False, [('public', 'payments')], {}, {})), \
            patch.object(logical_replication, '_validate_replica_identity'), \
            patch.object(logical_replication, '_slot_rows', return_value={}), \
            patch.object(logical_replication, '_set_publication_comment') as set_comment, \
            patch.object(logical_replication, '_wait_for_prepublication_transactions') as fence:
        result = logical_replication.prepare_publication(
            _config(), [_stream()], reconcile=True)

    queries = [str(item.args[0]) for item in cursor.execute.call_args_list]
    assert result == 'ppw_slot_orders'
    assert not any('CREATE PUBLICATION' in query or 'ALTER PUBLICATION' in query for query in queries)
    set_comment.assert_not_called()
    fence.assert_not_called()


def test_postgres15_schema_publication_is_rejected_before_membership_changes(permitted_boundary_messages):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.server_version = 150000
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect = [
        (
            False, True, True, True, False, True,
            logical_replication._encode_publication_fence_comment(
                'ready', None, {('public', 'payments')}),
        ),
        (1,),
    ]

    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, '_reject_selected_generated_columns'), \
            patch.object(logical_replication, '_validate_publication_tables', return_value=(
                [('public', 'payments')], False, [('public', 'payments')], {}, {})), \
            patch.object(logical_replication, '_validate_replica_identity'), \
            patch.object(logical_replication, '_slot_rows', return_value={}), \
            patch.object(logical_replication, '_set_publication_comment') as set_comment, \
            patch.object(logical_replication, '_wait_for_prepublication_transactions') as fence, \
            pytest.raises(
                logical_replication.ReplicationSlotMigrationError,
                match='must not publish whole schemas'):
        logical_replication.prepare_publication(_config(), [_stream()])

    queries = [str(item.args[0]) for item in cursor.execute.call_args_list]
    assert any('pg_publication_namespace' in query for query in queries)
    assert not any('ADD TABLE' in query or 'DROP TABLE' in query for query in queries)
    set_comment.assert_not_called()
    fence.assert_not_called()


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
    ('pgoutput_overlap', False, True), ('overlap_complete', False, True),
    ('bridge', False, False), (None, True, True), (None, False, False),
])
def test_partition_preflight_respects_durable_migration_phase(phase, fresh_start, allowed, permitted_boundary_messages):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.server_version = 140018
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (
        False, True, True, True, False, True,
        logical_replication._encode_publication_fence_comment(
            'ready', None, {('public', 'payments')}))
    cursor.fetchall.return_value = [('public', 'payments')]
    migration = {'version': 2, 'phase': phase, 'source_slot': 'pipelinewise_source_orders',
                 'destination_slot': 'ppw_slot_orders', 'slot_lsn': 123, 'bridge_lsn': 200,
                 'crossover_lsn': 300, 'boundary_lsn': 124}
    state = _state(migration=migration if phase is not None else None)
    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, '_reject_selected_generated_columns'), \
            patch.object(logical_replication, '_validate_publication_tables', return_value=(
                [('public', 'payments')], True, [('public', 'payments')], {}, {})), \
            patch.object(logical_replication, '_validate_replica_identity'), \
            patch.object(logical_replication, '_slot_rows', return_value={
                'pipelinewise_source_orders': _slot('wal2json'), 'ppw_slot_orders': _slot('pgoutput')}):
        if allowed:
            assert logical_replication.prepare_publication(
                _config(), [_stream()], state=state, fresh_start=fresh_start) == 'ppw_slot_orders'
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
            return_value=logical_replication.PreparedPublication('ppw_slot_orders')), \
            patch.object(
                logical_replication,
                'locate_replication_slot',
                return_value=logical_replication.PreparedReplicationSlot(
                    'ppw_slot_orders', confirmed_flush_lsn=100)), \
            patch.object(
                logical_replication.post_db,
                'open_connection',
                return_value=connection), \
            patch.object(
                logical_replication,
                'emit_wal_progress_message',
                return_value=155), \
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
            patch.object(logical_replication, 'CHECKPOINT_INTERVAL_SECONDS', 0):
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


@pytest.mark.parametrize('publication_missing', [False, True])
def test_missing_managed_publication_history_requires_resync(publication_missing, permitted_boundary_messages):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.server_version = 140018
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = None if publication_missing else (
        False, True, True, True, False, True,
        logical_replication._encode_publication_fence_comment(
            'ready', None, {('public', 'payments')}))
    cursor.fetchall.return_value = []
    with patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, '_reject_selected_generated_columns'), \
            patch.object(logical_replication, '_validate_publication_tables', return_value=(
                [('public', 'payments')], False, [('public', 'payments')], {}, {})), \
            patch.object(logical_replication, '_validate_replica_identity'), \
            patch.object(logical_replication, '_slot_rows', return_value={
                'ppw_slot_orders': _slot('pgoutput'),
            }), \
            patch.object(logical_replication, '_set_publication_comment') as set_comment, \
            pytest.raises(logical_replication.ReplicationSlotMigrationError, match='whole-tap FastSync'):
        logical_replication.prepare_publication(_config(), [_stream()], state=_state())
    set_comment.assert_not_called()
    queries = [str(item.args[0]) for item in cursor.execute.call_args_list]
    assert not any('ADD TABLE' in query or 'CREATE PUBLICATION' in query for query in queries)


def test_interrupted_overlap_never_acknowledges_replayed_older_commits():
    migration = {
        'version': 2, 'phase': 'pgoutput_overlap',
        'source_slot': 'pipelinewise_source_orders', 'destination_slot': 'ppw_slot_orders',
        'slot_lsn': 123, 'bridge_lsn': 200, 'boundary_lsn': 124,
    }
    connection = _ReplicationConnection([
        WalMessage(json.dumps({'action': 'B'}), 124),
        WalMessage(json.dumps({'action': 'C', 'end_lsn': 160}), 160),
        RuntimeError('connection lost during replay'),
    ])
    with patch.object(logical_replication, 'prepare_publication', return_value=(
            logical_replication.PreparedPublication('ppw_slot_orders'))), \
            patch.object(logical_replication, 'locate_replication_slot', return_value=(
                logical_replication.PreparedReplicationSlot('ppw_slot_orders', confirmed_flush_lsn=123))), \
            patch.object(logical_replication.post_db, 'open_connection', return_value=connection), \
            patch.object(logical_replication, 'consume_message', side_effect=lambda _s, state, *_a, **_k: state), \
            patch.object(logical_replication.sync_common, 'send_schema_message'), \
            patch.object(logical_replication, 'FEEDBACK_POLL_INTERVAL', 0), \
            patch.object(logical_replication, 'UPDATE_BOOKMARK_PERIOD', 1), \
            patch.object(logical_replication, '_read_target_acknowledged_lsn', return_value=200) as read_ack, \
            patch.object(logical_replication.singer, 'write_message') as write_state, \
            pytest.raises(RuntimeError, match='connection lost during replay'):
        logical_replication.sync_tables(
            _config(), [_stream()], _state(lsn=200, migration=migration), 200, 'state.json')
    read_ack.assert_not_called()
    connection.cursor_instance.send_feedback.assert_called_once_with(
        write_lsn=123, flush_lsn=0, reply=True, force=True)
    assert write_state.call_args_list
    for emitted in write_state.call_args_list:
        state = emitted.args[0].value
        assert state['bookmarks']['public-payments']['lsn'] == 200
        assert state[logical_replication.PGOUTPUT_MIGRATION_STATE_KEY]['phase'] == 'pgoutput_overlap'
