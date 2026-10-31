"""Planning and classification of a tap's tables for native-to-Iceberg conversion."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest
import snowflake.connector as snowflake_connector

from pipelinewise.fastsync.commons import snowflake_iceberg_conversion_plan as plan
from pipelinewise.fastsync.commons.snowflake_iceberg_converter import EVENTUAL_ICEBERG
from pipelinewise.fastsync.commons.snowflake_iceberg_coordination import RecoveryCoordinator
from pipelinewise.fastsync.commons.snowflake_iceberg_inspection import (
    SHOW_COMMAND_ROW_LIMIT,
    SnowflakeTableInspector,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_model import (
    IcebergColumn,
    IcebergTableSpec,
    SnowflakeObjectName,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_recovery import (
    TableFormatDiscoveryError,
)
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    FakeSnowflake as ScriptedSnowflake,
    make_attempt,
)
from tests.units.fastsync.commons.test_snowflake_iceberg_converter import (
    FakeSnowflake,
    _converter,
    _manifest_files,
)


TARGET_CONFIG = {
    'dbname': 'database',
    'schema_mapping': {'public': {'target_schema': 'schema'}},
}


def _stream(stream_id, properties=None, selected=True):
    return {
        'tap_stream_id': stream_id,
        'schema': {'properties': properties or {}},
        'metadata': [{'breadcrumb': [], 'metadata': {'selected': selected}}],
    }


def _row(name, is_iceberg=False, rows='10', bytes_='2048'):
    return {'name': name, 'is_iceberg': 'Y' if is_iceberg else 'N', 'rows': rows, 'bytes': bytes_}


def _classify(tmp_path, targets, listing):
    snowflake = ScriptedSnowflake([listing])
    classified = plan.classify_conversion_targets(
        targets,
        SnowflakeTableInspector(snowflake),
        RecoveryCoordinator(str(tmp_path)),
    )
    return snowflake, classified


def _target(table):
    return plan.ConversionTarget(f'public-{table.lower()}', SnowflakeObjectName('DATABASE', 'SCHEMA', table))


@pytest.mark.parametrize(
    ('stream_id', 'config', 'expected'),
    [
        ('public-orders', TARGET_CONFIG, ('DATABASE', 'SCHEMA', 'ORDERS')),
        ('orders', {'dbname': 'database', 'default_target_schema': ' raw '}, ('DATABASE', 'RAW', 'ORDERS')),
        ('db-public-order_items', TARGET_CONFIG, ('DATABASE', 'SCHEMA', 'ORDER_ITEMS')),
        ('public-my.table', TARGET_CONFIG, ('DATABASE', 'SCHEMA', 'MY_TABLE')),
        (
            'other-orders',
            {**TARGET_CONFIG, 'default_target_schema': 'fallback'},
            ('DATABASE', 'FALLBACK', 'ORDERS'),
        ),
        ('public-orders', {**TARGET_CONFIG, 'dbname': '"Mixed""Db"'}, ('Mixed"Db', 'SCHEMA', 'ORDERS')),
    ],
)
def test_singer_target_table_mirrors_target_snowflake(stream_id, config, expected):
    """Stream parsing, schema mapping, and table normalization match the connector."""
    table = plan.singer_target_table(stream_id, config)

    assert (table.database, table.schema, table.table) == expected


def test_missing_target_schema_blocks_planning():
    """A stream without a configured target schema cannot be planned."""
    targets = plan.plan_conversion_targets(
        {'streams': [_stream('other-orders')]},
        TARGET_CONFIG,
        ['other-orders'],
        fastsync_capable=False,
    )

    assert targets[0].table is None
    assert 'No target schema is configured for stream other-orders' in targets[0].blockers[0]


@pytest.mark.parametrize('fastsync_capable', [True, False])
def test_writer_name_disagreement_blocks_only_fastsync_taps(fastsync_capable):
    """A stream whose FastSync and Singer tables differ cannot be converted safely."""
    targets = plan.plan_conversion_targets(
        {'streams': [_stream('public-my.table')]},
        {**TARGET_CONFIG, 'default_target_schema': 'fallback'},
        ['public-my.table'],
        fastsync_capable,
    )

    assert targets[0].table.key == 'DATABASE.SCHEMA.MY_TABLE'
    if fastsync_capable:
        assert targets[0].blockers == (
            'FastSync writes DATABASE.FALLBACK.TABLE but Singer writes DATABASE.SCHEMA.MY_TABLE; '
            'conversion cannot choose between them',
        )
    else:
        assert targets[0].blockers == ()


def test_plan_records_catalog_text_and_variant_columns():
    """Level-0 catalog properties become the TEXT/VARIANT classes the writers expect."""
    catalog = {'streams': [_stream('public-orders', {
        'id': {'type': ['integer']},
        'note': {'type': ['null', 'string']},
        'attributes': {'type': ['null', 'object'], 'properties': {}},
        'updated_at': {'type': ['null', 'string'], 'format': 'date-time'},
    })]}

    targets = plan.plan_conversion_targets(catalog, TARGET_CONFIG, ['public-orders'], True)

    assert targets[0].writer_types == {'NOTE': plan.TEXT, 'ATTRIBUTES': plan.VARIANT}


def test_selected_stream_ids_use_root_metadata():
    """Only streams selected at the root breadcrumb are planned."""
    catalog = {'streams': [
        _stream('public-orders'),
        _stream('public-users', selected=False),
        {'tap_stream_id': 'public-events', 'metadata': []},
    ]}

    assert plan.selected_stream_ids(catalog) == ['public-orders']


def test_text_variant_blockers_compare_native_classes():
    """A native TEXT column the catalog now maps to VARIANT is reported, and vice versa."""
    columns = [
        SimpleNamespace(name='ATTRIBUTES', data_type='TEXT'),
        SimpleNamespace(name='PAYLOAD', data_type='VARIANT'),
        SimpleNamespace(name='NOTE', data_type='TEXT'),
        SimpleNamespace(name='ID', data_type='NUMBER'),
    ]
    writer_types = {'ATTRIBUTES': plan.VARIANT, 'PAYLOAD': plan.TEXT, 'NOTE': plan.TEXT, 'ID': plan.TEXT}

    assert plan.text_variant_blockers(columns, writer_types) == [
        'Column ATTRIBUTES is TEXT but the tap catalog maps it to VARIANT; '
        'the Iceberg writers would refuse the converted table',
        'Column PAYLOAD is VARIANT but the tap catalog maps it to TEXT; '
        'the Iceberg writers would refuse the converted table',
    ]


def test_classification_uses_one_listing_per_schema(tmp_path):
    """Each planned table is classified from one SHOW TABLES and no local locks."""
    listing = [
        _row('ORDERS'),
        _row('USERS', is_iceberg=True, rows='5', bytes_='1024'),
        _row('STAGED'),
        _row('STAGED_ICEBERG', is_iceberg=True),
        _row('BACKED'),
        _row('BACKED_NATIVE'),
        _row('GHOST_NATIVE'),
    ]
    targets = [_target(name) for name in ('ORDERS', 'USERS', 'NEW_TABLE', 'STAGED', 'BACKED', 'GHOST')]

    snowflake, classified = _classify(tmp_path, targets, listing)

    assert [query for query, _, _ in snowflake.queries] == [
        'SHOW TABLES IN SCHEMA "DATABASE"."SCHEMA" LIMIT 10000'
    ]
    assert [(item.target.table.table, item.state, item.primary) for item in classified] == [
        ('ORDERS', plan.STATE_NATIVE, 'native'),
        ('USERS', plan.STATE_ICEBERG, 'iceberg'),
        ('NEW_TABLE', plan.STATE_NEW, 'missing'),
        ('STAGED', plan.STATE_BLOCKED, 'native'),
        ('BACKED', plan.STATE_BLOCKED, 'native'),
        ('GHOST', plan.STATE_BLOCKED, 'missing'),
    ]
    assert (classified[1].rows, classified[1].bytes) == (5, 1024)
    assert (classified[2].rows, classified[2].bytes) == (None, None)
    assert 'STAGED_ICEBERG" already exists without a conversion manifest' in classified[3].reasons[0]
    assert 'BACKED_NATIVE" already exists' in classified[4].reasons[0]
    assert 'companion exists without a conversion manifest' in classified[5].reasons[0]
    assert not list(tmp_path.iterdir())


def test_fastsync_rebuilds_a_missing_table_beside_leftover_companions(tmp_path):
    """FastSync reloads a missing table from its source, so companions do not block it."""
    snowflake = ScriptedSnowflake([[_row('GHOST_NATIVE'), _row('GHOST_ICEBERG', is_iceberg=True)]])

    classified = plan.classify_conversion_targets(
        [_target('GHOST')],
        SnowflakeTableInspector(snowflake),
        RecoveryCoordinator(str(tmp_path)),
        fastsync_rebuild=True,
    )

    assert classified[0].state == plan.STATE_NEW


def test_missing_schema_classifies_every_table_as_new(tmp_path):
    """Tables in a schema that does not exist yet are created by the writers."""
    snowflake = ScriptedSnowflake([
        snowflake_connector.errors.ProgrammingError(msg='Schema does not exist', errno=2043),
        [],
    ])

    classified = plan.classify_conversion_targets(
        [_target('ORDERS')],
        SnowflakeTableInspector(snowflake),
        RecoveryCoordinator(str(tmp_path)),
    )

    assert classified[0].state == plan.STATE_NEW


@pytest.mark.parametrize(
    ('eventual', 'state'),
    [(EVENTUAL_ICEBERG, plan.STATE_PENDING), ('native', plan.STATE_BLOCKED)],
)
def test_pending_manifest_classification(tmp_path, eventual, state):
    """An eventual=iceberg manifest resumes; an eventual=native one needs the operator."""
    snowflake = FakeSnowflake()
    snowflake.staging_evidence = (2, 54321)
    with pytest.raises(Exception, match='does not match'):
        _converter(tmp_path, snowflake).convert('database.schema.table', eventual=eventual)
    assert len(_manifest_files(tmp_path)) == 1

    _, classified = _classify(
        tmp_path,
        [_target('TABLE')],
        [_row('TABLE'), _row('TABLE_ICEBERG', is_iceberg=True)],
    )

    assert classified[0].state == state
    if state == plan.STATE_BLOCKED:
        assert 'with --eventual native is pending' in classified[0].reasons[0]


def test_unreadable_manifest_blocks(tmp_path):
    """Recovery state that cannot be read stops classification for that table."""
    snowflake = FakeSnowflake()
    snowflake.staging_evidence = (2, 54321)
    with pytest.raises(Exception, match='does not match'):
        _converter(tmp_path, snowflake).convert('database.schema.table')
    _manifest_files(tmp_path)[0].write_text('{', encoding='utf-8')

    _, classified = _classify(tmp_path, [_target('TABLE')], [_row('TABLE')])

    assert classified[0].state == plan.STATE_BLOCKED
    assert 'Cannot read Iceberg recovery manifest' in classified[0].reasons[0]


def test_fastsync_pointer_on_native_table_blocks(tmp_path):
    """A pending FastSync attempt on a native table is inconsistent and needs inspection."""
    store = SimpleNamespace(peek=lambda: (None, object()))
    with patch.object(RecoveryCoordinator, 'recovery_store', return_value=store):
        _, classified = _classify(tmp_path, [_target('TABLE')], [_row('TABLE')])

    assert classified[0].state == plan.STATE_BLOCKED
    assert 'FastSync Iceberg attempt is pending' in classified[0].reasons[0]


def test_truncated_schema_listing_falls_back_to_each_table(tmp_path):
    """A schema at Snowflake's SHOW limit is read per table, including its companions."""
    truncated = [{'name': f'T{index}', 'is_iceberg': 'N'} for index in range(SHOW_COMMAND_ROW_LIMIT)]
    snowflake = ScriptedSnowflake([truncated, [_row('ORDERS'), _row('ORDERS_ICEBERG', is_iceberg=True)], []])

    classified = plan.classify_conversion_targets(
        [_target('ORDERS'), _target('USERS')],
        SnowflakeTableInspector(snowflake),
        RecoveryCoordinator(str(tmp_path)),
    )

    assert [query for query, _, _ in snowflake.queries] == [
        'SHOW TABLES IN SCHEMA "DATABASE"."SCHEMA" LIMIT 10000',
        'SHOW TABLES IN SCHEMA "DATABASE"."SCHEMA" STARTS WITH \'ORDERS\' LIMIT 10000',
        'SHOW TABLES IN SCHEMA "DATABASE"."SCHEMA" STARTS WITH \'USERS\' LIMIT 10000',
    ]
    assert [item.state for item in classified] == [plan.STATE_BLOCKED, plan.STATE_NEW]


def test_truncated_table_listing_fails_closed(tmp_path):
    """A table whose own prefix listing is truncated cannot be classified safely."""
    truncated = [{'name': f'T{index}', 'is_iceberg': 'N'} for index in range(SHOW_COMMAND_ROW_LIMIT)]
    inspector = SnowflakeTableInspector(ScriptedSnowflake([truncated, truncated]))

    with pytest.raises(TableFormatDiscoveryError, match='too many tables starting with'):
        plan.classify_conversion_targets([_target('T')], inspector, RecoveryCoordinator(str(tmp_path)))


def test_peek_reads_recovery_state_without_a_lock(tmp_path):
    """Read-only reports see the manifest without creating lock files."""
    snowflake = FakeSnowflake()
    snowflake.staging_evidence = (2, 54321)
    with pytest.raises(Exception, match='does not match'):
        _converter(tmp_path, snowflake).convert('database.schema.table')
    for lock in tmp_path.glob('*.lock'):
        lock.unlink()

    store = RecoveryCoordinator(str(tmp_path)).recovery_store(SnowflakeObjectName('DATABASE', 'SCHEMA', 'TABLE'))
    manifest, pointer = store.peek()

    assert manifest.kind == 'manual_conversion'
    assert pointer is None
    assert not list(tmp_path.glob('*.lock'))


def test_companion_names_of_planned_tables_block_conversion(tmp_path):
    """Two streams whose tables are T and T_NATIVE can never convert into each other."""
    _, classified = _classify(
        tmp_path,
        [_target('ADS'), _target('ADS_NATIVE')],
        [_row('ADS'), _row('ADS_NATIVE')],
    )

    assert [item.state for item in classified] == [plan.STATE_BLOCKED, plan.STATE_BLOCKED]
    assert classified[0].reasons[0].startswith(
        '"DATABASE"."SCHEMA"."ADS_NATIVE" is the table of stream public-ads_native'
    )
    assert classified[1].reasons[0].startswith(
        '"DATABASE"."SCHEMA"."ADS_NATIVE" uses the _NATIVE name that conversion reserves'
    )


def test_a_retained_backup_selected_later_is_never_converted(tmp_path):
    """A new stream that maps onto another table's native backup is blocked, not converted."""
    _, classified = _classify(
        tmp_path,
        [_target('ADS_NATIVE')],
        [_row('ADS', is_iceberg=True), _row('ADS_NATIVE')],
    )

    assert classified[0].state == plan.STATE_BLOCKED
    assert 'reserves for "DATABASE"."SCHEMA"."ADS"' in classified[0].reasons[0]


def test_a_native_iceberg_named_table_is_never_called_stale(tmp_path):
    """A native table at the _ICEBERG name is not a conversion copy, so it is never dropped."""
    _, classified = _classify(tmp_path, [_target('EVENTS')], [_row('EVENTS'), _row('EVENTS_ICEBERG')])

    assert classified[0].state == plan.STATE_BLOCKED
    assert classified[0].reasons == (
        '"DATABASE"."SCHEMA"."EVENTS_ICEBERG" is a native table, and conversion needs that name '
        'for its Iceberg copy. Find out what it holds before renaming it',
    )


@pytest.mark.parametrize(
    ('listing', 'expected'),
    [([_row('TABLE', is_iceberg=True)], plan.STATE_ICEBERG), ([], plan.STATE_NEW)],
)
@pytest.mark.parametrize('fastsync_rebuild', [True, False])
def test_interrupted_fastsync_publication_can_resume(tmp_path, listing, expected, fastsync_rebuild):
    """A FastSync pointer beside a missing or Iceberg primary never blocks its own resume."""
    spec = IcebergTableSpec(
        SnowflakeObjectName('DATABASE', 'SCHEMA', 'TABLE'),
        (IcebergColumn('ID', 'NUMBER(38,0)', False),),
        ('ID',),
    )
    RecoveryCoordinator(str(tmp_path)).persist_new_attempt(make_attempt(spec))
    store = RecoveryCoordinator(str(tmp_path)).recovery_store(spec.name)
    assert store.peek()[1] is not None

    classified = plan.classify_conversion_targets(
        [_target('TABLE')],
        SnowflakeTableInspector(ScriptedSnowflake([listing])),
        RecoveryCoordinator(str(tmp_path)),
        fastsync_rebuild=fastsync_rebuild,
    )

    assert classified[0].state == expected
