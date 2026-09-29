"""The SQL projection is selected before either exporter can create a file."""
from unittest.mock import MagicMock, patch

import pytest

from pipelinewise.fastsync.commons import tap_mysql, tap_postgres
from pipelinewise.fastsync.commons.partial_sync_boundary import PartialSyncBoundary
from pipelinewise.fastsync.mysql_to_snowflake import tap_type_to_target_type as mysql_mapper
from pipelinewise.fastsync.postgres_to_snowflake import tap_type_to_target_type as postgres_mapper


@pytest.fixture(params=['postgres', 'mysql', 'mariadb'])
def export_source(request):
    """Retain real route type mapping and mock only database/file I/O."""
    dialect = request.param
    if dialect == 'postgres':
        source = tap_postgres.FastSyncTapPostgres({}, postgres_mapper)
        source.curr = MagicMock()
        source.curr.mogrify.return_value = ' WHERE "secret" >= \'a\''
        column = {
            'column_name': 'secret', 'data_type': 'text',
            'safe_sql_value': '"secret"', 'character_maximum_length': None,
            0: 'secret',
        }
        module = tap_postgres
        cursor = source.curr
    else:
        source = tap_mysql.FastSyncTapMySql({'engine': dialect}, mysql_mapper)
        source.conn_unbuffered = MagicMock()
        cursor = source.conn_unbuffered.cursor.return_value.__enter__.return_value
        cursor.fetchmany.return_value = []
        cursor.mogrify.return_value = " WHERE `secret` >= 'a'"
        column = {
            'column_name': 'secret', 'data_type': 'text', 'column_type': 'text',
            'safe_sql_value': "REPLACE(CAST(`secret` AS CHAR CHARACTER SET utf8mb4), CHAR(0), '')",
        }
        module = tap_mysql
    source.get_table_columns = MagicMock(return_value=[column])
    source.source_transformations = {'transformations': [{
        'tap_stream_name': 'public-secrets', 'field_id': 'secret', 'type': 'HASH',
    }]}
    return source, module, cursor


def test_transformed_select_is_used_before_export(export_source, tmp_path):
    """FullSync and PartialSync share the transformed COPY/SELECT export seam."""
    source, module, cursor = export_source
    transformed = "SELECT 'transformed-only' AS secret"
    boundary = PartialSyncBoundary('secret', 'a')
    with patch.object(module, 'compile_source_select', return_value=transformed) as compiler, \
            patch.object(module.split_gzip, 'open') as output:
        source.copy_table('public.secrets', str(tmp_path / 'out.csv.gz'), boundary=boundary)

    compiler.assert_called_once()
    assert compiler.call_args.args[0] == 'public.secrets'
    assert 'WHERE' in compiler.call_args.args[2]
    assert compiler.call_args.args[4] is source.source_transformations
    execute = cursor.copy_expert if module is tap_postgres else cursor.execute
    sql = execute.call_args.args[0]
    assert f'FROM ({transformed}) AS _ppw_export' in sql
    assert '_SDC_DELETED_AT' in sql
    output.assert_called_once()


def test_unsupported_transformation_never_opens_export_file(export_source, tmp_path):
    """Invalid rules cannot fall back to writing the source column unchanged."""
    source, module, cursor = export_source
    source.source_transformations['transformations'][0]['type'] = 'UNKNOWN'
    with patch.object(module.split_gzip, 'open') as output, pytest.raises(ValueError):
        source.copy_table('public.secrets', str(tmp_path / 'out.csv.gz'))

    output.assert_not_called()
    cursor.copy_expert.assert_not_called()
    cursor.execute.assert_not_called()
    assert not list(tmp_path.iterdir())


def test_non_snowflake_sources_do_not_enable_projection():
    """The shared tap classes retain other targets' existing transformation path."""
    assert tap_postgres.FastSyncTapPostgres({}, postgres_mapper).source_transformations is None
    assert tap_mysql.FastSyncTapMySql({}, mysql_mapper).source_transformations is None


def test_transformed_incremental_key_is_rejected_before_reading_raw_bookmark(export_source):
    """Checkpoint preparation cannot leak a masked source value into state."""
    source, _module, _cursor = export_source
    with patch.object(source, 'query') as query, pytest.raises(ValueError, match='INCREMENTAL'):
        source.fetch_current_incremental_key_pos('public.secrets', 'SECRET')
    query.assert_not_called()
