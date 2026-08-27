import os

from tempfile import TemporaryDirectory
from unittest import mock

import pytest

from pipelinewise.fastsync.commons import utils
from pipelinewise.fastsync.commons.target_snowflake import FastSyncTargetSnowflake


@pytest.mark.parametrize(
    ('resolved_keys', 'message'),
    (
        (['loads/part-0', 'loads/part-0'], 'must be unique'),
        (
            ['loads/part-0', 'loads/part-0', None],
            'requires deterministic S3 keys',
        ),
    ),
)
def test_s3_key_error_precedence(
    resolved_keys,
    message,
):
    """A missing key remains more important than an earlier duplicate."""
    snowflake = mock.MagicMock()
    snowflake._get_s3_key.side_effect = resolved_keys  # pylint: disable=protected-access
    file_parts = [f'part-{index}' for index in range(len(resolved_keys))]

    with pytest.raises(ValueError, match=message):
        utils.get_expected_s3_keys(snowflake, file_parts)

    assert snowflake._get_s3_key.call_args_list == [  # pylint: disable=protected-access
        mock.call(file_part) for file_part in file_parts
    ]


def test_later_upload_failure_rolls_back():
    """A failed later part removes prior uploads and preserves local exports."""
    with TemporaryDirectory() as temp_directory:
        file_parts = [
            os.path.join(temp_directory, 'export.part0'),
            os.path.join(temp_directory, 'export.part1'),
        ]
        for file_part in file_parts:
            with open(file_part, 'w', encoding='utf8') as export_file:
                export_file.write('data')

        snowflake = object.__new__(FastSyncTargetSnowflake)
        snowflake.connection_config = {'s3_key_prefix': 'loads/'}
        snowflake.s3 = mock.MagicMock()
        snowflake.upload_to_s3 = mock.Mock(side_effect=[
            'loads/export.part0',
            RuntimeError('second upload failed'),
        ])

        with pytest.raises(RuntimeError, match='second upload failed'):
            utils.upload_files_to_s3(
                snowflake, file_parts, temp_directory, 'staging-bucket'
            )

        assert all(os.path.exists(path) for path in file_parts)
        assert snowflake.s3.delete_object.call_args_list == [
            mock.call(Bucket='staging-bucket', Key='loads/export.part0'),
            mock.call(Bucket='staging-bucket', Key='loads/export.part1'),
        ]


def test_atomic_save_preserves_valid_state():
    """A partial temporary write cannot truncate the last valid state file."""
    with TemporaryDirectory() as temp_directory:
        state_path = os.path.join(temp_directory, 'state.json')
        original_state = '{"bookmarks": {"source-table": {"position": 1}}}\n'
        with open(state_path, 'w', encoding='utf-8') as state_file:
            state_file.write(original_state)

        def fail_after_partial_write(_data, file_handle, **_kwargs):
            file_handle.write('{"bookmarks":')
            raise TypeError('not serializable')

        with mock.patch.object(
            utils.json, 'dump', side_effect=fail_after_partial_write
        ), pytest.raises(TypeError, match='not serializable'):
            utils.save_dict_to_json(state_path, {'not': object()})

        with open(state_path, encoding='utf-8') as state_file:
            assert state_file.read() == original_state
        assert not any(
            name.startswith('.state.json.') and name.endswith('.tmp')
            for name in os.listdir(temp_directory)
        )


def test_state_rename_fsyncs_directory():
    """The durable rename is flushed only after the new state path exists."""
    with TemporaryDirectory() as temp_directory:
        state_path = os.path.join(temp_directory, 'state.json')
        timeline = []
        replace = os.replace

        def replace_and_record(source, target):
            replace(source, target)
            timeline.append('replace')

        with mock.patch.object(
            utils.os,
            'replace',
            side_effect=replace_and_record,
        ), mock.patch.object(
            utils,
            '_fsync_directory',
            side_effect=lambda directory: timeline.append(('fsync', directory)),
        ):
            utils.save_dict_to_json(state_path, {'bookmark': 1})

        assert timeline == ['replace', ('fsync', temp_directory)]


def test_transient_grants_are_retried():
    """A transient post-publication grant failure does not force republish."""
    with mock.patch.object(
        utils,
        'apply_snowflake_table_grants',
        side_effect=[RuntimeError('transient failure'), None],
    ) as apply_mock:
        utils.retry_snowflake_table_grants(
            mock.sentinel.snowflake,
            {'default_target_schema_select_permissions': ['role']},
            'TARGET_SCHEMA',
            'source.table',
        )

    assert apply_mock.call_count == 2
