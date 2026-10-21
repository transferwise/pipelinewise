"""Cover staged file integrity, archive metadata, and failed transfers."""

import base64
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from snowflake.connector.encryption_util import EncryptionMetadata, SnowflakeEncryptionUtil
from snowflake.connector.storage_client import SnowflakeFileEncryptionMaterial

from target_snowflake.upload_clients.s3_upload_client import S3UploadClient
from target_snowflake.upload_clients.snowflake_upload_client import SnowflakeUploadClient


def s3_uploader(config):
    """Keep encryption and filesystem operations real while isolating AWS traffic."""
    with patch.object(S3UploadClient, '_create_s3_client', return_value=Mock()):
        return S3UploadClient({'s3_bucket': 'test-bucket', 's3_key_prefix': 'test-prefix/', **config})


@pytest.mark.parametrize('acl', [None, 'bucket-owner-full-control'])
@pytest.mark.parametrize('encrypted', [False, True])
def test_s3_upload_preserves_source_and_round_trips_encryption(tmp_path, acl, encrypted):
    source = tmp_path / 'records.csv'
    payload = '1,Καλημέρα 日本語\n2,"quoted,value"\n'.encode('utf-8')
    source.write_bytes(payload)
    master_key = base64.b64encode(bytes(range(32))).decode() if encrypted else ''
    uploader = s3_uploader({'s3_acl': acl, 'client_side_encryption_master_key': master_key})
    uploaded_paths = []

    def inspect_upload(filename, bucket, key, ExtraArgs=None):
        assert bucket == 'test-bucket'
        assert key.startswith('test-prefix/') and key.endswith(source.name)
        assert (ExtraArgs or {}).get('ACL') == acl
        staged = Path(filename)
        uploaded_paths.append(staged)
        if encrypted:
            assert staged.read_bytes() != payload
            metadata = ExtraArgs['Metadata']
            material = SnowflakeFileEncryptionMaterial(query_stage_master_key=master_key, query_id='', smk_id=0)
            restored = SnowflakeEncryptionUtil.decrypt_file(
                EncryptionMetadata(key=metadata['x-amz-key'], iv=metadata['x-amz-iv'], matdesc=''),
                material, str(staged), tmp_dir=str(tmp_path),
            )
            assert Path(restored).read_bytes() == payload
        else:
            assert staged.read_bytes() == payload

    uploader.s3_client.upload_file.side_effect = inspect_upload
    key = uploader.upload_file(str(source), 'public-items', temp_dir=str(tmp_path))
    assert key == uploader.s3_client.upload_file.call_args.args[2]
    assert source.read_bytes() == payload
    assert uploaded_paths[0].exists() is not encrypted


@pytest.mark.parametrize('encrypted', [False, True])
def test_s3_upload_failure_keeps_source_available_for_retry(tmp_path, encrypted):
    source = tmp_path / 'records.csv'
    source.write_bytes(b'1,unchanged\n')
    master_key = base64.b64encode(bytes(range(32))).decode() if encrypted else ''
    uploader = s3_uploader({'client_side_encryption_master_key': master_key})
    uploader.s3_client.upload_file.side_effect = RuntimeError('upload failed')
    with pytest.raises(RuntimeError, match='upload failed'):
        uploader.upload_file(str(source), 'public-items', temp_dir=str(tmp_path))
    assert source.read_bytes() == b'1,unchanged\n'
    uploader.s3_client.delete_object.assert_not_called()


def test_archive_copy_preserves_encryption_metadata_and_updates_bookmark_bounds():
    uploader = s3_uploader({})
    source_metadata = {'x-amz-key': 'encrypted-key', 'x-amz-iv': 'iv', 'incremental-key-max': 'old'}
    uploader.s3_client.head_object.return_value = {'Metadata': source_metadata}
    uploader.copy_object('test-bucket/staged/nested/file.csv', 'archive-bucket', 'archive/file.csv', {
        'incremental-key-max': '12345678901234567890.123456789', 'tap': 'test-tap',
    })
    request = uploader.s3_client.copy_object.call_args.kwargs
    assert request['CopySource'] == 'test-bucket/staged/nested/file.csv'
    assert (request['Bucket'], request['Key']) == ('archive-bucket', 'archive/file.csv')
    assert request['MetadataDirective'] == 'REPLACE'
    assert request['Metadata'] == {
        'x-amz-key': 'encrypted-key', 'x-amz-iv': 'iv',
        'incremental-key-max': '12345678901234567890.123456789', 'tap': 'test-tap',
    }


def test_archive_copy_stops_when_source_metadata_cannot_be_read():
    uploader = s3_uploader({})
    uploader.s3_client.head_object.side_effect = RuntimeError('source inaccessible')
    with pytest.raises(RuntimeError, match='source inaccessible'):
        uploader.copy_object('test-bucket/file.csv', 'archive-bucket', 'file.csv', {})
    uploader.s3_client.copy_object.assert_not_called()


def test_stage_cleanup_propagates_failed_s3_deletion():
    uploader = s3_uploader({})
    uploader.s3_client.delete_object.side_effect = RuntimeError('cleanup failed')
    with pytest.raises(RuntimeError, match='cleanup failed'):
        uploader.delete_object('public-items', 'staged/file.csv')
    uploader.s3_client.delete_object.assert_called_once_with(Bucket='test-bucket', Key='staged/file.csv')


@pytest.mark.parametrize('no_compression', [False, True])
def test_table_stage_returns_uploaded_filename_and_removes_the_same_object(tmp_path, no_compression):
    source = tmp_path / 'records.csv'
    source.write_text('1,test\n')
    database = Mock()
    database.get_stage_name.return_value = 'TEST_SCHEMA.%ITEMS'
    connection = Mock()
    connection.__enter__ = Mock(return_value=connection)
    connection.__exit__ = Mock(return_value=False)
    database.open_connection.return_value = connection
    uploader = SnowflakeUploadClient({'no_compression': no_compression}, database)
    key = uploader.upload_file(str(source), 'public-items')
    assert key == source.name
    command = connection.cursor.return_value.execute.call_args.args[0]
    assert ('SOURCE_COMPRESSION=GZIP' in command) is not no_compression
    uploader.delete_object('public-items', key)
    assert connection.cursor.return_value.execute.call_args.args == ("REMOVE '@TEST_SCHEMA.%ITEMS/records.csv'",)
    assert connection.__exit__.call_count == 2
    assert source.read_text() == '1,test\n'


@pytest.mark.parametrize('no_compression', [False, True])
@pytest.mark.parametrize('operation', ['upload', 'remove'])
def test_table_stage_transfer_closes_connection_after_failure(tmp_path, no_compression, operation):
    source = tmp_path / 'records.csv'
    source.write_text('1,test\n')
    database = Mock()
    database.get_stage_name.return_value = 'TEST_SCHEMA.%ITEMS'
    connection = Mock()
    connection.__enter__ = Mock(return_value=connection)
    connection.__exit__ = Mock(return_value=False)
    connection.cursor.return_value.execute.side_effect = RuntimeError('stage unavailable')
    database.open_connection.return_value = connection
    uploader = SnowflakeUploadClient({'no_compression': no_compression}, database)
    with pytest.raises(RuntimeError, match='stage unavailable'):
        if operation == 'upload':
            uploader.upload_file(str(source), 'public-items')
        else:
            uploader.delete_object('public-items', source.name)
    connection.__exit__.assert_called_once()
    database.get_stage_name.assert_called_once_with('public-items')
    command = connection.cursor.return_value.execute.call_args.args[0]
    assert 'TEST_SCHEMA.%ITEMS' in command
    assert ('SOURCE_COMPRESSION=GZIP' in command) is (operation == 'upload' and not no_compression)
    assert source.read_text() == '1,test\n'


def test_table_stage_cannot_archive_to_s3():
    uploader = SnowflakeUploadClient({}, Mock())
    with pytest.raises(NotImplementedError, match='not supported'):
        uploader.copy_object('source/file.csv', 'archive-bucket', 'file.csv', {})
