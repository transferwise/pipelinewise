"""Protocol-level tests for the PostgreSQL pgoutput adapter."""

import struct

import pytest

from tap_postgres.pgoutput import PgoutputDecoder, PgoutputProtocolError


def _cstring(value):
    return value.encode() + b'\x00'


def _relation(relation_id=42, numeric_typmod=1179654):
    columns = (
        (1, 'id', 23, -1),
        (0, 'amount', 1700, numeric_typmod),
        (0, 'description', 25, -1),
    )
    payload = b'R' + struct.pack('!I', relation_id) + _cstring('public') + _cstring('payments') + b'd'
    payload += struct.pack('!H', len(columns))
    for flags, name, type_oid, type_modifier in columns:
        payload += struct.pack('!B', flags) + _cstring(name) + struct.pack('!Ii', type_oid, type_modifier)
    return payload


def _tuple(*values):
    payload = struct.pack('!H', len(values))
    for value in values:
        if value is None:
            payload += b'n'
        elif value is Ellipsis:
            payload += b'u'
        else:
            encoded = str(value).encode()
            payload += b't' + struct.pack('!i', len(encoded)) + encoded
    return payload


def test_decodes_non_ascii_text_as_utf8():
    decoder = PgoutputDecoder()
    decoder.decode(_relation())

    insert = decoder.decode(
        b'I' + struct.pack('!I', 42) + b'N' + _tuple(1, '12.30', 'café 東京'))

    assert insert['columns'][2]['value'] == 'café 東京'


def test_decodes_relation_insert_and_commit_with_exact_numeric_text():
    decoder = PgoutputDecoder()
    relation = decoder.decode(_relation())
    begin = decoder.decode(b'B' + struct.pack('!QqI', 500, 0, 7))
    insert = decoder.decode(b'I' + struct.pack('!I', 42) + b'N' + _tuple(1, '1234567890.1200', 'ok'))
    commit = decoder.decode(b'C' + struct.pack('!BQQq', 0, 500, 501, 0))

    assert relation['relation_columns'][1] == {
        'name': 'amount',
        'type_oid': 1700,
        'type_modifier': 1179654,
        'is_key': False,
    }
    assert begin['final_lsn'] == 500
    assert insert['transaction_lsn'] == 500
    assert insert['columns'][1]['value'] == '1234567890.1200'
    assert commit['end_lsn'] == 501


def test_decodes_transactional_logical_boundary_message():
    decoder = PgoutputDecoder()
    prefix = b'pipelinewise_orders'
    content = b'boundary-token'
    payload = (
        b'M'
        + struct.pack('!BQ', 1, 500)
        + prefix + b'\x00'
        + struct.pack('!i', len(content))
        + content
    )

    assert decoder.decode(payload) == {
        'action': 'M',
        'transactional': True,
        'message_lsn': 500,
        'prefix': 'pipelinewise_orders',
        'content': b'boundary-token',
        '_pgoutput': True,
    }


def test_unrelated_binary_logical_message_content_remains_bytes():
    decoder = PgoutputDecoder()
    prefix = b'unrelated_extension'
    content = b'\xff\x00\xfe'
    payload = (
        b'M'
        + struct.pack('!BQ', 0, 500)
        + prefix + b'\x00'
        + struct.pack('!i', len(content))
        + content
    )

    decoded = decoder.decode(payload)

    assert decoded['prefix'] == 'unrelated_extension'
    assert decoded['content'] == content
    assert decoded['transactional'] is False


def test_unchanged_toast_value_is_omitted_from_patch_update():
    decoder = PgoutputDecoder()
    decoder.decode(_relation())

    update = decoder.decode(b'U' + struct.pack('!I', 42) + b'N' + _tuple(1, '12.30', Ellipsis))

    assert [column['name'] for column in update['columns']] == ['id', 'amount']


def test_delete_decodes_replica_identity_tuple():
    decoder = PgoutputDecoder()
    decoder.decode(_relation())

    delete = decoder.decode(b'D' + struct.pack('!I', 42) + b'K' + _tuple(1, None, None))

    assert delete['identity'] == [{
        'name': 'id',
        'type_oid': 23,
        'type_modifier': -1,
        'value': '1',
    }]


def test_key_tuple_retains_explicit_null_key_and_omits_non_keys():
    decoder = PgoutputDecoder()
    decoder.decode(_relation())

    delete = decoder.decode(b'D' + struct.pack('!I', 42) + b'K' + _tuple(None, None, None))

    assert delete['identity'] == [{
        'name': 'id',
        'type_oid': 23,
        'type_modifier': -1,
        'value': None,
    }]


def test_repeated_relation_marks_typmod_change_for_next_dml_only():
    decoder = PgoutputDecoder()
    decoder.decode(_relation(numeric_typmod=655366))
    changed_relation = decoder.decode(_relation(numeric_typmod=1310730))
    first_insert = decoder.decode(b'I' + struct.pack('!I', 42) + b'N' + _tuple(1, '1.23', 'changed'))
    second_insert = decoder.decode(b'I' + struct.pack('!I', 42) + b'N' + _tuple(2, '4.56', 'stable'))

    assert changed_relation['schema_changed'] is True
    assert first_insert['schema_changed'] is True
    assert second_insert['schema_changed'] is False


def test_rejects_binary_tuple_values_when_binary_mode_is_disabled():
    decoder = PgoutputDecoder()
    decoder.decode(_relation())
    payload = b'I' + struct.pack('!I', 42) + b'N' + struct.pack('!H', 3)
    payload += b'b' + struct.pack('!i', 4) + b'\x00\x00\x00\x01' + b'n' + b'n'

    with pytest.raises(PgoutputProtocolError, match='Binary tuple values'):
        decoder.decode(payload)
