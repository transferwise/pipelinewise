"""Decode PostgreSQL logical replication protocol version 1 messages."""

import struct
from dataclasses import dataclass


class PgoutputProtocolError(ValueError):
    """Raised when a pgoutput message is malformed or unsupported."""


@dataclass(frozen=True)
class PgoutputColumn:
    """Column metadata carried by a Relation message."""

    name: str
    type_oid: int
    type_modifier: int
    is_key: bool


@dataclass(frozen=True)
class PgoutputRelation:
    """Cached metadata for a published relation."""

    relation_id: int
    schema: str
    table: str
    replica_identity: str
    columns: tuple


class _Reader:
    def __init__(self, payload, encoding):
        self.raw = payload if isinstance(payload, bytes) else bytes(payload)
        self.payload = memoryview(self.raw)
        self.encoding = encoding
        self.offset = 0

    def _read(self, size):
        end = self.offset + size
        if end > len(self.payload):
            raise PgoutputProtocolError('Truncated pgoutput message')
        value = self.payload[self.offset:end]
        self.offset = end
        return value

    def byte(self):
        return self._read(1)[0]

    def uint16(self):
        return struct.unpack('!H', self._read(2))[0]

    def int32(self):
        return struct.unpack('!i', self._read(4))[0]

    def uint32(self):
        return struct.unpack('!I', self._read(4))[0]

    def int64(self):
        return struct.unpack('!q', self._read(8))[0]

    def uint64(self):
        return struct.unpack('!Q', self._read(8))[0]

    def cstring(self):
        nul = self.raw.find(b'\x00', self.offset)
        if nul < 0:
            raise PgoutputProtocolError('Unterminated string in pgoutput message')
        value = self.raw[self.offset:nul].decode(self.encoding)
        self.offset = nul + 1
        return value

    def text(self):
        length = self.int32()
        if length < 0:
            raise PgoutputProtocolError('Negative value length in pgoutput message')
        return self._read(length).tobytes().decode(self.encoding)

    def bytes(self, length):
        return self._read(length).tobytes()

    def ensure_finished(self):
        if self.offset != len(self.payload):
            raise PgoutputProtocolError('Unexpected trailing data in pgoutput message')


class PgoutputDecoder:
    """Stateful decoder for pgoutput protocol version 1."""

    def __init__(self, encoding='utf-8'):
        self.encoding = encoding
        self.relations = {}
        self.types = {}
        self.changed_relations = set()
        self.transaction_final_lsn = None

    def decode(self, payload):
        """Decode one output-plugin payload into the tap's existing action shape."""
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise PgoutputProtocolError('pgoutput payload must be bytes')

        reader = _Reader(payload, self.encoding)
        action = chr(reader.byte())

        match action:
            case 'B':
                result = self._decode_begin(reader)
            case 'C':
                result = self._decode_commit(reader)
            case 'R':
                result = self._decode_relation(reader)
            case 'Y':
                result = self._decode_type(reader)
            case 'I':
                result = self._decode_insert(reader)
            case 'U':
                result = self._decode_update(reader)
            case 'D':
                result = self._decode_delete(reader)
            case 'T':
                result = self._decode_truncate(reader)
            case 'O':
                result = self._decode_origin(reader)
            case 'M':
                result = self._decode_message(reader)
            case _:
                raise PgoutputProtocolError(f'Unsupported pgoutput message type {action!r}')

        reader.ensure_finished()
        return result

    def _decode_begin(self, reader):
        self.transaction_final_lsn = reader.uint64()
        return {
            'action': 'B',
            'final_lsn': self.transaction_final_lsn,
            'commit_timestamp': reader.int64(),
            'xid': reader.uint32(),
            '_pgoutput': True,
        }

    def _decode_commit(self, reader):
        result = {
            'action': 'C',
            'flags': reader.byte(),
            'commit_lsn': reader.uint64(),
            'end_lsn': reader.uint64(),
            'commit_timestamp': reader.int64(),
            '_pgoutput': True,
        }
        self.transaction_final_lsn = None
        return result

    def _decode_type(self, reader):
        type_oid = reader.uint32()
        namespace = reader.cstring()
        name = reader.cstring()
        self.types[type_oid] = (namespace, name)
        return {
            'action': 'Y',
            'type_oid': type_oid,
            'schema': namespace,
            'name': name,
            '_pgoutput': True,
        }

    @staticmethod
    def _decode_truncate(reader):
        relation_count = reader.uint32()
        options = reader.byte()
        return {
            'action': 'T',
            'options': options,
            'relation_ids': [reader.uint32() for _ in range(relation_count)],
            '_pgoutput': True,
        }

    @staticmethod
    def _decode_origin(reader):
        return {
            'action': 'O',
            'origin_lsn': reader.uint64(),
            'origin': reader.cstring(),
            '_pgoutput': True,
        }

    @staticmethod
    def _decode_message(reader):
        flags = reader.byte()
        message_lsn = reader.uint64()
        prefix = reader.cstring()
        length = reader.int32()
        if length < 0:
            raise PgoutputProtocolError('Negative logical message length')
        # The prefix identifies the content format; arbitrary message bytes
        # must reach the application without text decoding.
        return {
            'action': 'M',
            'transactional': bool(flags & 1),
            'message_lsn': message_lsn,
            'prefix': prefix,
            'content': reader.bytes(length),
            '_pgoutput': True,
        }

    def _decode_relation(self, reader):
        relation_id = reader.uint32()
        schema = reader.cstring() or 'pg_catalog'
        table = reader.cstring()
        replica_identity = chr(reader.byte())
        columns = []
        for _ in range(reader.uint16()):
            flags = reader.byte()
            columns.append(PgoutputColumn(
                name=reader.cstring(),
                type_oid=reader.uint32(),
                type_modifier=reader.int32(),
                is_key=bool(flags & 1),
            ))

        relation = PgoutputRelation(
            relation_id=relation_id,
            schema=schema,
            table=table,
            replica_identity=replica_identity,
            columns=tuple(columns),
        )
        previous = self.relations.get(relation_id)
        if previous is not None and previous != relation:
            self.changed_relations.add(relation_id)
        self.relations[relation_id] = relation
        return {
            'action': 'R',
            'relation_id': relation_id,
            'schema': schema,
            'table': table,
            'relation_columns': self._column_metadata(relation),
            'schema_changed': relation_id in self.changed_relations,
            '_pgoutput': True,
        }

    def _relation(self, relation_id):
        try:
            return self.relations[relation_id]
        except KeyError as ex:
            raise PgoutputProtocolError(
                f'DML message references unknown relation {relation_id}'
            ) from ex

    @staticmethod
    def _column_metadata(relation):
        return [
            {
                'name': column.name,
                'type_oid': column.type_oid,
                'type_modifier': column.type_modifier,
                'is_key': column.is_key,
            }
            for column in relation.columns
        ]

    def _tuple(self, reader, relation, key_only=False, unchanged_columns=None):
        column_count = reader.uint16()
        if column_count != len(relation.columns):
            raise PgoutputProtocolError(
                f'Relation {relation.relation_id} has {len(relation.columns)} columns, '
                f'but tuple contains {column_count}'
            )

        values = []
        for column in relation.columns:
            kind = chr(reader.byte())
            if kind == 'n':
                value = None
            elif kind == 'u':
                if unchanged_columns is not None:
                    unchanged_columns.add(column.name)
                continue
            elif kind == 't':
                value = reader.text()
            elif kind == 'b':
                raise PgoutputProtocolError('Binary tuple values are not enabled by tap-postgres')
            else:
                raise PgoutputProtocolError(f'Unsupported tuple value type {kind!r}')
            if not key_only or column.is_key:
                values.append({
                    'name': column.name,
                    'type_oid': column.type_oid,
                    'type_modifier': column.type_modifier,
                    'value': value,
                })
        return values

    def _base_dml(self, action, relation):
        schema_changed = relation.relation_id in self.changed_relations
        self.changed_relations.discard(relation.relation_id)
        return {
            'action': action,
            'relation_id': relation.relation_id,
            'schema': relation.schema,
            'table': relation.table,
            'relation_columns': self._column_metadata(relation),
            'schema_changed': schema_changed,
            'transaction_lsn': self.transaction_final_lsn,
            '_pgoutput': True,
        }

    def _decode_insert(self, reader):
        relation = self._relation(reader.uint32())
        if chr(reader.byte()) != 'N':
            raise PgoutputProtocolError('Insert message does not contain a new tuple')
        result = self._base_dml('I', relation)
        result['columns'] = self._tuple(reader, relation)
        return result

    def _decode_update(self, reader):
        relation = self._relation(reader.uint32())
        tuple_kind = chr(reader.byte())
        old_tuple = None
        if tuple_kind in {'K', 'O'}:
            old_tuple = self._tuple(reader, relation, key_only=tuple_kind == 'K')
            tuple_kind = chr(reader.byte())
        if tuple_kind != 'N':
            raise PgoutputProtocolError('Update message does not contain a new tuple')
        result = self._base_dml('U', relation)
        unchanged_columns = set()
        result['columns'] = self._tuple(reader, relation, unchanged_columns=unchanged_columns)
        if old_tuple is not None:
            result['identity'] = old_tuple
            key_names = {column.name for column in relation.columns if column.is_key}
            result['columns'].extend(
                column for column in old_tuple
                if column['name'] in unchanged_columns and column['name'] in key_names
            )
        return result

    def _decode_delete(self, reader):
        relation = self._relation(reader.uint32())
        tuple_kind = chr(reader.byte())
        if tuple_kind not in {'K', 'O'}:
            raise PgoutputProtocolError('Delete message does not contain replica identity')
        result = self._base_dml('D', relation)
        result['identity'] = self._tuple(reader, relation, key_only=tuple_kind == 'K')
        return result
