"""Exercise source-side FastSync privacy without a Snowflake connection."""

import csv
import gzip
import hashlib
import io
import os
import re
from argparse import Namespace
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

import pytest
from dotenv import load_dotenv

from pipelinewise.fastsync import mysql_to_snowflake, postgres_to_snowflake
from pipelinewise.fastsync.commons.partial_sync_boundary import PartialSyncBoundary
from pipelinewise.fastsync.commons.snowflake_iceberg_routes import validate_route_config
from pipelinewise.fastsync.commons.transform_utils import TransformationType
from pipelinewise.fastsync.partialsync import compatibility_report


RAW_SECRET = 'privacy/raw-value-must-not-leave-source/雪🚀\n\r\t'
TRANSFORMATIONS = [transformation.value for transformation in TransformationType]
SOURCE_MODES = [
    (engine, iceberg)
    for engine in ('postgres', 'mysql', 'mariadb')
    for iceberg in (False, True)
]


def _sha256(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def _expected_transformation(transformation, value):
    if transformation == 'SET-NULL':
        return None
    if transformation == 'MASK-HIDDEN':
        return 'hidden'
    if transformation == 'MASK-NUMBER':
        return '0'
    if value is None:
        return None
    if transformation == 'MASK-DATE':
        return value.replace(month=1, day=1)
    if transformation == 'HASH':
        return _sha256(value)
    width = int(transformation[-1])
    if transformation.startswith('HASH-SKIP-FIRST-'):
        return value[:width] + _sha256(value[width:])
    if len(value) <= width * 2:
        return '*' * len(value)
    return value[:width] + '*' * (len(value) - width * 2) + value[-width:]


@dataclass
class _SourceExport:
    source: object
    adapter: object
    engine: str
    iceberg: bool
    schema: str
    table_name: str

    @property
    def table(self):
        return f'{self.schema}.{self.table_name}'

    def quoted(self, value):
        quote = '"' if self.engine == 'postgres' else '`'
        return quote + value.replace(quote, quote * 2) + quote

    @property
    def quoted_table(self):
        return f'{self.quoted(self.schema)}.{self.quoted(self.table_name)}'

    def execute(self, statement, parameters=None):
        with self.source.conn.cursor() as cursor:
            cursor.execute(statement, parameters)
        self.source.conn.commit()

    def create(self, columns, rows):
        definitions = ', '.join(
            f'{self.quoted(name)} {column_type}'
            for name, column_type in columns
        )
        suffix = '' if self.engine == 'postgres' else ' ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
        self.execute(f'CREATE TABLE {self.quoted_table} ({definitions}){suffix}')
        placeholders = ', '.join('%s' for _ in columns)
        with self.source.conn.cursor() as cursor:
            cursor.executemany(
                f'INSERT INTO {self.quoted_table} VALUES ({placeholders})', rows,
            )
        self.source.conn.commit()

    def configure(self, transformations):
        self.source.source_transformations = {
            'transformations': [
                {'tap_stream_name': f'{self.schema}-{self.table_name}', **transformation}
                for transformation in transformations
            ],
        }

    def export(self, path, boundary=None):
        self.source.copy_table(self.table, str(path), boundary=boundary)
        with gzip.open(path, 'rt', encoding='utf-8', newline='') as exported_file:
            contents = exported_file.read()
        rows = list(csv.reader(io.StringIO(contents, newline=''), strict=True))
        return contents, rows


@pytest.fixture(params=SOURCE_MODES, ids=lambda mode: f'{mode[0]}-{"v3" if mode[1] else "native"}')
def source_export(request):
    """Create only uniquely named tables; leave shared E2E fixtures intact."""
    load_dotenv('dev-project/.env')
    engine, iceberg = request.param
    prefix = {
        'postgres': 'TAP_POSTGRES',
        'mysql': 'TAP_ORACLE_MYSQL',
        'mariadb': 'TAP_MYSQL',
    }[engine]
    fields = ('HOST', 'PORT', 'USER', 'PASSWORD', 'DB')
    missing = [f'{prefix}_{field}' for field in fields if not os.environ.get(f'{prefix}_{field}')]
    if missing:
        pytest.skip(f'Missing dev source configuration: {", ".join(missing)}')
    config = {field.lower(): os.environ[f'{prefix}_{field}'] for field in fields}
    config['dbname'] = config.pop('db')
    config['engine'] = engine
    route = postgres_to_snowflake if engine == 'postgres' else mysql_to_snowflake
    adapter = route._source_adapter()
    target = (
        {'target_table_format': 'iceberg', 'iceberg_version': 3, 'data_flattening_max_level': 0}
        if iceberg
        else {'target_table_format': 'native'}
    )
    args = Namespace(tap=config, target=target, transform={'transformations': []})
    # Validate the fixture target the way both runners do, so it cannot drift into a
    # shape production would reject, and hand the adapter the version that came back.
    source = adapter.create(args, validate_route_config(target))
    adapter.open(source)
    fixture = _SourceExport(
        source, adapter, engine, iceberg,
        'public' if engine == 'postgres' else config['dbname'],
        f'ppw_transform_{uuid4().hex[:12]}',
    )
    try:
        yield fixture
    finally:
        try:
            if engine != 'postgres':
                source.conn_unbuffered.rollback()
            fixture.execute(f'DROP TABLE IF EXISTS {fixture.quoted_table}')
        finally:
            adapter.close_finally(source)


def test_all_transformations_export_only_transformed_values(source_export, tmp_path):
    """Pin all transformations, short-string edges, Unicode, NULL and fractions."""
    date_type = 'TIMESTAMP(6)' if source_export.engine == 'postgres' else 'DATETIME(6)'
    columns = [('id', 'INTEGER PRIMARY KEY'), ('untouched_text', 'TEXT')]
    for index, transformation in enumerate(TRANSFORMATIONS):
        data_type = date_type if transformation == 'MASK-DATE' else (
            'INTEGER' if transformation == 'MASK-NUMBER' else 'TEXT'
        )
        columns.append((f'value_{index}', data_type))

    values = [None, '', *('x' * length for length in range(1, 20)), RAW_SECRET]
    timestamp = datetime(2024, 8, 19, 23, 59, 58, 123456)
    rows = []
    expected = {}
    for row_id, value in enumerate(values):
        public_text = None if row_id == 0 else '' if row_id == 1 else 'public'
        raw_values = [
            (timestamp if value is not None else None) if transformation == 'MASK-DATE'
            else (123 if value is not None else None) if transformation == 'MASK-NUMBER'
            else value
            for transformation in TRANSFORMATIONS
        ]
        rows.append((row_id, public_text, *raw_values))
        expected[row_id] = [
            _expected_transformation(transformation, raw_value)
            for transformation, raw_value in zip(TRANSFORMATIONS, raw_values)
        ]
    source_export.create(columns, rows)
    if not source_export.iceberg:
        tap_type = 'tap-postgres' if source_export.engine == 'postgres' else 'tap-mysql'
        config = source_export.source.connection_config
        with compatibility_report.source_connection(tap_type, config) as connection:
            report_source = compatibility_report.native_source(connection, tap_type, config, {})
            assert compatibility_report.mapped_source_columns(connection, report_source, source_export.table) == (
                source_export.source.map_column_types_to_target(source_export.table)['columns']
            )
    source_export.configure([
        {'field_id': f'value_{index}', 'type': transformation}
        for index, transformation in enumerate(TRANSFORMATIONS)
    ])

    contents, exported_rows = source_export.export(tmp_path / 'all_transformations.csv.gz')

    assert RAW_SECRET not in contents
    assert len(exported_rows) == len(rows)
    assert re.search(r'^"?0"?,,', contents, re.MULTILINE)
    assert re.search(r'^"?1"?,"",', contents, re.MULTILINE)
    for exported in exported_rows:
        assert len(exported) == len(columns) + 3
        row_id = int(exported[0])
        for transformation, actual, wanted in zip(TRANSFORMATIONS, exported[2:], expected[row_id]):
            if transformation == 'MASK-DATE' and wanted is not None:
                assert datetime.fromisoformat(actual) == wanted
            else:
                assert actual == ('' if wanted is None else str(wanted))


def test_conditions_keep_legacy_order_and_case_sensitive_matching(source_export, tmp_path):
    """Conditional stages see prior stages before the unconditional projection."""
    columns = [
        ('id', 'INTEGER PRIMARY KEY'), ('gate', 'TEXT'), ('Mixed Secret', 'TEXT'),
        ('nullable', 'TEXT'), ('conditional_number', 'INTEGER'),
        ('match_text', 'TEXT'), ('regex_secret', 'TEXT'), ('empty_secret', 'TEXT'),
    ]
    rows = [
        (1, "O'Brien雪", RAW_SECRET, 'remove', 17, 'A12', RAW_SECRET, ''),
        (2, 'other', RAW_SECRET, None, 17, 'xxA12xx', RAW_SECRET, ''),
        (3, 'other', RAW_SECRET, None, 17, 'a12', RAW_SECRET, ''),
    ]
    source_export.create(columns, rows)
    source_export.configure([
        {'field_id': 'gate', 'type': 'HASH'},
        {'field_id': 'nullable', 'type': 'SET-NULL', 'when': [{'column': 'gate', 'equals': "O'Brien雪"}]},
        {'field_id': 'conditional_number', 'type': 'MASK-NUMBER', 'when': [{'column': 'nullable', 'equals': None}]},
        {'field_id': 'Mixed Secret', 'type': 'HASH'},
        {
            'field_id': 'regex_secret', 'type': 'MASK-HIDDEN',
            'when': [{'column': 'match_text', 'regex_match': 'A[0-9]{2}'}],
        },
        {'field_id': 'empty_secret', 'type': 'MASK-HIDDEN', 'when': [{'column': 'empty_secret', 'equals': ''}]},
        {'field_id': 'regex_secret', 'type': 'HASH'},
    ])

    contents, exported_rows = source_export.export(tmp_path / 'conditions.csv.gz')

    assert RAW_SECRET not in contents
    actual = {int(row[0]): row[:len(columns)] for row in exported_rows}
    for row_id, gate, *_ in rows:
        assert actual[row_id][1:] == [
            _sha256(gate), _sha256(RAW_SECRET), '', '0',
            rows[row_id - 1][5],
            _sha256('hidden' if row_id == 1 else RAW_SECRET), 'hidden',
        ]


def test_partial_export_filters_raw_inclusive_boundary_before_masking(source_export, tmp_path):
    """Masking a boundary column cannot change which source rows are exported."""
    source_export.create(
        [('id', 'INTEGER PRIMARY KEY'), ('boundary_value', 'INTEGER'), ('secret', 'TEXT')],
        [(row_id, row_id, f'{RAW_SECRET}/{row_id}') for row_id in range(1, 6)],
    )
    source_export.configure([
        {'field_id': 'boundary_value', 'type': 'MASK-NUMBER'},
        {'field_id': 'secret', 'type': 'HASH'},
    ])

    contents, exported_rows = source_export.export(
        tmp_path / 'partial.csv.gz',
        PartialSyncBoundary('boundary_value', 2, 4),
    )

    assert RAW_SECRET not in contents
    assert sorted(row[:3] for row in exported_rows) == [
        [str(row_id), '0', _sha256(f'{RAW_SECRET}/{row_id}')]
        for row_id in (2, 3, 4)
    ]


def test_normalized_values_are_transformed_before_export(source_export, tmp_path):
    """Retain CHAR padding, date normalization and integer regex semantics."""
    postgres = source_export.engine == 'postgres'
    if not postgres:
        source_export.execute("SET SESSION sql_mode = ''")
    raw_text = 'prefix\r\n雪🚀' if postgres else 'prefix\x00\r\n雪🚀'
    integer_type = 'INTEGER' if postgres else 'INTEGER(10) ZEROFILL'
    source_export.create(
        [
            ('id', 'INTEGER PRIMARY KEY'), ('fixed_text', 'CHAR(7)'),
            ('normalized_text', 'TEXT'), ('recorded_date', 'DATE'),
            ('numeric_gate', integer_type), ('numeric_secret', 'TEXT'),
        ],
        [
            (1, 'é🚀', raw_text, '10000-06-30' if postgres else '0000-00-00', 8, RAW_SECRET),
            (2, None, None, None, 80, RAW_SECRET),
        ],
    )
    source_export.configure([
        {'field_id': 'fixed_text', 'type': 'HASH'},
        {'field_id': 'normalized_text', 'type': 'HASH'},
        {'field_id': 'recorded_date', 'type': 'MASK-DATE'},
        {
            'field_id': 'numeric_secret', 'type': 'MASK-HIDDEN',
            'when': [{'column': 'numeric_gate', 'regex_match': '[801]'}],
        },
        {'field_id': 'numeric_secret', 'type': 'HASH'},
    ])

    contents, exported_rows = source_export.export(tmp_path / 'normalized.csv.gz')

    assert RAW_SECRET not in contents
    actual = {int(row[0]): row[1:6] for row in exported_rows}
    assert actual[1][:2] == [_sha256('é🚀' + (' ' * 5 if postgres else '')), _sha256(raw_text.replace('\x00', ''))]
    if postgres:
        assert datetime.fromisoformat(actual[1][2]) == datetime(9999, 1, 1)
    else:
        assert actual[1][2] == ''
    assert actual[1][3:] == ['8', _sha256('hidden')]
    assert actual[2] == ['', '', '', '80', _sha256(RAW_SECRET)]


def test_unsupported_transform_rejects_export_before_file_creation(source_export, tmp_path):
    """An unsupported transformation must not leave raw local staging data."""
    source_export.create(
        [('id', 'INTEGER PRIMARY KEY'), ('secret', 'TEXT')],
        [(1, RAW_SECRET)],
    )
    source_export.configure([{'field_id': 'secret', 'type': 'UNSUPPORTED'}])
    export_path = tmp_path / 'rejected.csv.gz'

    with pytest.raises(ValueError):
        source_export.export(export_path)

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('condition_type', ['equals', 'regex_match'])
def test_backslash_conditions_reject_export_before_file_creation(source_export, tmp_path, condition_type):
    """Reject dialect-dependent escaping before any private values are exported."""
    source_export.create(
        [('id', 'INTEGER PRIMARY KEY'), ('secret', 'TEXT')],
        [(1, RAW_SECRET)],
    )
    source_export.configure([{
        'field_id': 'secret', 'type': 'HASH',
        'when': [{'column': 'secret', condition_type: r'private\value'}],
    }])

    with pytest.raises(ValueError, match='[Bb]ackslash'):
        source_export.export(tmp_path / 'rejected.csv.gz')

    assert list(tmp_path.iterdir()) == []


def test_mask_date_preserves_early_years_and_fractional_time(source_export, tmp_path):
    """Masking must not reinterpret years 1–99 as a different century."""
    date_type = 'TIMESTAMP(6)' if source_export.engine == 'postgres' else 'DATETIME(6)'
    years = (1, 69, 70, 99, 100)
    source_export.create(
        [('id', 'INTEGER PRIMARY KEY'), ('event_date', date_type)],
        [(year, datetime(year, 8, 19, 23, 59, 58, 123456)) for year in years],
    )
    source_export.configure([{'field_id': 'event_date', 'type': 'MASK-DATE'}])

    _contents, exported_rows = source_export.export(tmp_path / 'early_dates.csv.gz')

    assert sorted(
        (int(row[0]), datetime.fromisoformat(row[1])) for row in exported_rows
    ) == [(year, datetime(year, 1, 1, 23, 59, 58, 123456)) for year in years]


def test_float_conditions_compare_the_legacy_csv_value(source_export, tmp_path):
    """A REAL/FLOAT rounding artifact must not bypass conditional masking."""
    float_type = 'REAL' if source_export.engine == 'postgres' else 'FLOAT'
    values = (0.3, -0.3, 0.1, None)
    source_export.create(
        [('id', 'INTEGER PRIMARY KEY'), ('floating_gate', float_type), ('secret', 'TEXT')],
        [(index, value, RAW_SECRET) for index, value in enumerate(values)],
    )
    source_export.configure([
        {
            'field_id': 'secret', 'type': 'MASK-HIDDEN',
            'when': [{'column': 'floating_gate', 'equals': value}],
        }
        for value in values
    ])

    contents, exported_rows = source_export.export(tmp_path / 'floating_conditions.csv.gz')

    assert RAW_SECRET not in contents
    assert sorted(row[:3] for row in exported_rows) == [
        [str(index), '' if value is None else str(value), 'hidden']
        for index, value in enumerate(values)
    ]


@pytest.mark.parametrize('pattern', ['a b', 'a#b'])
def test_regex_keeps_literal_spaces_and_hashes(source_export, tmp_path, pattern):
    """MariaDB's session regex defaults must not change masking conditions."""
    source_export.create(
        [('id', 'INTEGER PRIMARY KEY'), ('gate', 'TEXT'), ('secret', 'TEXT')],
        [(1, pattern, RAW_SECRET), (2, 'ab', 'public')],
    )
    source_export.configure([{
        'field_id': 'secret', 'type': 'MASK-HIDDEN',
        'when': [{'column': 'gate', 'regex_match': pattern}],
    }])
    previous_flags = []
    try:
        if source_export.engine == 'mariadb':
            for connection in (source_export.source.conn, source_export.source.conn_unbuffered):
                with connection.cursor() as cursor:
                    cursor.execute('SELECT @@SESSION.default_regex_flags AS regex_flags')
                    row = cursor.fetchone()
                    cursor.fetchall()
                    previous_flags.append((connection, row['regex_flags'] if isinstance(row, dict) else row[0]))
                    cursor.execute("SET SESSION default_regex_flags = 'EXTENDED'")

        contents, exported_rows = source_export.export(tmp_path / 'regex_literals.csv.gz')

        assert RAW_SECRET not in contents
        assert sorted(row[:3] for row in exported_rows) == [['1', pattern, 'hidden'], ['2', 'ab', 'public']]
    finally:
        for connection, flags in previous_flags:
            with connection.cursor() as cursor:
                cursor.execute('SET SESSION default_regex_flags = %s', (flags,))


def test_network_address_hashes_and_conditions_use_copy_text(source_export, tmp_path):
    """INET host addresses must not acquire /32 or /128 before transformation."""
    address_type = 'INET' if source_export.engine == 'postgres' else 'VARCHAR(64)'
    addresses = ('192.0.2.1', '2001:db8::1', '192.0.2.1/24', None)
    source_export.create(
        [
            ('id', 'INTEGER PRIMARY KEY'), ('address', address_type),
            ('hashed_address', address_type), ('secret', 'TEXT'),
        ],
        [(index, address, address, RAW_SECRET) for index, address in enumerate(addresses)],
    )
    source_export.configure([
        {'field_id': 'hashed_address', 'type': 'HASH'},
        *({
            'field_id': 'secret', 'type': 'MASK-HIDDEN',
            'when': [{'column': 'address', 'equals': address}],
        } for address in addresses),
    ])

    contents, exported_rows = source_export.export(tmp_path / 'network_addresses.csv.gz')

    assert RAW_SECRET not in contents
    assert sorted(row[:4] for row in exported_rows) == [
        [str(index), address or '', _sha256(address) if address is not None else '', 'hidden']
        for index, address in enumerate(addresses)
    ]
