import json
from dataclasses import replace
from decimal import Decimal

import pytest

from pipelinewise.fastsync.commons.snowflake_iceberg import RecoveryManifestError
from pipelinewise.fastsync.commons.snowflake_iceberg_converter import (
    EVENTUAL_ICEBERG,
    NativeColumn,
    NativeToIcebergConversionError,
    SnowflakeNativeToIcebergConverter,
    SnowflakeTableName,
    parse_native_columns,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_validation import (
    ICEBERG_VARCHAR_LENGTH,
    ConversionMetadata,
)


def _column(data_type, **overrides):
    row = {
        'COLUMN_NAME': 'VALUE',
        'DATA_TYPE': data_type,
        'IS_NULLABLE': 'YES',
        'NUMERIC_PRECISION': None,
        'NUMERIC_SCALE': None,
        'COMMENT': None,
    }
    row.update(overrides)
    return NativeColumn.from_row(row)


def test_preserves_quoted_table_names():
    """Quoted identifiers retain case, punctuation, and escaped quotes."""
    table = SnowflakeTableName.parse('"Mixed.DB".schema."Table ""Name"""')

    assert table == SnowflakeTableName('Mixed.DB', 'SCHEMA', 'Table "Name"')
    assert table.quoted == '"Mixed.DB"."SCHEMA"."Table ""Name"""'
    assert table.with_suffix('_ICEBERG').quoted == (
        '"Mixed.DB"."SCHEMA"."Table ""Name""_ICEBERG"'
    )


@pytest.mark.parametrize(
    'fqtn',
    [
        None,
        '',
        'TABLE',
        'SCHEMA.TABLE',
        'DATABASE.SCHEMA.TABLE.EXTRA',
        'DATABASE.SCHEMA.TABLE NAME',
        'DATABASE.SCHEMA.""',
        'DATABASE.SCHEMA."UNTERMINATED',
    ],
)
def test_table_name_rejects_invalid_values(fqtn):
    """Conversion cannot target an ambiguous or partial table name."""
    with pytest.raises(ValueError):
        SnowflakeTableName.parse(fqtn)


@pytest.mark.parametrize(
    ('data_type', 'expected'),
    [
        ('TEXT', f'VARCHAR({ICEBERG_VARCHAR_LENGTH})'),
        ('FLOAT', 'DOUBLE'),
        ('BOOLEAN', 'BOOLEAN'),
        ('BINARY', 'BINARY(67108864)'),
        ('DATE', 'DATE'),
        ('TIME', 'TIME(6)'),
        ('TIMESTAMP_NTZ', 'TIMESTAMP_NTZ(6)'),
        ('TIMESTAMP_LTZ', 'TIMESTAMP_LTZ(6)'),
        ('TIMESTAMP_TZ', 'TIMESTAMP_LTZ(6)'),
        ('VARIANT', 'VARIANT'),
    ],
)
def test_maps_native_types_to_v3(data_type, expected):
    """Every supported native type has one canonical Iceberg v3 type."""
    assert _column(data_type).iceberg_type == expected


def test_number_preserves_precision_scale():
    """Fixed-point columns retain their declared precision and scale."""
    column = _column('NUMBER', NUMERIC_PRECISION=38, NUMERIC_SCALE=10)

    assert column.iceberg_type == 'NUMBER(38,10)'


def test_owner_changes_metadata_fingerprint():
    """Recovery schema identity includes owner name and owner kind."""
    metadata = ConversionMetadata(
        columns=(_column('NUMBER', NUMERIC_PRECISION=19, NUMERIC_SCALE=0),),
        primary_key=('VALUE',),
        owner='OWNER_ROLE',
        owner_role_type='ROLE',
        table_comment=None,
        grants=(),
        tags=(),
    )

    assert replace(metadata, owner='OTHER_ROLE').fingerprint != metadata.fingerprint
    assert (
        replace(metadata, owner_role_type='DATABASE_ROLE').fingerprint
        != metadata.fingerprint
    )


@pytest.mark.parametrize(
    'column',
    [
        _column('NUMBER'),
        _column('GEOGRAPHY'),
    ],
)
def test_incomplete_or_unsupported_types_fail(column):
    """The converter does not infer lossy mappings for unknown metadata."""
    with pytest.raises(NativeToIcebergConversionError):
        _ = column.iceberg_type


def test_builds_escaped_column_sql():
    """DDL and CTAS projections quote names and preserve comments safely."""
    column = _column(
        'TEXT',
        COLUMN_NAME='Display "Name',
        IS_NULLABLE='NO',
        COMMENT="operator's label",
    )

    assert column.definition == (
        f'"Display ""Name" VARCHAR({ICEBERG_VARCHAR_LENGTH}) NOT NULL '
        "COMMENT 'operator''s label'"
    )
    assert column.projection == (
        f'CAST("Display ""Name" AS VARCHAR({ICEBERG_VARCHAR_LENGTH})) '
        'AS "Display ""Name"'
    )


def test_rejects_invalid_column_metadata():
    """Missing or contradictory Snowflake metadata stops conversion."""
    with pytest.raises(NativeToIcebergConversionError, match='no visible columns'):
        parse_native_columns(())

    row = {
        'column_name': 'ID',
        'data_type': 'NUMBER',
        'is_nullable': 'NO',
        'numeric_precision': 19,
        'numeric_scale': 0,
    }
    with pytest.raises(NativeToIcebergConversionError, match='duplicate'):
        parse_native_columns((row, row.copy()))


class FakeSnowflake:  # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """Stateful Snowflake adapter for conversion and interruption tests."""

    def __init__(self):
        self.connection_config = {
            'account': 'test-account',
            'dbname': 'DATABASE',
            'user': 'PIPELINEWISE_USER',
            'role': 'OWNER_ROLE',
            'private_key': 'private-key-secret',
        }
        self.formats = {'TABLE': 'native'}
        self.queries = []
        self.source_evidence = (2, 12345)
        self.staging_evidence = (2, 12345)
        self.policy_rows = []
        self.stream_rows = []
        self.column_tags = []
        self.other_constraints = []
        self.exported_keys = []
        self.grants = []
        self.companion_grants = None
        self.companion_tags = None
        self.column_default = None
        self.is_identity = 'NO'
        self.is_nullable = 'NO'
        self.column_comment = 'identifier'
        self.companion_column_comment = None
        self.companion_column_overrides = {}
        self.companion_primary_key = ('ID',)
        self.clustering_key = None
        self.owner = 'OWNER_ROLE'
        self.owner_role_type = 'ROLE'
        self.companion_owner = None
        self.companion_owner_role_type = None
        self.current_role = 'OWNER_ROLE'
        self.table_comment = 'source table'
        self.companion_table_comment = None
        self.null_keys = 0
        self.fail_promotion = None
        self.interrupt_after_native_rename = False
        self.commit_before_promotion_error = False
        self.post_promotion_staging_evidence = None
        self.interrupt_iceberg_rollback = False
        self.rollback_commits = False
        self.interrupt_native_restore = False

    # Stateful SQL dispatch keeps the recovery scenarios deterministic.
    # pylint: disable-next=too-many-return-statements,too-many-branches,too-many-statements
    def query(  # noqa: C901
        self,
        sql,
        params=None,
        query_tag_props=None,
    ):
        """Return deterministic metadata and mutate names for DDL."""
        self.queries.append((sql, params, query_tag_props))
        if sql.startswith('SHOW TABLES IN SCHEMA'):
            return [
                {
                    'name': name,
                    'is_iceberg': table_format == 'iceberg',
                    'owner': (
                        self.companion_owner
                        if table_format == 'iceberg'
                        and self.companion_owner is not None
                        else self.owner
                    ),
                    'owner_role_type': (
                        self.companion_owner_role_type
                        if table_format == 'iceberg'
                        and self.companion_owner_role_type is not None
                        else self.owner_role_type
                    ),
                }
                for name, table_format in self.formats.items()
            ]
        if 'INFORMATION_SCHEMA"."COLUMNS' in sql:
            table_name = params['table']
            is_companion = self.formats.get(table_name) == 'iceberg'
            row = {
                'COLUMN_NAME': 'ID',
                'DATA_TYPE': 'NUMBER',
                'IS_NULLABLE': 'NO' if is_companion else self.is_nullable,
                'NUMERIC_PRECISION': 19,
                'NUMERIC_SCALE': 0,
                'DATETIME_PRECISION': None,
                'COMMENT': (
                    self.companion_column_comment
                    if is_companion and self.companion_column_comment is not None
                    else self.column_comment
                ),
                'COLUMN_DEFAULT': self.column_default,
                'IS_IDENTITY': self.is_identity,
            }
            if is_companion:
                row.update(self.companion_column_overrides)
            return [row]
        if sql.startswith('SHOW PRIMARY KEYS'):
            is_companion = any(
                f'"{name}"' in sql and table_format == 'iceberg'
                for name, table_format in self.formats.items()
            )
            keys = self.companion_primary_key if is_companion else ('ID',)
            return [
                {'COLUMN_NAME': column, 'KEY_SEQUENCE': sequence}
                for sequence, column in enumerate(keys, start=1)
            ]
        if 'INFORMATION_SCHEMA"."TABLE_CONSTRAINTS' in sql:
            return self.other_constraints
        if sql.startswith('SHOW EXPORTED KEYS IN TABLE'):
            return self.exported_keys
        if 'NULL_KEY_COUNT' in sql:
            return [{'NULL_KEY_COUNT': self.null_keys}]
        if sql.startswith('SHOW STREAMS'):
            return self.stream_rows
        if 'POLICY_REFERENCES' in sql:
            return self.policy_rows
        if 'TAG_REFERENCES_ALL_COLUMNS' in sql:
            return self.column_tags
        if 'TAG_REFERENCES(' in sql:
            table_name = 'TABLE_ICEBERG' if 'TABLE_ICEBERG' in str(params) else 'TABLE'
            is_companion = self.formats.get(table_name) == 'iceberg'
            if is_companion and self.companion_tags is not None:
                return self.companion_tags
            return []
        if 'INFORMATION_SCHEMA"."TABLES' in sql:
            is_companion = self.formats.get(params['table']) == 'iceberg'
            return [{
                'COMMENT': (
                    self.companion_table_comment
                    if is_companion and self.companion_table_comment is not None
                    else self.table_comment
                ),
                'CLUSTERING_KEY': self.clustering_key,
                'TABLE_OWNER': (
                    self.companion_owner
                    if is_companion and self.companion_owner is not None
                    else self.owner
                ),
            }]
        if sql.startswith('SELECT CURRENT_ROLE()'):
            return [{'CURRENT_ROLE': self.current_role}]
        if sql.startswith('SHOW GRANTS'):
            is_companion = any(
                f'"{name}"' in sql and table_format == 'iceberg'
                for name, table_format in self.formats.items()
            )
            if is_companion and self.companion_grants is not None:
                return self.companion_grants
            return self.grants
        if sql.startswith('CREATE ICEBERG TABLE'):
            self.formats['TABLE_ICEBERG'] = 'iceberg'
            return []
        if sql.startswith('SHOW ICEBERG TABLES'):
            return [
                {'name': name, 'catalog_name': 'SNOWFLAKE'}
                for name, table_format in self.formats.items()
                if table_format == 'iceberg'
            ]
        if sql.startswith("SHOW PARAMETERS LIKE 'ICEBERG_VERSION'"):
            return [{'key': 'ICEBERG_VERSION', 'value': '3'}]
        if sql.startswith("SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR'"):
            default = {'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR', 'value': 'DISABLED', 'level': 'TABLE'}
            return getattr(self, 'merge_on_read_parameters', [default])
        if sql.startswith('SELECT COUNT(*) AS "ROW_COUNT"'):
            evidence = (
                self.staging_evidence
                if '"TABLE_ICEBERG"' in sql or (
                    self.formats.get('TABLE') == 'iceberg'
                    and sql.endswith('FROM "DATABASE"."SCHEMA"."TABLE")')
                )
                else self.source_evidence
            )
            return [{'ROW_COUNT': evidence[0], 'ROW_HASH': evidence[1]}]
        if sql.startswith('ALTER TABLE "DATABASE"."SCHEMA"."TABLE" RENAME'):
            self.formats['TABLE_NATIVE'] = self.formats.pop('TABLE')
            if self.interrupt_after_native_rename:
                raise SystemExit('simulated interruption after native rename')
            return []
        if sql.startswith(
            'ALTER ICEBERG TABLE "DATABASE"."SCHEMA"."TABLE_ICEBERG" RENAME'
        ):
            if self.fail_promotion and self.commit_before_promotion_error:
                self.formats['TABLE'] = self.formats.pop('TABLE_ICEBERG')
                raise self.fail_promotion
            if self.fail_promotion:
                raise self.fail_promotion
            self.formats['TABLE'] = self.formats.pop('TABLE_ICEBERG')
            if self.post_promotion_staging_evidence is not None:
                self.staging_evidence = self.post_promotion_staging_evidence
            return []
        if sql.startswith(
            'ALTER ICEBERG TABLE "DATABASE"."SCHEMA"."TABLE" RENAME'
        ):
            if self.interrupt_iceberg_rollback:
                if self.rollback_commits:
                    self.formats['TABLE_ICEBERG'] = self.formats.pop('TABLE')
                raise SystemExit('simulated first rollback interruption')
            self.formats['TABLE_ICEBERG'] = self.formats.pop('TABLE')
            return []
        if sql.startswith('ALTER TABLE "DATABASE"."SCHEMA"."TABLE_NATIVE" RENAME'):
            if self.interrupt_native_restore:
                raise SystemExit('simulated rollback interruption')
            self.formats['TABLE'] = self.formats.pop('TABLE_NATIVE')
            return []
        if sql.startswith('GRANT ') or ' SET TAG ' in sql:
            return []
        raise AssertionError(f'Unexpected Snowflake query: {sql}')


def _converter(tmp_path, snowflake=None):
    return SnowflakeNativeToIcebergConverter(
        snowflake or FakeSnowflake(),
        str(tmp_path),
    )


def _manifest_files(tmp_path):
    return list(tmp_path.glob('iceberg-recovery-*.json'))


@pytest.mark.parametrize(
    'iceberg_version',
    (True, '3', 3.0, Decimal('3'), 2, 4),
)
def test_rejects_unsupported_versions_early(
    tmp_path,
    iceberg_version,
):
    """Past and future versions fail before Snowflake or recovery state changes."""
    snowflake = FakeSnowflake()

    with pytest.raises(ValueError, match='Only managed Iceberg version 3'):
        _converter(tmp_path, snowflake).convert(
            'database.schema.table',
            iceberg_version=iceberg_version,
        )

    assert snowflake.queries == []
    assert not _manifest_files(tmp_path)


@pytest.mark.parametrize('iceberg_version', (2, 4, 3.0))
def test_tampered_manifest_version_fails(
    tmp_path,
    iceberg_version,
):
    """Persisted unsupported or non-integer versions fail before Snowflake."""
    snowflake = FakeSnowflake()
    snowflake.staging_evidence = (2, 54321)
    with pytest.raises(NativeToIcebergConversionError, match='does not match'):
        _converter(tmp_path, snowflake).convert('database.schema.table')

    manifest_path = _manifest_files(tmp_path)[0]
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    manifest['iceberg_version'] = iceberg_version
    manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
    query_count = len(snowflake.queries)

    with pytest.raises(
        RecoveryManifestError,
        match='table format contract is unsupported',
    ):
        _converter(tmp_path, snowflake).convert('database.schema.table')

    assert len(snowflake.queries) == query_count


def test_native_mode_keeps_companion(tmp_path):
    """The default mode leaves the source name and a v3 Iceberg copy."""
    snowflake = FakeSnowflake()

    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        iceberg_version=3,
    )

    assert snowflake.formats == {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}
    assert not _manifest_files(tmp_path)
    create_sql = next(sql for sql, _, _ in snowflake.queries if sql.startswith('CREATE'))
    assert "CATALOG = 'SNOWFLAKE' ICEBERG_VERSION = 3" in create_sql
    assert "TARGET_FILE_SIZE = '16MB'" in create_sql
    assert 'OR REPLACE' not in create_sql
    assert create_sql.startswith(
        'CREATE ICEBERG TABLE "DATABASE"."SCHEMA"."TABLE_ICEBERG"'
    )
    assert create_sql.endswith('FROM "DATABASE"."SCHEMA"."TABLE"')
    assert 'PRIMARY KEY ("ID")' in create_sql
    assert 'AS SELECT CAST("ID" AS NUMBER(19,0)) AS "ID"' in create_sql


def test_promotes_existing_companion(tmp_path):
    """A later explicit cutover reuses and promotes the validated companion."""
    snowflake = FakeSnowflake()

    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        iceberg_version=3,
    )
    create_query = next(
        query for query in snowflake.queries if query[0].startswith('CREATE')
    )

    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        eventual=EVENTUAL_ICEBERG,
        iceberg_version=3,
    )

    assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}
    assert not _manifest_files(tmp_path)
    assert len([
        sql for sql, _, _ in snowflake.queries if sql.startswith('CREATE')
    ]) == 1
    rename_queries = [
        query for query in snowflake.queries if ' RENAME TO ' in query[0]
    ]
    assert [query[0].split()[1] for query in rename_queries] == [
        'TABLE',
        'ICEBERG',
    ]
    assert create_query[2]['load_id'] != rename_queries[0][2]['load_id']


def test_rejects_changed_existing_companion(tmp_path):
    """A later cutover cannot promote a companion whose contents have drifted."""
    snowflake = FakeSnowflake()
    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        iceberg_version=3,
    )
    snowflake.staging_evidence = (2, 54321)

    with pytest.raises(NativeToIcebergConversionError, match='does not match'):
        _converter(tmp_path, snowflake).convert(
            'database.schema.table',
            eventual=EVENTUAL_ICEBERG,
            iceberg_version=3,
        )

    assert snowflake.formats == {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}
    assert not _manifest_files(tmp_path)


@pytest.mark.parametrize(
    ('column_overrides', 'primary_key'),
    [
        ({'DATA_TYPE': 'FLOAT'}, ('ID',)),
        ({'IS_NULLABLE': 'YES'}, ('ID',)),
        ({}, ()),
    ],
)
def test_rejects_bad_companion_schema(
    tmp_path,
    column_overrides,
    primary_key,
):
    """A later cutover cannot adopt a same-content companion with incompatible DDL."""
    snowflake = FakeSnowflake()
    _converter(tmp_path, snowflake).convert('database.schema.table')
    snowflake.companion_column_overrides = column_overrides
    snowflake.companion_primary_key = primary_key

    with pytest.raises(NativeToIcebergConversionError, match='schema or primary key'):
        _converter(tmp_path, snowflake).convert(
            'database.schema.table',
            eventual=EVENTUAL_ICEBERG,
        )

    assert snowflake.formats == {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}
    assert not _manifest_files(tmp_path)
    assert not any(' RENAME TO ' in sql for sql, _, _ in snowflake.queries)


@pytest.mark.parametrize('metadata_kind', ['comment', 'grant', 'tag'])
def test_rejects_stale_companion_metadata(tmp_path, metadata_kind):
    """Removed governance metadata cannot remain on a later promoted companion."""
    snowflake = FakeSnowflake()
    _converter(tmp_path, snowflake).convert('database.schema.table')
    if metadata_kind == 'comment':
        snowflake.column_comment = None
        snowflake.companion_column_comment = 'identifier'
    elif metadata_kind == 'grant':
        snowflake.companion_grants = [{
            'privilege': 'SELECT',
            'granted_to': 'ROLE',
            'grantee_name': 'REVOKED_ROLE',
            'grant_option': 'false',
        }]
    else:
        snowflake.companion_tags = [{
            'TAG_DATABASE': 'DATABASE',
            'TAG_SCHEMA': 'GOVERNANCE',
            'TAG_NAME': 'CLASSIFICATION',
            'TAG_VALUE': 'REVOKED',
            'LEVEL': 'TABLE',
            'APPLY_METHOD': 'MANUAL',
        }]

    with pytest.raises(NativeToIcebergConversionError, match='comments, grants, or tags'):
        _converter(tmp_path, snowflake).convert(
            'database.schema.table',
            eventual=EVENTUAL_ICEBERG,
        )

    assert snowflake.formats == {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}
    assert len(_manifest_files(tmp_path)) == 1
    assert not any(' RENAME TO ' in sql for sql, _, _ in snowflake.queries)


def test_companion_promotion_recovers(tmp_path):
    """A failed later promotion retains a fresh manifest and retries safely."""
    snowflake = FakeSnowflake()
    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        iceberg_version=3,
    )
    snowflake.fail_promotion = RuntimeError('promotion rejected')

    with pytest.raises(RuntimeError, match='promotion rejected'):
        _converter(tmp_path, snowflake).convert(
            'database.schema.table',
            eventual=EVENTUAL_ICEBERG,
            iceberg_version=3,
        )

    assert snowflake.formats == {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}
    assert len(_manifest_files(tmp_path)) == 1

    snowflake.fail_promotion = None
    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        eventual=EVENTUAL_ICEBERG,
        iceberg_version=3,
    )
    assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}
    assert not _manifest_files(tmp_path)


def test_primary_key_is_not_null_in_iceberg(tmp_path):
    """A data-valid native key becomes an Iceberg identifier field."""
    snowflake = FakeSnowflake()
    snowflake.is_nullable = 'YES'

    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        iceberg_version=3,
    )

    create_sql = next(sql for sql, _, _ in snowflake.queries if sql.startswith('CREATE'))
    assert '"ID" NUMBER(19,0) NOT NULL' in create_sql


def test_iceberg_mode_retains_native_backup(tmp_path):
    """Cutover promotes Iceberg and leaves the native rollback table."""
    snowflake = FakeSnowflake()

    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        eventual=EVENTUAL_ICEBERG,
        iceberg_version=3,
    )

    assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}
    assert not _manifest_files(tmp_path)


def test_preserves_explicit_grants(tmp_path):
    """Explicit non-ownership grants are replayed on the Iceberg object."""
    snowflake = FakeSnowflake()
    snowflake.grants = [
        {
            'privilege': 'SELECT',
            'granted_to': 'ROLE',
            'grantee_name': 'Mixed Role',
            'grant_option': 'false',
        },
        {
            'privilege': 'INSERT',
            'granted_to': 'DATABASE_ROLE',
            'grantee_name': 'DATABASE.INGESTER',
            'grant_option': 'true',
        },
    ]

    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        iceberg_version=3,
    )

    grant_sql = [sql for sql, _, _ in snowflake.queries if sql.startswith('GRANT ')]
    assert grant_sql == [
        'GRANT SELECT ON TABLE "DATABASE"."SCHEMA"."TABLE_ICEBERG" '
        'TO ROLE "Mixed Role"',
        'GRANT INSERT ON TABLE "DATABASE"."SCHEMA"."TABLE_ICEBERG" '
        'TO DATABASE ROLE "DATABASE"."INGESTER" WITH GRANT OPTION',
    ]


def test_promotion_failure_restores_native(tmp_path):
    """A rejected promotion restores the live name and retains recovery state."""
    snowflake = FakeSnowflake()
    snowflake.fail_promotion = RuntimeError('promotion rejected')

    with pytest.raises(RuntimeError, match='promotion rejected'):
        _converter(tmp_path, snowflake).convert(
            'database.schema.table',
            eventual=EVENTUAL_ICEBERG,
            iceberg_version=3,
        )

    assert snowflake.formats == {'TABLE_ICEBERG': 'iceberg', 'TABLE': 'native'}
    assert len(_manifest_files(tmp_path)) == 1

    snowflake.fail_promotion = None
    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        eventual=EVENTUAL_ICEBERG,
        iceberg_version=3,
    )
    assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}


def test_lost_promotion_response_reconciles(tmp_path):
    """A committed rename is finalized instead of rolling data back."""
    snowflake = FakeSnowflake()
    snowflake.fail_promotion = RuntimeError('response lost')
    snowflake.commit_before_promotion_error = True

    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        eventual=EVENTUAL_ICEBERG,
        iceberg_version=3,
    )

    assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}
    assert not _manifest_files(tmp_path)


def test_final_failure_restores_native(tmp_path):
    """A failed final validation restores the native live name for retry."""
    snowflake = FakeSnowflake()
    snowflake.post_promotion_staging_evidence = (2, 54321)

    with pytest.raises(NativeToIcebergConversionError, match='does not match'):
        _converter(tmp_path, snowflake).convert(
            'database.schema.table',
            eventual=EVENTUAL_ICEBERG,
            iceberg_version=3,
        )

    assert snowflake.formats == {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}
    assert len(_manifest_files(tmp_path)) == 1

    snowflake.post_promotion_staging_evidence = None
    snowflake.staging_evidence = snowflake.source_evidence
    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        eventual=EVENTUAL_ICEBERG,
        iceberg_version=3,
    )

    assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}
    assert not _manifest_files(tmp_path)


def test_restart_finishes_required_rollback(tmp_path):
    """Recovery completes a rollback interrupted between its two renames."""
    snowflake = FakeSnowflake()
    snowflake.post_promotion_staging_evidence = (2, 54321)
    snowflake.interrupt_native_restore = True

    with pytest.raises(SystemExit, match='rollback interruption'):
        _converter(tmp_path, snowflake).convert(
            'database.schema.table',
            eventual=EVENTUAL_ICEBERG,
        )

    assert snowflake.formats == {
        'TABLE_ICEBERG': 'iceberg',
        'TABLE_NATIVE': 'native',
    }
    manifest_path = _manifest_files(tmp_path)[0]
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    assert manifest['context']['rollback_required'] is True

    snowflake.interrupt_native_restore = False
    snowflake.post_promotion_staging_evidence = None
    snowflake.staging_evidence = snowflake.source_evidence
    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        eventual=EVENTUAL_ICEBERG,
    )

    assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}
    assert not _manifest_files(tmp_path)


@pytest.mark.parametrize('committed_before_interrupt', [False, True])
def test_first_rename_rollback_recovery(
    tmp_path,
    committed_before_interrupt,
):
    """Recovery uses the durable marker after either first-rename outcome."""
    snowflake = FakeSnowflake()
    snowflake.post_promotion_staging_evidence = (2, 54321)
    snowflake.interrupt_iceberg_rollback = True
    snowflake.rollback_commits = committed_before_interrupt

    with pytest.raises(SystemExit, match='first rollback interruption'):
        _converter(tmp_path, snowflake).convert(
            'database.schema.table',
            eventual=EVENTUAL_ICEBERG,
        )

    expected_formats = (
        {'TABLE_ICEBERG': 'iceberg', 'TABLE_NATIVE': 'native'}
        if committed_before_interrupt
        else {'TABLE': 'iceberg', 'TABLE_NATIVE': 'native'}
    )
    assert snowflake.formats == expected_formats
    manifest_path = _manifest_files(tmp_path)[0]
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    assert manifest['context']['rollback_required'] is True

    snowflake.interrupt_iceberg_rollback = False
    snowflake.post_promotion_staging_evidence = None
    snowflake.staging_evidence = snowflake.source_evidence
    _converter(tmp_path, snowflake).convert(
        'database.schema.table',
        eventual=EVENTUAL_ICEBERG,
    )

    assert snowflake.formats == {'TABLE_NATIVE': 'native', 'TABLE': 'iceberg'}
    assert not _manifest_files(tmp_path)


def test_mismatch_stops_before_cutover(tmp_path):
    """Row count and hash evidence must match before the native rename."""
    snowflake = FakeSnowflake()
    snowflake.staging_evidence = (2, 54321)

    with pytest.raises(NativeToIcebergConversionError, match='does not match.*actual_row_fingerprint=54321'):
        _converter(tmp_path, snowflake).convert(
            'database.schema.table',
            eventual=EVENTUAL_ICEBERG,
            iceberg_version=3,
        )

    assert snowflake.formats == {'TABLE': 'native', 'TABLE_ICEBERG': 'iceberg'}


@pytest.mark.parametrize(
    ('attribute', 'value', 'message'),
    [
        (
            'policy_rows',
            [{'POLICY_KIND': 'MASKING_POLICY', 'POLICY_NAME': 'MASK'}],
            'policies',
        ),
        (
            'column_tags',
            [{'LEVEL': 'COLUMN', 'APPLY_METHOD': 'MANUAL'}],
            'direct column tags',
        ),
        (
            'other_constraints',
            [{'CONSTRAINT_TYPE': 'UNIQUE', 'CONSTRAINT_NAME': 'UNIQUE_VALUE'}],
            'non-primary-key constraints',
        ),
        (
            'other_constraints',
            [{
                'DEPENDENCY_KIND': 'INBOUND_FOREIGN_KEY',
                'CONSTRAINT_TYPE': 'FOREIGN KEY',
                'CONSTRAINT_NAME': 'CHILD_PARENT_FK',
            }],
            'inbound foreign keys',
        ),
        (
            'exported_keys',
            [{
                'fk_database_name': 'OTHER_DATABASE',
                'fk_schema_name': 'PUBLIC',
                'fk_table_name': 'CHILD',
            }],
            'inbound foreign keys',
        ),
        ('column_default', 'CURRENT_TIMESTAMP()', 'has a default'),
        ('is_identity', 'YES', 'identity column'),
        ('clustering_key', 'LINEAR(ID)', 'clustering key'),
        ('null_keys', 1, 'NULL primary-key values'),
    ],
)
def test_preflight_rejects_unsafe_tables(tmp_path, attribute, value, message):
    """Metadata that cannot be preserved fails before Iceberg creation."""
    snowflake = FakeSnowflake()
    setattr(snowflake, attribute, value)

    with pytest.raises(NativeToIcebergConversionError, match=message):
        _converter(tmp_path, snowflake).convert(
            'database.schema.table',
            iceberg_version=3,
        )

    assert snowflake.formats == {'TABLE': 'native'}


def test_preflight_requires_owner_role(tmp_path):
    """Conversion cannot silently transfer ownership to the active role."""
    snowflake = FakeSnowflake()
    snowflake.owner = 'OTHER_OWNER'

    with pytest.raises(NativeToIcebergConversionError, match='owning role OTHER_OWNER'):
        _converter(tmp_path, snowflake).convert('database.schema.table')

    assert snowflake.formats == {'TABLE': 'native'}
    assert not _manifest_files(tmp_path)
    assert not any(sql.startswith('CREATE') for sql, _, _ in snowflake.queries)


def test_rejects_database_role_owner(tmp_path):
    """A same-name database role is not the active account role."""
    snowflake = FakeSnowflake()
    snowflake.owner_role_type = 'DATABASE_ROLE'

    with pytest.raises(
        NativeToIcebergConversionError,
        match='account-role ownership',
    ):
        _converter(tmp_path, snowflake).convert('database.schema.table')

    assert snowflake.formats == {'TABLE': 'native'}
    assert not _manifest_files(tmp_path)
    assert not any(sql.startswith('CREATE') for sql, _, _ in snowflake.queries)


@pytest.mark.parametrize(
    ('field', 'value'),
    [
        ('account', 'other-account'),
        ('dbname', '"DATABASE"'),
        ('user', 'OTHER_USER'),
        ('role', 'OTHER_ROLE'),
    ],
)
def test_target_identity_drift_before_query(
    tmp_path,
    field,
    value,
):
    """A retry cannot adopt recovery state created by another target principal."""
    snowflake = FakeSnowflake()
    snowflake.staging_evidence = (2, 54321)
    with pytest.raises(NativeToIcebergConversionError, match='does not match'):
        _converter(tmp_path, snowflake).convert('database.schema.table')

    manifest_path = _manifest_files(tmp_path)[0]
    persisted = manifest_path.read_bytes()
    query_count = len(snowflake.queries)
    snowflake.connection_config[field] = value

    with pytest.raises(
        NativeToIcebergConversionError,
        match='different Snowflake target identity',
    ):
        _converter(tmp_path, snowflake).convert('database.schema.table')

    assert len(snowflake.queries) == query_count
    assert manifest_path.read_bytes() == persisted


def test_manifest_excludes_target_credentials(tmp_path):
    """Manual recovery stores only a hash of the Snowflake principal."""
    snowflake = FakeSnowflake()
    snowflake.staging_evidence = (2, 54321)

    with pytest.raises(NativeToIcebergConversionError, match='does not match'):
        _converter(tmp_path, snowflake).convert('database.schema.table')

    manifest = _manifest_files(tmp_path)[0].read_text(encoding='utf-8')
    for excluded in (
        'test-account',
        'PIPELINEWISE_USER',
        'OWNER_ROLE',
        'private-key-secret',
    ):
        assert excluded not in manifest


def test_rejects_other_database(tmp_path):
    """A target config cannot convert a table in another database."""
    snowflake = FakeSnowflake()

    with pytest.raises(NativeToIcebergConversionError, match='outside'):
        _converter(tmp_path, snowflake).convert(
            'other_database.schema.table',
            iceberg_version=3,
        )

    assert snowflake.queries == []


def test_rejects_overlong_companion_name(tmp_path):
    """Reserved rollback and staging names must fit Snowflake's identifier limit."""
    snowflake = FakeSnowflake()
    table_name = 'T' * 248

    with pytest.raises(NativeToIcebergConversionError, match='too long'):
        _converter(tmp_path, snowflake).convert(
            f'database.schema.{table_name}',
            iceberg_version=3,
        )

    assert snowflake.queries == []


def test_rejects_corrupt_recovery_manifest(tmp_path):
    """Truncated recovery state fails closed before a retry mutates Snowflake."""
    snowflake = FakeSnowflake()
    snowflake.staging_evidence = (2, 54321)

    with pytest.raises(NativeToIcebergConversionError, match='does not match'):
        _converter(tmp_path, snowflake).convert(
            'database.schema.table',
            iceberg_version=3,
        )

    manifest_path = _manifest_files(tmp_path)[0]
    manifest_path.write_text('{', encoding='utf-8')
    query_count = len(snowflake.queries)

    with pytest.raises(RecoveryManifestError, match='Cannot read Iceberg recovery manifest'):
        _converter(tmp_path, snowflake).convert(
            'database.schema.table',
            iceberg_version=3,
        )

    assert len(snowflake.queries) == query_count
