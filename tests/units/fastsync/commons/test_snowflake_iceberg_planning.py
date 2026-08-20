"""Tests for Snowflake Iceberg discovery and publication planning."""

from unittest.mock import MagicMock

import pytest
import snowflake.connector as snowflake_connector

from pipelinewise.fastsync.commons import snowflake_iceberg_versions as versions
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    IcebergColumn,
    IcebergTableSpec,
    MANAGED_ICEBERG_V3_SPEC,
    MANAGED_ICEBERG_V3_TABLE_OPTIONS,
    PHASE_PUBLISHED,
    PUBLICATION_ADDITIVE_OVERWRITE,
    PUBLICATION_INSERT_OVERWRITE,
    PUBLICATION_MISSING_CTAS,
    PUBLICATION_PARTIAL_MERGE,
    PUBLICATION_PARTIAL_REPLACEMENT_CTAS,
    PUBLICATION_REPLACEMENT_CTAS,
    RecoveryManifestError,
    SnowflakeIcebergPublisher,
    SnowflakeTableMetadata,
    SnowflakeTableSnapshot,
    SUPPORTED_MANAGED_ICEBERG_TABLE_FORMATS,
    TABLE_FORMAT_MANAGED_ICEBERG_V3,
    TABLE_FORMAT_MISSING,
    TABLE_FORMAT_NATIVE,
    TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG,
    TableCompatibilityError,
    TableFormatDiscoveryError,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_versions import (
    build_recovery_identity,
)
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    RECOVERY_IDENTITY,
    FakeSnowflake,
    future_version_spec,
    make_attempt,
    missing_snapshot,
    replace_nullable,
    v3_snapshot,
)


class TestDiscoveryPlanning:
    """Validate table discovery and publication plans."""

    def test_discovery_uses_exact_names_with_wildcard_like_identifiers(self, tmp_path):
        """Discovery uses exact names with wildcard like identifiers."""
        snowflake = FakeSnowflake(
            responses=[
                [
                    {"name": "ORDER_1", "is_iceberg": "N"},
                    {"name": "ORDER_%", "is_iceberg": "Y"},
                ],
                [
                    {"name": "ORDER_1", "catalog_name": "OTHER"},
                    {"name": "ORDER_%", "catalog_name": "SNOWFLAKE"},
                ],
                [{"key": "ICEBERG_VERSION", "value": "3"}],
                [{
                    "key": "ICEBERG_MERGE_ON_READ_BEHAVIOR",
                    "value": "DISABLED",
                    "level": "TABLE",
                }],
            ]
        )
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))

        assert publisher.discover_table_format("weird_schema", "order_%") == TABLE_FORMAT_MANAGED_ICEBERG_V3
        assert snowflake.queries[0][0].endswith("STARTS WITH 'ORDER_%'")

    @pytest.mark.parametrize(
        ('schema_name', 'quoted_schema'),
        (
            ('my.schema', '"MY.SCHEMA"'),
            ('my"schema', '"MY""SCHEMA"'),
        ),
    )
    def test_discovery_quotes_schema_as_one_identifier(
        self,
        tmp_path,
        schema_name,
        quoted_schema,
    ):
        """Iceberg discovery preserves dots and quotes within schema names."""
        snowflake = FakeSnowflake([
            [{'name': 'TABLE', 'is_iceberg': True}],
            [{'name': 'TABLE', 'catalog_name': 'SNOWFLAKE'}],
            [{'key': 'ICEBERG_VERSION', 'value': '3'}],
            [{
                'key': 'ICEBERG_MERGE_ON_READ_BEHAVIOR',
                'value': 'DISABLED',
                'level': 'TABLE',
            }],
        ])
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))

        assert (
            publisher.discover_table_format(schema_name, 'table')
            == TABLE_FORMAT_MANAGED_ICEBERG_V3
        )
        assert snowflake.queries[1][0] == (
            f'SHOW ICEBERG TABLES IN SCHEMA "TEST_DB".{quoted_schema} '
            "STARTS WITH 'TABLE'"
        )

    def test_inspection_returns_missing_when_schema_is_genuinely_absent(self, tmp_path, spec):
        """Inspection returns missing when schema is genuinely absent."""
        missing_schema = snowflake_connector.errors.ProgrammingError(msg="Schema does not exist or not authorized")
        snowflake_adapter = FakeSnowflake(
            [
                missing_schema,
                [{"name": f"{spec.name.schema}_DECOY"}],
            ]
        )
        publisher = SnowflakeIcebergPublisher(snowflake_adapter, str(tmp_path))

        assert publisher.inspect_table(spec.name) == missing_snapshot()
        assert snowflake_adapter.queries[1][0] == ("SHOW SCHEMAS IN DATABASE \"TEST_DB\" STARTS WITH 'TEST_SCHEMA'")

    def test_inspection_preserves_table_discovery_error_when_schema_exists(self, tmp_path, spec):
        """Inspection preserves table discovery error when schema exists."""
        discovery_error = snowflake_connector.errors.ProgrammingError(msg="Table discovery is not authorized")
        publisher = SnowflakeIcebergPublisher(
            FakeSnowflake([discovery_error, [{"name": spec.name.schema}]]),
            str(tmp_path),
        )

        with pytest.raises(
            snowflake_connector.errors.ProgrammingError,
            match="not authorized",
        ):
            publisher.inspect_table(spec.name)

    @staticmethod
    def _managed_v3_inspection_responses(column_rows):
        """Return exact metadata responses for one existing managed-v3 table."""
        return [
            [{"name": "ORDERS", "is_iceberg": True, "id": "target-id"}],
            [{"name": "ORDERS", "is_iceberg": True}],
            [{"name": "ORDERS", "catalog_name": "SNOWFLAKE"}],
            [{"key": "ICEBERG_VERSION", "value": "3"}],
            [{
                "key": "ICEBERG_MERGE_ON_READ_BEHAVIOR",
                "value": "DISABLED",
                "level": "TABLE",
            }],
            column_rows,
            [],
        ]

    def test_inspection_reads_and_accepts_canonical_varchar_width(self, tmp_path, spec):
        """Existing managed-v3 strings expose their physical width in the snapshot."""
        snowflake = FakeSnowflake(self._managed_v3_inspection_responses([{
            "COLUMN_NAME": "BODY",
            "DATA_TYPE": "TEXT",
            "CHARACTER_MAXIMUM_LENGTH": 134217728,
            "IS_NULLABLE": "YES",
        }]))
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))

        snapshot = publisher.inspect_table(spec.name)

        assert snapshot.spec.columns == (IcebergColumn("BODY", "VARCHAR"),)
        assert 'CHARACTER_MAXIMUM_LENGTH' in snowflake.queries[-2][0]

    def test_inspection_rejects_narrow_varchar_before_mutation(self, tmp_path, spec):
        """An existing narrow string cannot be adopted for managed-v3 writes."""
        snowflake = FakeSnowflake(self._managed_v3_inspection_responses([{
            "COLUMN_NAME": "BODY",
            "DATA_TYPE": "TEXT",
            "CHARACTER_MAXIMUM_LENGTH": 16777216,
            "IS_NULLABLE": "YES",
        }]))
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))

        with pytest.raises(TableCompatibilityError) as error:
            publisher.inspect_table(spec.name)

        assert 'ALTER ICEBERG TABLE' in str(error.value)
        assert 'recreate the table' in str(error.value)
        assert all(
            query.lstrip().startswith(('SELECT', 'SHOW'))
            for query, _, _ in snowflake.queries
        )

    @pytest.mark.parametrize(
        ("table_rows", "extra_rows", "expected"),
        (
            ([], (), TABLE_FORMAT_MISSING),
            ([{"name": "TABLE", "is_iceberg": False}], (), TABLE_FORMAT_NATIVE),
            (
                [{"name": "TABLE", "is_iceberg": True}],
                ([{"name": "TABLE", "catalog_name": "GLUE"}],),
                TABLE_FORMAT_UNSUPPORTED_EXTERNAL_ICEBERG,
            ),
        ),
    )
    def test_discovery_classifies_physical_formats(self, tmp_path, table_rows, extra_rows, expected):
        """Discovery classifies physical formats."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake([table_rows, *extra_rows]), str(tmp_path))
        assert publisher.discover_table_format("SCHEMA", "TABLE") == expected

    def test_supported_managed_versions_are_explicit(self):
        """Only explicitly enabled managed Iceberg versions are supported."""
        assert SUPPORTED_MANAGED_ICEBERG_TABLE_FORMATS == {
            3: TABLE_FORMAT_MANAGED_ICEBERG_V3,
        }

    @pytest.mark.parametrize("version", (2, 4))
    def test_discovery_rejects_unsupported_managed_versions(self, tmp_path, version):
        """Discovery rejects past and future managed Iceberg versions."""
        publisher = SnowflakeIcebergPublisher(
            FakeSnowflake([
                [{"name": "TABLE", "is_iceberg": True}],
                [{"name": "TABLE", "catalog_name": "SNOWFLAKE"}],
                [{"key": "ICEBERG_VERSION", "value": version}],
            ]),
            str(tmp_path),
        )

        with pytest.raises(
            TableFormatDiscoveryError,
            match=rf"unsupported ICEBERG_VERSION {version}",
        ):
            publisher.discover_table_format("SCHEMA", "TABLE")

    def test_discovery_rejects_ambiguous_metadata(self, tmp_path):
        """Discovery rejects ambiguous metadata."""
        snowflake = FakeSnowflake([[{"name": "TABLE", "is_iceberg": True}]])
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))

        with pytest.raises(TableFormatDiscoveryError, match="metadata is incomplete"):
            publisher.discover_table_format("SCHEMA", "TABLE")

    def test_missing_full_sync_plans_explicit_schema_ctas(self, tmp_path, spec):
        """Missing full sync plans explicit schema ctas."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())
        attempt = make_attempt(spec)

        plan = publisher.plan_full_sync(attempt, spec)

        assert plan.method == PUBLICATION_MISSING_CTAS
        assert plan.preparation_statements == ()
        assert plan.publication_statements == (
            'CREATE ICEBERG TABLE "TEST_DB"."TEST_SCHEMA"."ORDERS" '
            '("ID" NUMBER(38,0) NOT NULL, "PAYLOAD" VARIANT, "UPDATED AT" TIMESTAMP_NTZ(6), '
            '"_SDC_EXTRACTED_AT" TIMESTAMP_NTZ(6), "_SDC_BATCHED_AT" TIMESTAMP_NTZ(6), '
            '"_SDC_DELETED_AT" VARCHAR(134217728), PRIMARY KEY ("ID")) '
            "CATALOG = 'SNOWFLAKE' ICEBERG_VERSION = 3 "
            f"{MANAGED_ICEBERG_V3_TABLE_OPTIONS} AS SELECT "
            'CAST("ID" AS NUMBER(38,0)) AS "ID", CAST("PAYLOAD" AS VARIANT) AS "PAYLOAD", '
            'CAST("UPDATED AT" AS TIMESTAMP_NTZ(6)) AS "UPDATED AT", '
            'CAST("_SDC_EXTRACTED_AT" AS TIMESTAMP_NTZ(6)) AS "_SDC_EXTRACTED_AT", '
            'CAST("_SDC_BATCHED_AT" AS TIMESTAMP_NTZ(6)) AS "_SDC_BATCHED_AT", '
            'CAST("_SDC_DELETED_AT" AS VARCHAR(134217728)) AS "_SDC_DELETED_AT" '
            f"FROM {spec.name.with_table(attempt.staging_table).quoted}",
        )

    def test_ctas_uses_version_persisted_in_attempt(
        self,
        tmp_path,
        spec,
        monkeypatch,
    ):
        """A future version dispatches through durable state, not a SQL literal."""
        future_spec = future_version_spec()
        monkeypatch.setattr(
            versions,
            'MANAGED_ICEBERG_VERSION_SPECS',
            versions.managed_iceberg_version_registry(
                MANAGED_ICEBERG_V3_SPEC,
                future_spec,
            ),
        )
        recovery_identity = build_recovery_identity(
            'fastsync',
            {'route': 'unit-test'},
            transformation_config={},
            stream_identity={
                'tap_id': 'unit-test',
                'route': 'unit-test',
                'table': 'source.table',
            },
            target_table_format='iceberg',
            iceberg_version=4,
        )
        publisher = SnowflakeIcebergPublisher(
            FakeSnowflake(), str(tmp_path)
        )
        publisher.inspect_table = MagicMock(return_value=missing_snapshot())
        future_table_spec = IcebergTableSpec.from_fastsync(
            spec.name.database,
            spec.name.schema,
            spec.name.table,
            ['"ID" NUMBER', '"PAYLOAD" VARIANT', '"UPDATED AT" TIMESTAMP_NTZ'],
            ['"ID"'],
            iceberg_version=4,
        )
        attempt = make_attempt(future_table_spec)
        attempt.iceberg_version = 4
        attempt.recovery_identity = recovery_identity

        sql = publisher.plan_full_sync(
            attempt, future_table_spec
        ).publication_statements[0]

        assert 'ICEBERG_VERSION = 4' in sql
        assert 'FUTURE_VERSION_OPTIONS = TRUE' in sql
        assert '"ID" NUMBER(20,0) NOT NULL' in sql

    def test_exact_full_sync_plans_insert_overwrite(self, tmp_path, spec):
        """Exact full sync plans insert overwrite."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(spec))

        plan = publisher.plan_full_sync(
            make_attempt(
                spec,
                method=PUBLICATION_INSERT_OVERWRITE,
                snapshot=v3_snapshot(spec),
            ),
            spec,
        )

        assert plan.method == PUBLICATION_INSERT_OVERWRITE
        assert plan.publication_statements[0].startswith('INSERT OVERWRITE INTO "TEST_DB"."TEST_SCHEMA"."ORDERS" ')
        assert "SELECT *" not in plan.publication_statements[0]

    def test_additive_full_sync_adds_only_nullable_columns_before_overwrite(self, tmp_path, spec):
        """Additive full sync adds only nullable columns before overwrite."""
        existing = IcebergTableSpec(spec.name, spec.columns[:-1], spec.primary_key)
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(existing))

        plan = publisher.plan_full_sync(
            make_attempt(
                spec,
                method=PUBLICATION_ADDITIVE_OVERWRITE,
                snapshot=v3_snapshot(existing),
            ),
            spec,
        )

        assert plan.method == PUBLICATION_ADDITIVE_OVERWRITE
        assert plan.preparation_statements == (
            'ALTER ICEBERG TABLE "TEST_DB"."TEST_SCHEMA"."ORDERS" ADD COLUMN "_SDC_DELETED_AT" VARCHAR(134217728)',
        )

    @pytest.mark.parametrize(
        ("kind", "method"),
        (
            ("full", PUBLICATION_ADDITIVE_OVERWRITE),
            ("partial", PUBLICATION_PARTIAL_MERGE),
        ),
    )
    def test_additive_retry_plans_only_columns_remaining_after_interrupted_ddl(
        self,
        tmp_path,
        spec,
        kind,
        method,
    ):
        """Additive retry plans only columns remaining after interrupted ddl."""
        original = IcebergTableSpec(spec.name, spec.columns[:-2], spec.primary_key)
        after_first_ddl = IcebergTableSpec(spec.name, spec.columns[:-1], spec.primary_key)
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(after_first_ddl))
        attempt = make_attempt(
            spec,
            kind=kind,
            method=method,
            snapshot=v3_snapshot(original),
            context={
                "where_clause_sql": ' WHERE "ID" >= 10',
                "drop_target": False,
            },
        )

        plan = publisher.plan_full_sync(attempt, spec) if kind == "full" else publisher.plan_partial_sync(attempt, spec)

        assert plan.method == method
        assert plan.preparation_statements == (
            'ALTER ICEBERG TABLE "TEST_DB"."TEST_SCHEMA"."ORDERS" ADD COLUMN "_SDC_DELETED_AT" VARCHAR(134217728)',
        )

    def test_additive_retry_rejects_concurrent_incompatible_schema_change(self, tmp_path, spec):
        """Additive retry rejects concurrent incompatible schema change."""
        original = IcebergTableSpec(spec.name, spec.columns[:-1], spec.primary_key)
        incompatible = IcebergTableSpec(
            spec.name,
            original.columns + (IcebergColumn("UNEXPECTED", "TEXT"),),
            spec.primary_key,
        )
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(incompatible))
        attempt = make_attempt(
            spec,
            method=PUBLICATION_ADDITIVE_OVERWRITE,
            snapshot=v3_snapshot(original),
        )

        with pytest.raises(RecoveryManifestError, match="target changed"):
            publisher.plan_full_sync(attempt, spec)

    def test_incompatible_full_sync_preflights_and_plans_guarded_replacement(self, tmp_path, spec):
        """Incompatible full sync preflights and plans guarded replacement."""
        changed_columns = tuple(
            IcebergColumn(column.name, "BOOLEAN", column.nullable) if column.name == "PAYLOAD" else column
            for column in spec.columns
        )
        existing = IcebergTableSpec(spec.name, changed_columns, spec.primary_key)
        snowflake = FakeSnowflake()
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(existing))
        publisher._verify_replacement_metadata = MagicMock()  # pylint: disable=protected-access

        plan = publisher.plan_full_sync(
            make_attempt(
                spec,
                method=PUBLICATION_REPLACEMENT_CTAS,
                snapshot=v3_snapshot(existing),
                context={
                    "replacement_metadata": {
                        "table_comment": "orders",
                        "column_comments": [["PAYLOAD", "payload"]],
                    },
                },
            ),
            spec,
        )

        assert plan.method == PUBLICATION_REPLACEMENT_CTAS
        assert "CREATE OR REPLACE ICEBERG TABLE" in plan.publication_statements[0]
        assert "COPY GRANTS COPY TAGS" in plan.publication_statements[0]
        assert "\"PAYLOAD\" VARIANT COMMENT 'payload'" in plan.publication_statements[0]
        assert "COMMENT = 'orders'" in plan.publication_statements[0]
        publisher._verify_replacement_metadata.assert_called_once()  # pylint: disable=protected-access

    def test_full_sync_never_converts_native_target(self, tmp_path, spec):
        """Full sync never converts a native target."""
        table_format = TABLE_FORMAT_NATIVE
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=SnowflakeTableSnapshot(table_format, None, "id"))

        with pytest.raises(TableCompatibilityError, match="Expected managed Iceberg v3"):
            publisher.plan_full_sync(
                make_attempt(
                    spec,
                    snapshot=SnowflakeTableSnapshot(table_format, None, "id"),
                ),
                spec,
            )

    @pytest.mark.parametrize(("kind", "drop_target"), (("full", False), ("partial", True)))
    def test_text_variant_mismatch_requires_explicit_migration(self, tmp_path, spec, kind, drop_target):
        """Text variant mismatch requires explicit migration."""
        existing = IcebergTableSpec(
            spec.name,
            tuple(
                IcebergColumn(column.name, "TEXT", column.nullable) if column.name == "PAYLOAD" else column
                for column in spec.columns
            ),
            spec.primary_key,
        )
        snapshot = v3_snapshot(existing)
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=snapshot)
        attempt = make_attempt(
            spec,
            kind=kind,
            method=(PUBLICATION_REPLACEMENT_CTAS if kind == "full" else PUBLICATION_PARTIAL_REPLACEMENT_CTAS),
            snapshot=snapshot,
            context={
                "where_clause_sql": ' WHERE "ID" >= 1',
                "drop_target": drop_target,
                "end_is_unbounded": True,
                "delete_mode": "hard",
            },
        )

        with pytest.raises(TableCompatibilityError, match="explicit TEXT/VARIANT migration"):
            if kind == "full":
                publisher.plan_full_sync(attempt, spec)
            else:
                publisher.plan_partial_sync(attempt, spec)


class TestReplacementSafety:
    """Validate replacement preflight and metadata safety."""

    def test_replacement_persists_only_destination_column_comments(
        self,
        tmp_path,
        spec,
    ):
        """Removed column comments cannot block replacement or recovery."""
        existing = IcebergTableSpec(
            spec.name,
            tuple(
                IcebergColumn("LEGACY_PAYLOAD", column.data_type, column.nullable)
                if column.name == "PAYLOAD"
                else column
                for column in spec.columns
            ),
            spec.primary_key,
        )
        columns = [
            {
                "COLUMN_NAME": "UPDATED AT",
                "COMMENT": "updated comment",
                "COLUMN_DEFAULT": None,
                "IS_IDENTITY": "NO",
            },
            {
                "COLUMN_NAME": "LEGACY_PAYLOAD",
                "COMMENT": "removed comment",
                "COLUMN_DEFAULT": None,
                "IS_IDENTITY": "NO",
            },
            {
                "COLUMN_NAME": "ID",
                "COMMENT": "id comment",
                "COLUMN_DEFAULT": None,
                "IS_IDENTITY": "NO",
            },
        ]
        snowflake = FakeSnowflake(
            [
                [],
                [],
                [],
                [],
                [],
                columns,
                [
                    {
                        "name": spec.name.table,
                        "cluster_by": "",
                        "owner": "PIPELINEWISE_ROLE",
                        "owner_role_type": "ROLE",
                    }
                ],
                [{"CURRENT_ROLE": "PIPELINEWISE_ROLE"}],
                [],
                [],
            ]
        )
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(existing))

        attempt = publisher.prepare_full_sync(
            spec,
            {"lsn": "1/2"},
            recovery_identity=RECOVERY_IDENTITY,
        )

        expected_comments = [
            ["ID", "id comment"],
            ["UPDATED AT", "updated comment"],
        ]
        assert attempt.context["replacement_metadata"]["column_comments"] == expected_comments
        recovered = publisher.load_attempt(
            spec,
            expected_kind="full",
            recovery_identity=RECOVERY_IDENTITY,
        )
        assert recovered.context["replacement_metadata"]["column_comments"] == expected_comments

        publisher._verify_replacement_metadata = MagicMock()  # pylint: disable=protected-access
        statement = publisher.plan_full_sync(attempt, spec).publication_statements[0]
        assert "removed comment" not in statement
        assert '"ID" NUMBER(38,0) NOT NULL COMMENT \'id comment\'' in statement
        assert '"UPDATED AT" TIMESTAMP_NTZ(6) COMMENT \'updated comment\'' in statement

    def test_replacement_preflight_rejects_policies_or_streams(self, tmp_path, spec):
        """Replacement preflight rejects policies or streams."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake([[{"POLICY_NAME": "MASK"}]]), str(tmp_path))
        with pytest.raises(TableCompatibilityError, match="policies"):
            publisher._preflight_replacement(spec.name, spec)  # pylint: disable=protected-access

        publisher = SnowflakeIcebergPublisher(
            FakeSnowflake(
                [
                    [],
                    [],
                    [
                        {
                            "table_name": spec.name.key,
                            "source_type": "Table",
                            "name": "ORDERS_STREAM",
                        }
                    ],
                ]
            ),
            str(tmp_path),
        )
        with pytest.raises(TableCompatibilityError, match="streams"):
            publisher._preflight_replacement(spec.name, spec)  # pylint: disable=protected-access

    def test_replacement_preflight_rejects_cross_schema_view_stream(self, tmp_path, spec):
        """Replacement preflight rejects cross schema view stream."""
        stream_row = {
            "name": "CROSS_SCHEMA_STREAM",
            "database_name": spec.name.database,
            "schema_name": "OTHER_SCHEMA",
            "table_name": f"{spec.name.database}.OTHER_SCHEMA.ORDERS_VIEW",
            "source_type": "View",
            "base_tables": f"{spec.name.database}.OTHER_SCHEMA.OTHER, {spec.name.key}",
        }
        publisher = SnowflakeIcebergPublisher(
            FakeSnowflake([[], [], [stream_row]]),
            str(tmp_path),
        )

        with pytest.raises(TableCompatibilityError, match="dependent streams"):
            publisher._preflight_replacement(spec.name, spec)  # pylint: disable=protected-access

    def test_replacement_preflight_fails_closed_on_invalid_view_stream_metadata(
        self,
        tmp_path,
        spec,
    ):
        """Replacement preflight fails closed on invalid view stream metadata."""
        stream_row = {
            "name": "INVALID_STREAM",
            "table_name": f"{spec.name.database}.OTHER_SCHEMA.ORDERS_VIEW",
            "source_type": "View",
            "base_tables": "not-qualified",
        }
        publisher = SnowflakeIcebergPublisher(
            FakeSnowflake([[], [], [stream_row]]),
            str(tmp_path),
        )

        with pytest.raises(TableCompatibilityError, match="invalid source object metadata"):
            publisher._preflight_replacement(spec.name, spec)  # pylint: disable=protected-access

    def test_replacement_preflight_rejects_column_tags_defaults_and_identity(self, tmp_path, spec):
        """Replacement preflight rejects column tags defaults and identity."""
        publisher = SnowflakeIcebergPublisher(
            FakeSnowflake([[], [{"TAG_NAME": "PII", "LEVEL": "COLUMN", "APPLY_METHOD": "MANUAL"}]]),
            str(tmp_path),
        )
        with pytest.raises(TableCompatibilityError, match="column tags"):
            publisher._preflight_replacement(spec.name, spec)  # pylint: disable=protected-access

        for column in (
            {"COLUMN_NAME": "ID", "COLUMN_DEFAULT": "1", "IS_IDENTITY": "NO"},
            {"COLUMN_NAME": "ID", "COLUMN_DEFAULT": None, "IS_IDENTITY": "YES"},
        ):
            publisher = SnowflakeIcebergPublisher(
                FakeSnowflake([[], [], [], [], [], [column]]),
                str(tmp_path),
            )
            with pytest.raises(TableCompatibilityError, match="defaults or identity"):
                publisher._preflight_replacement(spec.name, spec)  # pylint: disable=protected-access

    def test_replacement_preflight_allows_table_tags_inherited_by_columns(self, tmp_path, spec):
        """Replacement preflight allows table tags inherited by columns."""
        snowflake = FakeSnowflake(
            [
                [],
                [{"TAG_NAME": "CLASSIFICATION", "LEVEL": "TABLE", "APPLY_METHOD": "INHERITED"}],
                [],
                [],
                [],
                [
                    {
                        "COLUMN_NAME": column.name,
                        "COMMENT": None,
                        "COLUMN_DEFAULT": None,
                        "IS_IDENTITY": "NO",
                    }
                    for column in spec.columns
                ],
                [
                    {
                        "name": spec.name.table,
                        "owner": "OWNER_ROLE",
                        "owner_role_type": "ROLE",
                        "cluster_by": "",
                    }
                ],
                [{"CURRENT_ROLE": "OWNER_ROLE"}],
                [],
                [
                    {
                        "TAG_DATABASE": spec.name.database,
                        "TAG_SCHEMA": spec.name.schema,
                        "TAG_NAME": "CLASSIFICATION",
                        "TAG_VALUE": "PUBLIC",
                        "LEVEL": "TABLE",
                        "APPLY_METHOD": "MANUAL",
                    }
                ],
            ]
        )

        publisher = SnowflakeIcebergPublisher(
            snowflake,
            str(tmp_path),
        )
        metadata = publisher._preflight_replacement(  # pylint: disable=protected-access
            spec.name,
            spec,
        )

        assert metadata.table_tags == (
            (
                spec.name.database,
                spec.name.schema,
                "CLASSIFICATION",
                "PUBLIC",
            ),
        )

    @pytest.mark.parametrize(
        "constraint_row",
        (
            {
                "DEPENDENCY_KIND": "DIRECT",
                "CONSTRAINT_TYPE": "UNIQUE",
                "CONSTRAINT_NAME": "ORDERS_UNIQUE",
            },
            {
                "DEPENDENCY_KIND": "INBOUND_FOREIGN_KEY",
                "CONSTRAINT_TYPE": "FOREIGN KEY",
                "CONSTRAINT_NAME": "ORDER_ITEM_ORDERS_FK",
            },
        ),
    )
    def test_replacement_preflight_rejects_constraints(self, tmp_path, spec, constraint_row):
        """Replacement preflight rejects constraints."""
        publisher = SnowflakeIcebergPublisher(
            FakeSnowflake([[], [], [], [constraint_row]]),
            str(tmp_path),
        )

        with pytest.raises(
            TableCompatibilityError,
            match="secondary constraints or inbound foreign keys",
        ):
            publisher._preflight_replacement(spec.name, spec)  # pylint: disable=protected-access

    def test_replacement_preflight_rejects_cross_database_inbound_foreign_key(self, tmp_path, spec):
        """Replacement preflight rejects cross database inbound foreign key."""
        exported_key = {
            "pk_database_name": spec.name.database,
            "pk_schema_name": spec.name.schema,
            "pk_table_name": spec.name.table,
            "fk_database_name": "OTHER_DATABASE",
            "fk_schema_name": "PUBLIC",
            "fk_table_name": "ORDER_ITEMS",
        }
        snowflake = FakeSnowflake([[], [], [], [], [exported_key]])
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))

        with pytest.raises(
            TableCompatibilityError,
            match="secondary constraints or inbound foreign keys",
        ):
            publisher._preflight_replacement(spec.name, spec)  # pylint: disable=protected-access

        assert snowflake.queries[-1][0] == ('SHOW EXPORTED KEYS IN TABLE "TEST_DB"."TEST_SCHEMA"."ORDERS"')

    def test_replacement_metadata_comments_are_captured_and_restored(self, tmp_path, spec):
        """Replacement metadata comments are captured and restored."""
        columns = [
            {
                "COLUMN_NAME": "ID",
                "COMMENT": "customer's id",
                "COLUMN_DEFAULT": None,
                "IS_IDENTITY": "NO",
            },
            {
                "COLUMN_NAME": "PAYLOAD",
                "COMMENT": None,
                "COLUMN_DEFAULT": None,
                "IS_IDENTITY": "NO",
            },
        ]
        snowflake = FakeSnowflake(
            [
                [],
                [],
                [],
                [],
                [],
                columns,
                [
                    {
                        "name": "ORDERS",
                        "is_iceberg": True,
                        "comment": "order table",
                        "cluster_by": "",
                        "owner": "PIPELINEWISE_ROLE",
                        "owner_role_type": "ROLE",
                    }
                ],
                [{"CURRENT_ROLE": "PIPELINEWISE_ROLE"}],
                [
                    {
                        "privilege": "OWNERSHIP",
                        "granted_to": "ROLE",
                        "grantee_name": "PIPELINEWISE_ROLE",
                        "grant_option": "true",
                    },
                    {
                        "privilege": "SELECT",
                        "granted_to": "ROLE",
                        "grantee_name": "READER_ROLE",
                        "grant_option": "false",
                    },
                ],
                [
                    {
                        "TAG_DATABASE": "GOVERNANCE",
                        "TAG_SCHEMA": "TAGS",
                        "TAG_NAME": "CLASSIFICATION",
                        "TAG_VALUE": "INTERNAL",
                        "APPLY_METHOD": "MANUAL",
                        "LEVEL": "TABLE",
                    }
                ],
            ]
        )
        publisher = SnowflakeIcebergPublisher(snowflake, str(tmp_path))

        metadata = publisher._preflight_replacement(spec.name, spec)  # pylint: disable=protected-access
        attempt = make_attempt(
            spec,
            phase=PHASE_PUBLISHED,
            method=PUBLICATION_REPLACEMENT_CTAS,
            context={"replacement_metadata": metadata.as_dict()},
        )
        publisher.restore_metadata(attempt)

        assert metadata.table_comment == "order table"
        assert metadata.column_comments == (("ID", "customer's id"),)
        assert metadata.owner == "PIPELINEWISE_ROLE"
        assert metadata.explicit_grants == (("SELECT", "ROLE", "READER_ROLE", False),)
        assert metadata.table_tags == (("GOVERNANCE", "TAGS", "CLASSIFICATION", "INTERNAL"),)
        assert snowflake.queries[-1][0] == (
            'ALTER ICEBERG TABLE "TEST_DB"."TEST_SCHEMA"."ORDERS" ALTER COLUMN "ID" COMMENT \'customer\'\'s id\''
        )

    @pytest.mark.parametrize(
        ("table_row", "role_rows", "message"),
        (
            (
                {"name": "ORDERS", "owner": "OWNER_ROLE", "cluster_by": "LINEAR(ID)"},
                [],
                "clustering key",
            ),
            (
                {
                    "name": "ORDERS",
                    "owner": "OWNER_ROLE",
                    "owner_role_type": "ROLE",
                    "cluster_by": "",
                },
                [{"CURRENT_ROLE": "OTHER_ROLE"}],
                "owning role OWNER_ROLE",
            ),
            (
                {
                    "name": "ORDERS",
                    "owner": "",
                    "owner_role_type": "ROLE",
                    "cluster_by": "",
                },
                [{"CURRENT_ROLE": "OWNER_ROLE"}],
                "Cannot prove account-role ownership",
            ),
            (
                {
                    "name": "ORDERS",
                    "owner": "OWNER_ROLE",
                    "owner_role_type": "DATABASE_ROLE",
                    "cluster_by": "",
                },
                [{"CURRENT_ROLE": "OWNER_ROLE"}],
                "Cannot prove account-role ownership",
            ),
        ),
    )
    def test_replacement_preflight_requires_unclustered_table_owned_by_current_role(
        self,
        tmp_path,
        spec,
        table_row,
        role_rows,
        message,
    ):
        """Replacement preflight requires unclustered table owned by current role."""
        columns = [
            {
                "COLUMN_NAME": "ID",
                "COMMENT": None,
                "COLUMN_DEFAULT": None,
                "IS_IDENTITY": "NO",
            }
        ]
        publisher = SnowflakeIcebergPublisher(
            FakeSnowflake([[], [], [], [], [], columns, [table_row], role_rows]),
            str(tmp_path),
        )

        with pytest.raises(TableCompatibilityError, match=message):
            publisher._preflight_replacement(spec.name, spec)  # pylint: disable=protected-access

    def test_replacement_metadata_verification_rejects_grant_or_tag_drift(self, tmp_path, spec):
        """Replacement metadata verification rejects grant or tag drift."""
        expected = SnowflakeTableMetadata(
            owner="PIPELINEWISE_ROLE",
            explicit_grants=(("SELECT", "ROLE", "READER_ROLE", False),),
            table_tags=(("GOVERNANCE", "TAGS", "CLASSIFICATION", "INTERNAL"),),
        )
        actual = SnowflakeTableMetadata(
            owner="PIPELINEWISE_ROLE",
            explicit_grants=(("SELECT", "ROLE", "OTHER_ROLE", False),),
            table_tags=expected.table_tags,
        )
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher._preflight_replacement = MagicMock(return_value=actual)  # pylint: disable=protected-access
        attempt = make_attempt(
            spec,
            method=PUBLICATION_REPLACEMENT_CTAS,
            context={"replacement_metadata": expected.as_dict()},
        )

        with pytest.raises(RecoveryManifestError, match="metadata changed"):
            publisher._verify_replacement_metadata(attempt)  # pylint: disable=protected-access

    def test_partial_sync_plans_one_idempotent_transaction(self, tmp_path, spec):
        """Partial sync plans one idempotent transaction."""
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(spec))
        attempt = make_attempt(
            spec,
            kind="partial",
            context={"where_clause_sql": ' WHERE "ID" >= 10', "drop_target": False},
            method=PUBLICATION_PARTIAL_MERGE,
            snapshot=v3_snapshot(spec),
        )

        plan = publisher.plan_partial_sync(attempt, spec)

        assert plan.method == PUBLICATION_PARTIAL_MERGE
        assert len(plan.publication_statements) == 3
        assert plan.publication_statements[0].startswith("UPDATE ")
        assert plan.publication_statements[1].startswith("MERGE INTO ")
        assert plan.publication_statements[2].startswith("DELETE FROM ")
        assert all('"ID" >= 10' in query for query in (plan.publication_statements[0], plan.publication_statements[2]))

    def test_partial_sync_without_primary_key_fails_before_dml(self, tmp_path, spec):
        """Partial sync without primary key fails before dml."""
        no_key = IcebergTableSpec(spec.name, tuple(replace_nullable(column) for column in spec.columns), ())
        publisher = SnowflakeIcebergPublisher(FakeSnowflake(), str(tmp_path))
        publisher.inspect_table = MagicMock(return_value=v3_snapshot(no_key))
        attempt = make_attempt(
            no_key,
            kind="partial",
            context={"where_clause_sql": " WHERE 1=1"},
            method=PUBLICATION_PARTIAL_MERGE,
            snapshot=v3_snapshot(no_key),
        )

        with pytest.raises(TableCompatibilityError, match="requires a primary key"):
            publisher.plan_partial_sync(attempt, no_key)
