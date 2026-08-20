"""Tests for Snowflake Iceberg models and canonical schema handling."""

import pytest

from pipelinewise.fastsync.commons import snowflake_iceberg_versions as versions
from pipelinewise.fastsync.commons.snowflake_iceberg import (
    IcebergColumn,
    IcebergTableSpec,
    SnowflakeObjectName,
    TableCompatibilityError,
    canonical_iceberg_type,
    sql_string_literal,
)
from pipelinewise.fastsync.commons.snowflake_types import (
    SNOWFLAKE_MAX_VARCHAR_LENGTH,
)
from tests.units.fastsync.commons.snowflake_iceberg_test_helpers import (
    future_version_spec,
)


class TestIcebergModel:
    """Validate Iceberg model behavior."""

    def test_object_name_quotes_exact_identifiers_and_suffixes(self):
        """Object name quotes exact identifiers and suffixes."""
        name = SnowflakeObjectName.parse('DB."Odd.Schema"."T""able"')

        assert name == SnowflakeObjectName("DB", "Odd.Schema", 'T"able')
        assert name.quoted == '"DB"."Odd.Schema"."T""able"'
        assert name.with_suffix("_NATIVE").quoted == '"DB"."Odd.Schema"."T""able_NATIVE"'

    def test_object_name_parses_comma_separated_snowflake_metadata(self):
        """Object name parses comma separated snowflake metadata."""
        names = SnowflakeObjectName.parse_list('DB.SCHEMA.FIRST, "Odd, DB"."Odd.Schema"."T""able"')

        assert names == (
            SnowflakeObjectName("DB", "SCHEMA", "FIRST"),
            SnowflakeObjectName("Odd, DB", "Odd.Schema", 'T"able'),
        )

    @pytest.mark.parametrize(
        ('value', 'literal'),
        (
            ("customer's id", "'customer''s id'"),
            ('source: C:\\data\\', "'source: C:\\\\data\\\\'"),
            ("x\\' || CURRENT_USER() || '", "'x\\\\'' || CURRENT_USER() || '''"),
        ),
    )
    def test_string_literal_escapes_snowflake_quotes_and_backslashes(
        self, value, literal
    ):
        """Metadata text remains one valid Snowflake string literal."""
        assert sql_string_literal(value) == literal

    def test_staging_name_is_stable_unique_and_length_safe(self):
        """Staging name is stable unique and length safe."""
        target = SnowflakeObjectName("DB", "SCHEMA", "T" * 255)

        staging = target.staging_name("load-one")

        assert staging == target.staging_name("load-one")
        assert staging != target.staging_name("load-two")
        assert len(staging) == 255
        assert staging.endswith("_PW_ICEBERG_7882F3D1435F7F0A")

    @pytest.mark.parametrize(
        ("source_type", "expected"),
        (
            ("TEXT", "VARCHAR(134217728)"),
            ("varchar(100)", "VARCHAR(134217728)"),
            ("BINARY", "BINARY(67108864)"),
            ("NUMBER", "NUMBER(38,0)"),
            ("decimal(20, 4)", "NUMBER(20,4)"),
            ("FLOAT", "DOUBLE"),
            ("FLOAT4", "DOUBLE"),
            ("FLOAT8", "DOUBLE"),
            ("REAL", "DOUBLE"),
            ("DOUBLE", "DOUBLE"),
            ("DOUBLE PRECISION", "DOUBLE"),
            ("TIME(9)", "TIME(6)"),
            ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ(6)"),
            ("TIMESTAMP_TZ", "TIMESTAMP_LTZ(6)"),
            ("VARIANT", "VARIANT"),
        ),
    )
    def test_canonical_iceberg_types(self, source_type, expected):
        """Canonical iceberg types."""
        assert canonical_iceberg_type(source_type) == expected

    def test_fastsync_spec_is_explicit_and_adds_metadata(self, spec):
        """Fastsync spec is explicit and adds metadata."""
        assert [column.name for column in spec.columns] == [
            "ID",
            "PAYLOAD",
            "UPDATED AT",
            "_SDC_EXTRACTED_AT",
            "_SDC_BATCHED_AT",
            "_SDC_DELETED_AT",
        ]
        assert spec.columns[0].definition == '"ID" NUMBER(38,0) NOT NULL'
        assert spec.columns[1].projection() == 'CAST("PAYLOAD" AS VARIANT) AS "PAYLOAD"'
        assert spec.columns[-1].definition == '"_SDC_DELETED_AT" VARCHAR(134217728)'
        assert spec.primary_key_clause == ', PRIMARY KEY ("ID")'

    def test_float_column_uses_lossless_iceberg_double_ddl_and_projection(self):
        """Snowflake FLOAT values remain 64-bit when written to Iceberg."""
        column = IcebergColumn("MEASUREMENT", "FLOAT")

        assert column.definition == '"MEASUREMENT" DOUBLE'
        assert column.projection() == 'CAST("MEASUREMENT" AS DOUBLE) AS "MEASUREMENT"'

    def test_schema_fingerprint_includes_column_and_primary_key_order(self, spec):
        """Schema fingerprint includes column and primary key order."""
        reordered = IcebergTableSpec(spec.name, tuple(reversed(spec.columns)), spec.primary_key)
        changed_key = IcebergTableSpec(spec.name, spec.columns, ())

        assert reordered.fingerprint != spec.fingerprint
        assert changed_key.fingerprint != spec.fingerprint

    def test_table_spec_rejects_columns_from_multiple_managed_versions(
        self,
        monkeypatch,
    ):
        """One table specification cannot mix version-specific type strategies."""
        future_spec = future_version_spec()
        monkeypatch.setattr(
            versions,
            'MANAGED_ICEBERG_VERSION_SPECS',
            versions.managed_iceberg_version_registry(
                versions.MANAGED_ICEBERG_V3_SPEC,
                future_spec,
            ),
        )
        v3_column = IcebergColumn('V3_ID', 'NUMBER')
        v4_column = IcebergColumn(
            'V4_ID',
            'NUMBER',
            iceberg_version=4,
        )

        assert v4_column.data_type == 'NUMBER(20,0)'
        with pytest.raises(ValueError, match='same managed version'):
            IcebergTableSpec(
                SnowflakeObjectName('DB', 'SCHEMA', 'TABLE'),
                (v3_column, v4_column),
            )

    def test_snowflake_column_metadata_preserves_precision_and_nullability(self):
        """Snowflake column metadata preserves precision and nullability."""
        column = IcebergColumn.from_snowflake_row(
            {
                "COLUMN_NAME": "AMOUNT",
                "DATA_TYPE": "NUMBER",
                "NUMERIC_PRECISION": 27,
                "NUMERIC_SCALE": 9,
                "IS_NULLABLE": "NO",
            }
        )

        assert column == IcebergColumn("AMOUNT", "NUMBER(27,9)", nullable=False)

    def test_snowflake_string_metadata_requires_canonical_varchar_width(self):
        """Existing managed-v3 strings must use the physical maximum width."""
        column = IcebergColumn.from_snowflake_row(
            {
                "COLUMN_NAME": "BODY",
                "DATA_TYPE": "TEXT",
                "CHARACTER_MAXIMUM_LENGTH": SNOWFLAKE_MAX_VARCHAR_LENGTH,
                "IS_NULLABLE": "YES",
            }
        )

        assert column == IcebergColumn("BODY", "VARCHAR(134217728)")

        with pytest.raises(TableCompatibilityError) as error:
            IcebergColumn.from_snowflake_row(
                {
                    "COLUMN_NAME": "BODY",
                    "DATA_TYPE": "TEXT",
                    "CHARACTER_MAXIMUM_LENGTH": 16777216,
                    "IS_NULLABLE": "YES",
                }
            )

        assert 'CHARACTER_MAXIMUM_LENGTH is 16777216' in str(error.value)
        assert 'ALTER ICEBERG TABLE' in str(error.value)
        assert 'recreate the table' in str(error.value)
        assert 'does not alter existing column widths automatically' in str(error.value)

    def test_snowflake_string_metadata_requires_width_evidence(self):
        """Missing string-width metadata fails closed."""
        with pytest.raises(
            TableCompatibilityError,
            match="CHARACTER_MAXIMUM_LENGTH is 'missing'",
        ):
            IcebergColumn.from_snowflake_row(
                {
                    "COLUMN_NAME": "BODY",
                    "DATA_TYPE": "TEXT",
                    "IS_NULLABLE": "YES",
                }
            )

    @pytest.mark.parametrize("data_type", ("TIME", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ"))
    def test_snowflake_column_metadata_rejects_noncanonical_datetime_precision(self, data_type):
        """Snowflake column metadata rejects noncanonical datetime precision."""
        with pytest.raises(TableCompatibilityError, match="precision 9; expected 6"):
            IcebergColumn.from_snowflake_row(
                {
                    "COLUMN_NAME": "EVENT_TIME",
                    "DATA_TYPE": data_type,
                    "DATETIME_PRECISION": 9,
                    "IS_NULLABLE": "YES",
                }
            )
