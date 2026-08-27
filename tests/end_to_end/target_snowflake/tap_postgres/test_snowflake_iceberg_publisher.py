"""Credentialed recovery checks for the shared Snowflake Iceberg publisher."""

import os
from uuid import uuid4

import pytest

from pipelinewise.fastsync.commons.snowflake_iceberg import (
    PHASE_STAGING_CREATED,
    PHASE_PUBLISHED,
    PHASE_SUBMITTED,
    PartialSyncBoundary,
    PUBLICATION_ADDITIVE_OVERWRITE,
    PUBLICATION_INSERT_OVERWRITE,
    PUBLICATION_MISSING_CTAS,
    PUBLICATION_PARTIAL_MERGE,
    RECOVERY_FINALIZE,
    RECOVERY_PUBLISH,
    RECOVERY_RESTART_STAGING,
    IcebergTableSpec,
    MANAGED_ICEBERG_V3_TABLE_OPTIONS,
    SnowflakeIcebergPublisher,
    SnowflakeObjectName,
    SnowflakeQueryAdapter,
    StagingPrimaryKeyError,
    TABLE_FORMAT_MANAGED_ICEBERG_V3,
    TABLE_FORMAT_MISSING,
    TableCompatibilityError,
)
from pipelinewise.fastsync.commons.snowflake_iceberg_parameters import assert_managed_v3_copy_on_write_parameter
from pipelinewise.fastsync.commons.snowflake_iceberg_versions import (
    build_recovery_identity,
)


REQUIRED_SNOWFLAKE_ENV = (
    "TARGET_SNOWFLAKE_ACCOUNT",
    "TARGET_SNOWFLAKE_DBNAME",
    "TARGET_SNOWFLAKE_USER",
    "TARGET_SNOWFLAKE_PRIVATE_KEY",
    "TARGET_SNOWFLAKE_WAREHOUSE",
)
RECOVERY_IDENTITY = build_recovery_identity(
    "fastsync",
    {"route": "credentialed-publisher-test"},
    transformation_config={},
    stream_identity={
        "tap_id": "credentialed-publisher-test",
        "route": "credentialed-publisher-test",
        "table": "source.table",
    },
    target_table_format="iceberg",
    iceberg_version=3,
)


class LostPublicationResponseAdapter:
    """Execute one publication successfully, then simulate a lost response."""

    def __init__(self, adapter, statement_prefix="INSERT OVERWRITE"):
        self.adapter = adapter
        self.connection_config = adapter.connection_config
        self.statement_prefix = statement_prefix
        self.response_was_lost = False

    def create_query_tag(self, query_tag_props=None):
        """Use the production adapter's exact query-tag serialization."""
        return self.adapter.create_query_tag(query_tag_props)

    def query(self, query, params=None, query_tag_props=None):
        """Lose the first successful matching publication response only."""
        if query.startswith(self.statement_prefix) and not self.response_was_lost:
            self.adapter.query(query, params, query_tag_props)
            self.response_was_lost = True
            raise ConnectionError("simulated response loss after Snowflake committed")
        return self.adapter.query(query, params, query_tag_props)

    def execute_transaction(self, queries, query_tag_props=None):
        """Delegate transactions unchanged."""
        return self.adapter.execute_transaction(queries, query_tag_props)


class LostTransactionResponseAdapter(LostPublicationResponseAdapter):
    """Commit one partial transaction, then simulate a lost response."""

    def execute_transaction(self, queries, query_tag_props=None):
        """Lose the first successful transaction response only."""
        result = self.adapter.execute_transaction(queries, query_tag_props)
        if not self.response_was_lost:
            self.response_was_lost = True
            raise ConnectionError("simulated response loss after Snowflake committed")
        return result


class NondeterministicMergeAllowedAdapter(SnowflakeQueryAdapter):
    """Prove the staging guard does not rely on Snowflake's MERGE default."""

    def __init__(self, connection_config):
        super().__init__(connection_config)
        self.transactions = []

    def open_connection(
        self,
        query_tag_props=None,
        autocommit=True,
        *,
        login_timeout=None,
        network_timeout=None,
        socket_timeout=None,
    ):
        """Disable nondeterministic MERGE errors only for this test connection."""
        connection = super().open_connection(
            query_tag_props,
            autocommit,
            login_timeout=login_timeout,
            network_timeout=network_timeout,
            socket_timeout=socket_timeout,
        )
        with connection.cursor() as cursor:
            cursor.execute("ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = FALSE")
        return connection

    def execute_transaction(self, queries, query_tag_props=None):
        """Record any attempted publication transaction before delegating."""
        self.transactions.append((tuple(queries), query_tag_props))
        return super().execute_transaction(queries, query_tag_props)


def _connection_config():
    missing = [name for name in REQUIRED_SNOWFLAKE_ENV if not os.environ.get(name)]
    if missing:
        pytest.fail(f"Missing Snowflake integration environment: {', '.join(missing)}")
    return {
        "account": os.environ["TARGET_SNOWFLAKE_ACCOUNT"],
        "dbname": os.environ["TARGET_SNOWFLAKE_DBNAME"],
        "user": os.environ["TARGET_SNOWFLAKE_USER"],
        "private_key": os.environ["TARGET_SNOWFLAKE_PRIVATE_KEY"],
        "warehouse": os.environ["TARGET_SNOWFLAKE_WAREHOUSE"],
        "role": os.environ.get("TARGET_SNOWFLAKE_ROLE"),
    }


def _create_native_stage(adapter, spec, attempt, row_id=None, note=None):
    stage = spec.name.with_table(attempt.staging_table)
    adapter.query(f"CREATE TABLE {stage.quoted} ({spec.column_definitions})")
    if row_id is None:
        return
    expressions = {
        "ID": str(row_id),
        "PAYLOAD": f"PARSE_JSON('{{\"id\":{row_id}}}')",
        "NOTE": (str(note) if isinstance(note, (int, float)) else f"'{note}'" if note is not None else "NULL"),
        "_SDC_EXTRACTED_AT": "CURRENT_TIMESTAMP()",
        "_SDC_BATCHED_AT": "CURRENT_TIMESTAMP()",
        "_SDC_DELETED_AT": "NULL",
    }
    values = ", ".join(expressions[column.name] for column in spec.columns)
    adapter.query(f"INSERT INTO {stage.quoted} ({spec.quoted_columns}) SELECT {values}")


def _create_recorded_native_stage(
    adapter,
    publisher,
    spec,
    attempt,
    row_id=None,
    note=None,
):
    """Create a fixture-owned stage through the production manifest phases."""
    publisher.record_planned_uploads(attempt, [])
    publisher.record_uploaded(attempt, [])
    _create_native_stage(adapter, spec, attempt, row_id=row_id, note=note)
    publisher.record_staging_created(attempt)


def _finish_attempt(adapter, publisher, attempt):
    adapter.query(f"DROP TABLE IF EXISTS {attempt.target.with_table(attempt.staging_table).quoted}")
    completed_actions = ["grants", "s3_cleanup", "staging_cleanup"]
    if attempt.manifest_payload.replacement_metadata is not None:
        completed_actions.append("metadata")
    publisher.mark_finalized(attempt, completed_actions)
    publisher.complete_state_handoff(attempt)


def _record_staged(publisher, attempt, spec, loaded_row_count):
    row_count, row_fingerprint = publisher.staging_evidence(attempt, spec, loaded_row_count)
    publisher.record_staged(
        attempt,
        row_count=row_count,
        row_fingerprint=row_fingerprint,
    )


def _assert_copy_on_write_table(adapter, table):
    rows = adapter.query(
        "SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR' "
        f'IN TABLE {table.quoted}'
    )
    assert_managed_v3_copy_on_write_parameter(rows, table)


def _recover_missing_ctas_response_loss(adapter, tmp_path, spec):
    interrupted = SnowflakeIcebergPublisher(
        LostPublicationResponseAdapter(adapter, "CREATE ICEBERG TABLE"),
        str(tmp_path),
    )
    assert interrupted.discover_table_format(spec.name.schema, spec.name.table) == TABLE_FORMAT_MISSING
    attempt = interrupted.prepare_full_sync(
        spec,
        {"boundary": "lost-ctas-response"},
        recovery_identity=RECOVERY_IDENTITY,
    )
    assert attempt.method == PUBLICATION_MISSING_CTAS
    _create_recorded_native_stage(
        adapter,
        interrupted,
        spec,
        attempt,
        row_id=7,
    )
    _record_staged(interrupted, attempt, spec, 1)

    with pytest.raises(ConnectionError, match="simulated response loss"):
        interrupted.publish_full_sync(attempt, spec)
    assert (
        interrupted.load_attempt(
            spec,
            expected_kind="full",
            recovery_identity=RECOVERY_IDENTITY,
        ).phase
        == PHASE_SUBMITTED
    )

    recovered_publisher = SnowflakeIcebergPublisher(adapter, str(tmp_path))
    recovered_attempt = recovered_publisher.load_attempt(
        spec,
        expected_kind="full",
        recovery_identity=RECOVERY_IDENTITY,
    )
    assert recovered_publisher.reconcile(recovered_attempt, spec).action == RECOVERY_FINALIZE
    assert recovered_attempt.query_id
    snapshot = recovered_publisher.inspect_table(spec.name)
    assert snapshot.table_format == TABLE_FORMAT_MANAGED_ICEBERG_V3
    assert snapshot.spec == spec
    _assert_copy_on_write_table(adapter, spec.name)
    assert adapter.query(
        f'SELECT "ID", GET("PAYLOAD", \'id\')::NUMBER AS "PAYLOAD_ID", '
        f'"_SDC_DELETED_AT" FROM {spec.name.quoted}'
    ) == [{"ID": 7, "PAYLOAD_ID": 7, "_SDC_DELETED_AT": None}]

    staging_table = recovered_attempt.staging_table
    _finish_attempt(adapter, recovered_publisher, recovered_attempt)
    assert (
        recovered_publisher.load_attempt(
            spec,
            expected_kind="full",
            recovery_identity=RECOVERY_IDENTITY,
        )
        is None
    )
    assert (
        recovered_publisher.discover_table_format(spec.name.schema, staging_table)
        == TABLE_FORMAT_MISSING
    )


def _publish_initial(adapter, publisher, spec):
    initial = publisher.prepare_full_sync(
        spec,
        {"boundary": "initial"},
        recovery_identity=RECOVERY_IDENTITY,
    )
    _create_recorded_native_stage(adapter, publisher, spec, initial)
    _record_staged(publisher, initial, spec, 0)
    publisher.publish_full_sync(initial, spec)
    assert publisher.discover_table_format(spec.name.schema, spec.name.table) == TABLE_FORMAT_MANAGED_ICEBERG_V3
    _assert_copy_on_write_table(adapter, spec.name)
    _finish_attempt(adapter, publisher, initial)


def _publish_empty_overwrite(adapter, publisher, spec):
    adapter.query(
        f"INSERT INTO {spec.name.quoted} ({spec.quoted_columns}) "
        "SELECT 99, PARSE_JSON('{\"stale\":true}'), "
        "CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL"
    )
    attempt = publisher.prepare_full_sync(
        spec,
        {"boundary": "empty"},
        recovery_identity=RECOVERY_IDENTITY,
    )
    assert attempt.method == PUBLICATION_INSERT_OVERWRITE
    _create_recorded_native_stage(adapter, publisher, spec, attempt)
    _record_staged(publisher, attempt, spec, 0)
    publisher.publish_full_sync(attempt, spec)
    assert adapter.query(f"SELECT COUNT(*) AS ROW_COUNT FROM {spec.name.quoted}")[0]["ROW_COUNT"] == 0
    _finish_attempt(adapter, publisher, attempt)


def _publish_additive_overwrite(adapter, publisher, spec):
    extended_spec = IcebergTableSpec.from_fastsync(
        spec.name.database,
        spec.name.schema,
        spec.name.table,
        ['"ID" NUMBER', '"PAYLOAD" VARIANT', '"NOTE" VARCHAR'],
        ['"ID"'],
    )
    attempt = publisher.prepare_full_sync(
        extended_spec,
        {"boundary": "additive"},
        recovery_identity=RECOVERY_IDENTITY,
    )
    assert attempt.method == PUBLICATION_ADDITIVE_OVERWRITE
    _create_recorded_native_stage(
        adapter,
        publisher,
        extended_spec,
        attempt,
        row_id=1,
        note="résumé",
    )
    _record_staged(publisher, attempt, extended_spec, 1)
    publisher.publish_full_sync(attempt, extended_spec)
    assert adapter.query(f'SELECT "ID", "NOTE" FROM {spec.name.quoted}') == [{"ID": 1, "NOTE": "résumé"}]
    _finish_attempt(adapter, publisher, attempt)
    return extended_spec


def _recover_full_response_loss(adapter, tmp_path, spec):
    interrupted = SnowflakeIcebergPublisher(
        LostPublicationResponseAdapter(adapter),
        str(tmp_path),
    )
    attempt = interrupted.prepare_full_sync(
        spec,
        {"boundary": "lost-response"},
        recovery_identity=RECOVERY_IDENTITY,
    )
    _create_recorded_native_stage(
        adapter,
        interrupted,
        spec,
        attempt,
        row_id=2,
        note="recovered",
    )
    _record_staged(interrupted, attempt, spec, 1)
    with pytest.raises(ConnectionError, match="simulated response loss"):
        interrupted.publish_full_sync(attempt, spec)
    assert (
        interrupted.load_attempt(
            spec,
            expected_kind="full",
            recovery_identity=RECOVERY_IDENTITY,
        ).phase
        == PHASE_SUBMITTED
    )

    recovered_publisher = SnowflakeIcebergPublisher(adapter, str(tmp_path))
    recovered_attempt = recovered_publisher.load_attempt(
        spec,
        expected_kind="full",
        recovery_identity=RECOVERY_IDENTITY,
    )
    assert (
        recovered_publisher.reconcile(
            recovered_attempt,
            spec,
        ).action
        == RECOVERY_FINALIZE
    )
    assert adapter.query(f'SELECT "ID", "NOTE" FROM {spec.name.quoted}') == [{"ID": 2, "NOTE": "recovered"}]
    _finish_attempt(adapter, recovered_publisher, recovered_attempt)


def _reject_transformed_key_collisions(connection_config, tmp_path, spec):
    adapter = NondeterministicMergeAllowedAdapter(connection_config)
    publisher = SnowflakeIcebergPublisher(adapter, str(tmp_path))
    before = adapter.query(
        f'SELECT "ID", "NOTE" FROM {spec.name.quoted} ORDER BY "ID"'
    )
    assert any(row["ID"] == 2 for row in before)
    attempt = publisher.prepare_partial_sync(
        spec,
        {"boundary": "transformed-duplicate-keys"},
        PartialSyncBoundary(
            'ID',
            start_value=2,
            end_value=5,
        ),
        recovery_identity=RECOVERY_IDENTITY,
    )
    assert attempt.method == PUBLICATION_PARTIAL_MERGE
    _create_recorded_native_stage(
        adapter,
        publisher,
        spec,
        attempt,
        row_id=2,
        note="matched-first",
    )
    stage = attempt.target.with_table(attempt.staging_table)
    for row_id, note in (
        (3, "matched-second"),
        (4, "unmatched-first"),
        (5, "unmatched-second"),
    ):
        adapter.query(
            f"INSERT INTO {stage.quoted} ({spec.quoted_columns}) "
            f"SELECT {row_id}, PARSE_JSON('{{\"id\":{row_id}}}'), '{note}', "
            "CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL"
        )
    adapter.query(
        f'UPDATE {stage.quoted} SET "ID" = CASE "ID" '
        'WHEN 3 THEN 2 WHEN 5 THEN 4 ELSE "ID" END WHERE "ID" IN (3, 5)'
    )

    with pytest.raises(StagingPrimaryKeyError, match="duplicate groups"):
        publisher.staging_evidence(attempt, spec, loaded_row_count=4)

    assert adapter.transactions == []
    assert adapter.query(
        f'SELECT "ID", "NOTE" FROM {spec.name.quoted} ORDER BY "ID"'
    ) == before
    recovered = publisher.load_attempt(
        spec,
        expected_kind="partial",
        recovery_identity=RECOVERY_IDENTITY,
    )
    assert recovered.phase == PHASE_STAGING_CREATED
    assert publisher.reconcile(recovered, spec).action == RECOVERY_RESTART_STAGING
    adapter.query(f"DROP TABLE {stage.quoted}")
    publisher.abort(recovered)


def _recover_partial_response_loss(adapter, tmp_path, spec):
    adapter.query(f"DELETE FROM {spec.name.quoted}")
    for row_id in (1, 2, 3):
        adapter.query(
            f"INSERT INTO {spec.name.quoted} ({spec.quoted_columns}) "
            f"SELECT {row_id}, PARSE_JSON('{{\"id\":{row_id}}}'), "
            f"'before-{row_id}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL"
        )
    publisher = SnowflakeIcebergPublisher(
        LostTransactionResponseAdapter(adapter),
        str(tmp_path),
    )
    attempt = publisher.prepare_partial_sync(
        spec,
        {"boundary": "partial-lost-response"},
        PartialSyncBoundary(
            'ID',
            start_value=2,
            end_value=4,
        ),
        recovery_identity=RECOVERY_IDENTITY,
    )
    assert attempt.method == PUBLICATION_PARTIAL_MERGE
    _create_recorded_native_stage(
        adapter,
        publisher,
        spec,
        attempt,
        row_id=2,
        note="after-2",
    )
    partial_stage = attempt.target.with_table(attempt.staging_table)
    adapter.query(
        f"INSERT INTO {partial_stage.quoted} ({spec.quoted_columns}) "
        "SELECT 4, PARSE_JSON('{\"id\":4}'), 'after-4', "
        "CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL"
    )
    _record_staged(publisher, attempt, spec, 2)
    with pytest.raises(ConnectionError, match="simulated response loss"):
        publisher.publish_partial_sync(attempt, spec)
    assert (
        publisher.load_attempt(
            spec,
            expected_kind="partial",
            recovery_identity=RECOVERY_IDENTITY,
        ).phase
        == PHASE_SUBMITTED
    )

    recovery = SnowflakeIcebergPublisher(adapter, str(tmp_path))
    recovered = recovery.load_attempt(
        spec,
        expected_kind="partial",
        recovery_identity=RECOVERY_IDENTITY,
    )
    assert recovery.reconcile(recovered, spec).action == RECOVERY_PUBLISH
    recovery.publish_partial_sync(recovered, spec)
    assert recovered.phase == PHASE_PUBLISHED
    _assert_copy_on_write_table(adapter, spec.name)
    assert adapter.query(f'SELECT "ID", "NOTE" FROM {spec.name.quoted} ORDER BY "ID"') == [
        {"ID": 1, "NOTE": "before-1"},
        {"ID": 2, "NOTE": "after-2"},
        {"ID": 4, "NOTE": "after-4"},
    ]
    _finish_attempt(adapter, recovery, recovered)


def _replace_incompatible_target(adapter, publisher, spec, schema_name):
    adapter.query(f"DROP ICEBERG TABLE {spec.name.quoted}")
    definitions = ", ".join(
        column.definition + (" COMMENT 'publisher column'" if column.name == "NOTE" else "") for column in spec.columns
    )
    adapter.query(
        f"CREATE ICEBERG TABLE {spec.name.quoted} "
        f"({definitions}{spec.primary_key_clause}) "
        "CATALOG = 'SNOWFLAKE' ICEBERG_VERSION = 3 "
        f"{MANAGED_ICEBERG_V3_TABLE_OPTIONS} COMMENT = 'publisher table'"
    )
    tag = SnowflakeObjectName(spec.name.database, spec.name.schema, "PUBLISHER_TAG")
    adapter.query(f"CREATE TAG {tag.quoted}")
    adapter.query(f"ALTER ICEBERG TABLE {spec.name.quoted} SET TAG {tag.quoted} = 'preserved'")
    adapter.query(f"GRANT SELECT ON TABLE {spec.name.quoted} TO ROLE PUBLIC")
    adapter.query(
        f"INSERT INTO {spec.name.quoted} ({spec.quoted_columns}) "
        "SELECT 2, PARSE_JSON('{\"id\":2}'), 'before replacement', "
        "CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL"
    )
    replacement_spec = IcebergTableSpec.from_fastsync(
        spec.name.database,
        spec.name.schema,
        spec.name.table,
        ['"ID" NUMBER', '"PAYLOAD" VARIANT', '"NOTE" NUMBER'],
        ['"ID"'],
    )
    attempt = publisher.prepare_full_sync(
        replacement_spec,
        {"boundary": "replacement"},
        recovery_identity=RECOVERY_IDENTITY,
    )
    _create_recorded_native_stage(
        adapter,
        publisher,
        replacement_spec,
        attempt,
        row_id=3,
        note=123,
    )
    _record_staged(publisher, attempt, replacement_spec, 1)
    plan = publisher.publish_full_sync(attempt, replacement_spec)
    assert "CREATE OR REPLACE ICEBERG TABLE" in plan.publication_statements[0]
    assert "COPY GRANTS COPY TAGS" in plan.publication_statements[0]
    _assert_copy_on_write_table(adapter, spec.name)
    publisher.restore_metadata(attempt)
    assert adapter.query(f'SELECT "ID", "NOTE" FROM {spec.name.quoted}') == [{"ID": 3, "NOTE": 123}]
    comments = adapter.query(
        "SELECT COLUMN_NAME, COMMENT "
        f'FROM "{spec.name.database}".INFORMATION_SCHEMA.COLUMNS '
        "WHERE TABLE_SCHEMA = %(schema)s AND TABLE_NAME = %(table)s "
        "AND COMMENT IS NOT NULL",
        {"schema": spec.name.schema, "table": spec.name.table},
    )
    assert comments == [{"COLUMN_NAME": "NOTE", "COMMENT": "publisher column"}]
    table_rows = adapter.query(f"SHOW TABLES IN SCHEMA {schema_name} STARTS WITH '{spec.name.table}'")
    exact_table = [row for row in table_rows if row["name"] == spec.name.table]
    assert exact_table[0]["comment"] == "publisher table"
    grants = adapter.query(f"SHOW GRANTS ON TABLE {spec.name.quoted}")
    assert any(
        row["privilege"] == "SELECT"
        and row["granted_to"] == "ROLE"
        and row["grantee_name"] == "PUBLIC"
        for row in grants
    )
    tags = adapter.query(
        "SELECT TAG_NAME, TAG_VALUE "
        f'FROM TABLE("{spec.name.database}".INFORMATION_SCHEMA.'
        "TAG_REFERENCES(%(target)s, 'TABLE'))",
        {"target": spec.name.quoted},
    )
    assert any(row["TAG_NAME"] == tag.table and row["TAG_VALUE"] == "preserved" for row in tags)
    _finish_attempt(adapter, publisher, attempt)


def test_rejects_existing_non_cow_target(tmp_path):
    """Reject an existing managed-v3 target with a non-CoW table setting."""
    connection_config = _connection_config()
    adapter = SnowflakeQueryAdapter(connection_config)
    database = adapter.query("SELECT CURRENT_DATABASE() AS DATABASE_NAME")[0]["DATABASE_NAME"]
    schema = f"PW_CORE_ICEBERG_COW_{uuid4().hex[:12].upper()}"
    target = SnowflakeObjectName(database, schema, "PUBLISHER_COW_REJECTION_TEST")
    schema_name = ".".join((target.quoted.rsplit(".", 2)[0], f'"{schema}"'))
    adapter.query(f"CREATE SCHEMA {schema_name}")

    try:
        spec = IcebergTableSpec.from_fastsync(
            database,
            schema,
            target.table,
            ['"ID" NUMBER', '"PAYLOAD" VARIANT'],
            ['"ID"'],
        )
        adapter.query(
            f"CREATE ICEBERG TABLE {target.quoted} "
            f"({spec.column_definitions}{spec.primary_key_clause}) "
            "CATALOG = 'SNOWFLAKE' ICEBERG_VERSION = 3 "
            "ICEBERG_MERGE_ON_READ_BEHAVIOR = 'AUTO'"
        )
        adapter.query(
            f"INSERT INTO {target.quoted} ({spec.quoted_columns}) "
            "SELECT 1, PARSE_JSON('{\"source\":\"before-rejection\"}'), "
            "CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL"
        )
        parameter_rows = adapter.query(
            "SHOW PARAMETERS LIKE 'ICEBERG_MERGE_ON_READ_BEHAVIOR' "
            f"IN TABLE {target.quoted}"
        )
        assert len(parameter_rows) == 1
        assert parameter_rows[0]["value"] == "AUTO"
        assert parameter_rows[0]["level"] == "TABLE"
        before = adapter.query(
            f'SELECT "ID", GET("PAYLOAD", \'source\')::VARCHAR AS "SOURCE" '
            f"FROM {target.quoted}"
        )

        publisher = SnowflakeIcebergPublisher(adapter, str(tmp_path))
        with pytest.raises(
            TableCompatibilityError,
            match="must set ICEBERG_MERGE_ON_READ_BEHAVIOR",
        ):
            publisher.prepare_full_sync(
                spec,
                {"boundary": "non-copy-on-write-rejection"},
                recovery_identity=RECOVERY_IDENTITY,
            )

        assert adapter.query(
            f'SELECT "ID", GET("PAYLOAD", \'source\')::VARCHAR AS "SOURCE" '
            f"FROM {target.quoted}"
        ) == before
        assert not list(tmp_path.glob("iceberg-recovery-*.json"))
        assert not list(tmp_path.glob("iceberg-fastsync-target-*.json"))
        table_rows = adapter.query(f"SHOW TABLES IN SCHEMA {schema_name}")
        assert not any("_PW_ICEBERG_" in row["name"] for row in table_rows)
    finally:
        adapter.query(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")


def test_live_publication_and_recovery(tmp_path):
    """Exercise publication methods whose atomicity cannot be proved by mocks."""
    connection_config = _connection_config()
    adapter = SnowflakeQueryAdapter(connection_config)
    database = adapter.query("SELECT CURRENT_DATABASE() AS DATABASE_NAME")[0]["DATABASE_NAME"]
    schema = f"PW_CORE_ICEBERG_{uuid4().hex[:12].upper()}"
    target = SnowflakeObjectName(database, schema, "PUBLISHER_TEST")
    schema_name = ".".join((target.quoted.rsplit(".", 2)[0], f'"{schema}"'))
    adapter.query(f"CREATE SCHEMA {schema_name}")

    try:
        publisher = SnowflakeIcebergPublisher(adapter, str(tmp_path))
        ctas_spec = IcebergTableSpec.from_fastsync(
            database,
            schema,
            "PUBLISHER_CTAS_RECOVERY_TEST",
            ['"ID" NUMBER', '"PAYLOAD" VARIANT'],
            ['"ID"'],
        )
        _recover_missing_ctas_response_loss(adapter, tmp_path, ctas_spec)
        spec = IcebergTableSpec.from_fastsync(
            database,
            schema,
            target.table,
            ['"ID" NUMBER', '"PAYLOAD" VARIANT'],
            ['"ID"'],
        )
        _publish_initial(adapter, publisher, spec)
        _publish_empty_overwrite(adapter, publisher, spec)
        extended_spec = _publish_additive_overwrite(adapter, publisher, spec)
        _recover_full_response_loss(adapter, tmp_path, extended_spec)
        _reject_transformed_key_collisions(
            connection_config, tmp_path, extended_spec
        )
        _recover_partial_response_loss(adapter, tmp_path, extended_spec)
        _replace_incompatible_target(adapter, publisher, extended_spec, schema_name)
    finally:
        adapter.query(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
