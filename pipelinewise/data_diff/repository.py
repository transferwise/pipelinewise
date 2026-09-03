"""Data-diff persistence built on the shared backend database module."""

import uuid

from datetime import datetime, timezone
from typing import Iterable, Optional

import psycopg2.extras
import psycopg2.extensions

from pipelinewise.backend_db import BackendDatabase

from .config import CheckDefinition
from .coverage import advance_coverage, calculate_coverage, coverage_event_type

psycopg2.extensions.register_adapter(uuid.UUID, psycopg2.extras.UUID_adapter)


SCHEMA = "public"


class DataDiffRepository:
    """Data-diff persistence using an injected shared backend database."""

    def __init__(self, database: BackendDatabase):
        self.database = database

    @classmethod
    def from_backend_config(cls, config: dict):
        """Create a repository from PipelineWise's shared ``backend_db`` block."""
        return cls(
            BackendDatabase.from_config(
                config,
                application_name="pipelinewise-data-diff",
            )
        )

    def cursor(self):
        """Return an atomic cursor from the shared backend database."""
        return self.database.cursor()

    def ensure_schema(self):
        """Run Alembic migrations via the shared backend database."""
        self.database.migrate()

    # pylint: disable=too-many-locals
    def sync_definitions(
        self,
        definitions: Iterable[CheckDefinition],
        *,
        selected_taps: Optional[Iterable[str]] = None,
        excluded_taps: Optional[Iterable[str]] = None,
    ) -> dict:
        """Reconcile definitions in scope while preserving excluded taps."""
        excluded = set(excluded_taps or [])
        definitions = [
            definition
            for definition in definitions
            if definition.tap_id not in excluded
        ]
        selected = set(selected_taps or ["*"])
        include_all = "*" in selected
        now = datetime.now(timezone.utc)
        stats = {"created": 0, "unchanged": 0, "superseded": 0, "deactivated": 0}
        incoming_keys = {definition.full_check_name for definition in definitions}

        self.ensure_schema()
        with self.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                ("pipelinewise.data_diff.definition_sync",),
            )
            cursor.execute(
                f"""
                SELECT *
                  FROM {SCHEMA}.dd_check_definitions
                 WHERE is_current
                 FOR UPDATE
                """
            )
            current = {row["full_check_name"]: row for row in cursor.fetchall()}

            for definition in definitions:
                active = current.get(definition.full_check_name)
                if active and active["config_hash"] == definition.config_hash:
                    stats["unchanged"] += 1
                    continue

                if active:
                    cursor.execute(
                        f"""
                        UPDATE {SCHEMA}.dd_check_definitions
                           SET is_current = FALSE, superseded_at = %s
                         WHERE check_id = %s
                        """,
                        (now, active["check_id"]),
                    )
                    stats["superseded"] += 1

                cursor.execute(
                    f"""
                    SELECT COALESCE(MAX(revision), 0) + 1 AS revision
                      FROM {SCHEMA}.dd_check_definitions
                     WHERE full_check_name = %s
                    """,
                    (definition.full_check_name,),
                )
                revision = cursor.fetchone()["revision"]
                self._insert_check(cursor, revision, definition, now)
                stats["created"] += 1

            for full_check_name, active in current.items():
                in_scope = (
                    active["tap_id"] not in excluded
                    and (include_all or active["tap_id"] in selected)
                )
                if in_scope and full_check_name not in incoming_keys:
                    cursor.execute(
                        f"""
                        UPDATE {SCHEMA}.dd_check_definitions
                           SET is_current = FALSE, superseded_at = %s
                         WHERE check_id = %s
                        """,
                        (now, active["check_id"]),
                    )
                    stats["deactivated"] += 1

        return stats

    @staticmethod
    def _insert_check(cursor, revision, definition, now):
        cursor.execute(
            f"""
            INSERT INTO {SCHEMA}.dd_check_definitions(
                check_id, full_check_name, revision, config_hash,
                canonical_config, target_id, tap_id,
                source_type, target_type, source_database, target_database,
                source_schema, source_table, target_schema, target_table,
                source_key_column, target_key_column,
                source_timestamp_column, target_timestamp_column, checks,
                frequency, window_start_seconds, window_end_seconds,
                statement_timeout_seconds,
                is_current, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, TRUE, %s
            )
            """,
            (
                uuid.uuid4(), definition.full_check_name, revision,
                definition.config_hash,
                psycopg2.extras.Json(definition.canonical_config),
                definition.target_id,
                definition.tap_id, definition.source_type, definition.target_type,
                definition.source_database, definition.target_database,
                definition.source_schema, definition.source_table,
                definition.target_schema, definition.target_table,
                definition.source_key_column, definition.target_key_column,
                definition.source_timestamp_column,
                definition.target_timestamp_column,
                psycopg2.extras.Json(list(definition.checks)),
                definition.frequency, definition.window_start_seconds,
                definition.window_end_seconds,
                definition.statement_timeout_seconds,
                now,
            ),
        )

    def slot_status(self, check_id, scheduled_for: datetime):
        """Return the latest attempt status for one scheduled slot."""
        with self.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT status
                  FROM {SCHEMA}.dd_run_attempts
                 WHERE check_id = %s
                   AND scheduled_for = %s
                   AND trigger_type != 'REMEDIATION'
                 ORDER BY attempt DESC
                 LIMIT 1
                """,
                (check_id, scheduled_for),
            )
            row = cursor.fetchone()
            return row["status"] if row else None

    def expire_stale_running_attempts(
        self,
        check_id,
        stale_before: datetime,
    ) -> int:
        """Fail every attempt of this check abandoned behind an expired lease.

        Deliberately not scoped to one slot or to scheduled triggers. A RUNNING row
        makes its slot look observed, so the scheduler advances past it; scoping the
        sweep to due slots would therefore never reach the row that caused the skip.
        Remediation attempts are swept for the same reason — nothing else sweeps them.
        """
        recorded_at = datetime.now(timezone.utc)
        with self.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE {SCHEMA}.dd_run_attempts
                   SET status = 'ERROR',
                       finished_at = %s,
                       error = 'Scheduler lease expired before the worker completed'
                 WHERE check_id = %s
                   AND status = 'RUNNING'
                   AND started_at <= %s
                RETURNING run_id
                """,
                (
                    recorded_at,
                    check_id,
                    stale_before,
                ),
            )
            run_ids = [row["run_id"] for row in cursor.fetchall()]
            for run_id in run_ids:
                self._record_watermark_transition(cursor, run_id, recorded_at)
            return len(run_ids)

    def list_checks(
        self,
        *,
        target_id: Optional[str] = None,
        tap_id: Optional[str] = None,
        include_versioned: bool = False,
    ) -> list:
        """List deterministic backend rows for current or historical checks."""
        where = [] if include_versioned else ["checks.is_current"]
        params = []
        if target_id and target_id != "*":
            where.append("checks.target_id = %s")
            params.append(target_id)
        if tap_id and tap_id != "*":
            where.append("checks.tap_id = %s")
            params.append(tap_id)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        with self.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT checks.*,
                       coverage.coverage_start, coverage.verified_through,
                       coverage.max_observed_end, coverage.coverage_status,
                       coverage.blocking_run_id,
                       coverage.updated_at AS verified_at,
                       coverage.evaluated_run_id AS coverage_run_id,
                       coverage.reason AS coverage_reason
                  FROM {SCHEMA}.dd_check_definitions checks
                  LEFT JOIN {SCHEMA}.dd_watermark_state coverage
                    ON coverage.check_id = checks.check_id
                  {where_sql}
                 ORDER BY checks.target_id, checks.tap_id,
                          checks.source_schema, checks.source_table,
                          checks.revision
                """,
                params,
            )
            checks = []
            for raw_row in cursor.fetchall():
                row = dict(raw_row)
                canonical_config = row.get("canonical_config") or {}
                row["source_compare_columns"] = canonical_config.get(
                    "source_compare_columns", []
                )
                row["target_compare_columns"] = canonical_config.get(
                    "target_compare_columns", []
                )
                checks.append(row)
            return checks

    def get_check_version(self, check_id) -> dict:
        """Load one exact definition revision, including inactive history."""
        with self.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT checks.*,
                       coverage.coverage_start, coverage.verified_through,
                       coverage.max_observed_end, coverage.coverage_status,
                       coverage.blocking_run_id,
                       coverage.updated_at AS verified_at,
                       coverage.evaluated_run_id AS coverage_run_id,
                       coverage.reason AS coverage_reason
                  FROM {SCHEMA}.dd_check_definitions checks
                  LEFT JOIN {SCHEMA}.dd_watermark_state coverage
                    ON coverage.check_id = checks.check_id
                 WHERE checks.check_id = %s
                """,
                (check_id,),
            )
            raw_row = cursor.fetchone()
            if raw_row is None:
                raise ValueError(
                    f"Data-diff check '{check_id}' does not exist"
                )
            row = dict(raw_row)
            canonical_config = row.get("canonical_config") or {}
            row["source_compare_columns"] = canonical_config.get(
                "source_compare_columns", []
            )
            row["target_compare_columns"] = canonical_config.get(
                "target_compare_columns", []
            )
            return row

    def latest_scheduled_for(self, check_id):
        """Return the latest scheduled slot observed for one definition revision."""
        with self.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT MAX(scheduled_for) AS scheduled_for
                  FROM {SCHEMA}.dd_run_attempts
                 WHERE check_id = %s
                   AND trigger_type != 'REMEDIATION'
                """,
                (check_id,),
            )
            row = cursor.fetchone()
            return row["scheduled_for"] if row else None

    def get_run(self, run_id) -> dict:
        """Load one run attempt for reporting or exact-window remediation."""
        with self.cursor() as cursor:
            cursor.execute(
                f"SELECT * FROM {SCHEMA}.dd_run_attempts WHERE run_id = %s",
                (run_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"Data-diff run '{run_id}' does not exist")
            return dict(row)

    def start_remediation_run(self, original_run: dict, remediation_reference: str) -> dict:
        """Create a linked attempt for the exact definition and window that failed."""
        if original_run["status"] not in ("FAIL", "ERROR"):
            raise ValueError(
                f"Only failed or errored runs can be remediated; run "
                f"'{original_run['run_id']}' is {original_run['status']}"
            )
        if not remediation_reference:
            raise ValueError("A remediation reference is required")

        with self.cursor() as cursor:
            lock_key = (
                f"{original_run['check_id']}:"
                f"{original_run['scheduled_for'].isoformat()}"
            )
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (lock_key,))
            cursor.execute(
                f"""
                SELECT status, COALESCE(MAX(attempt), 0) AS max_attempt
                  FROM {SCHEMA}.dd_run_attempts
                 WHERE check_id = %s AND scheduled_for = %s
                 GROUP BY status
                """,
                (
                    original_run['check_id'],
                    original_run["scheduled_for"],
                ),
            )
            previous = cursor.fetchall()
            if any(row["status"] == "RUNNING" for row in previous):
                raise ValueError("A data-diff attempt for this window is already running")
            attempt = max((row["max_attempt"] for row in previous), default=0) + 1
            root_run_id = original_run.get("rerun_of_run_id") or original_run["run_id"]
            run_id = uuid.uuid4()
            started_at = datetime.now(timezone.utc)
            cursor.execute(
                f"""
                INSERT INTO {SCHEMA}.dd_run_attempts(
                    run_id, check_id, scheduled_for, window_start,
                    window_end, attempt, trigger_type, status, rerun_of_run_id,
                    remediation_reference, started_at
                ) VALUES (%s, %s, %s, %s, %s, %s, 'REMEDIATION', 'RUNNING', %s, %s, %s)
                """,
                (
                    run_id, original_run['check_id'],
                    original_run["scheduled_for"], original_run["window_start"],
                    original_run["window_end"], attempt, root_run_id,
                    remediation_reference, started_at,
                ),
            )
            return {
                "run_id": run_id,
                "attempt": attempt,
                "trigger_type": "REMEDIATION",
                "rerun_of_run_id": root_run_id,
                "remediation_reference": remediation_reference,
            }

    def start_run(
        self,
        check: dict,
        scheduled_for: datetime,
        window_start: datetime,
        window_end: datetime,
        *,
        force: bool = False,
    ) -> Optional[dict]:
        """Start one idempotent slot, retrying only operational errors by default."""
        with self.cursor() as cursor:
            lock_key = f"{check['check_id']}:{scheduled_for.isoformat()}"
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (lock_key,))
            cursor.execute(
                f"""
                SELECT status, COALESCE(MAX(attempt), 0) AS max_attempt
                  FROM {SCHEMA}.dd_run_attempts
                 WHERE check_id = %s AND scheduled_for = %s
                 GROUP BY status
                """,
                (check["check_id"], scheduled_for),
            )
            previous = cursor.fetchall()
            if any(row["status"] == "RUNNING" for row in previous):
                return None
            if not force and any(row["status"] in ("PASS", "FAIL") for row in previous):
                return None
            attempt = max((row["max_attempt"] for row in previous), default=0) + 1
            trigger_type = "MANUAL" if force else (
                "RETRY" if previous else "SCHEDULED"
            )
            run_id = uuid.uuid4()
            started_at = datetime.now(timezone.utc)
            cursor.execute(
                f"""
                INSERT INTO {SCHEMA}.dd_run_attempts(
                    run_id, check_id, scheduled_for, window_start,
                    window_end, attempt, trigger_type, status, started_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'RUNNING', %s)
                """,
                (
                    run_id, check["check_id"], scheduled_for, window_start, window_end,
                    attempt, trigger_type, started_at,
                ),
            )
            return {
                "run_id": run_id,
                "attempt": attempt,
                "trigger_type": trigger_type,
            }

    def record_preflight(self, check_id, preflight: dict):
        """Persist one immutable source-plan preflight attempt."""
        preflight_id = uuid.uuid4()
        with self.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {SCHEMA}.dd_preflight_log(
                    preflight_id, check_id, status, checked_at,
                    query_fingerprint, index_metadata, findings, error,
                    table_rows, row_limit, has_leading_index
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    preflight_id, check_id, preflight["status"],
                    datetime.now(timezone.utc), preflight["query_fingerprint"],
                    psycopg2.extras.Json(preflight.get("index_metadata", [])),
                    psycopg2.extras.Json(preflight.get("findings", [])),
                    preflight.get("error"),
                    preflight.get("table_rows"),
                    preflight.get("row_limit"),
                    preflight.get("has_leading_index"),
                ),
            )
        return preflight_id

    def finish_run(
        self,
        run_id,
        status: str,
        results: Iterable[dict],
        *,
        preflight_id=None,
        error: Optional[str] = None,
    ):
        """Persist result rows and terminal run state in one transaction."""
        with self.cursor() as cursor:
            for result in results:
                cursor.execute(
                    f"""
                    INSERT INTO {SCHEMA}.dd_run_results(
                        run_id, check_type, status, source_value, target_value,
                        source_query_seconds, target_query_seconds, error
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run_id, result["check_type"], result["status"],
                        psycopg2.extras.Json(result.get("source_value")),
                        psycopg2.extras.Json(result.get("target_value")),
                        result.get("source_query_seconds"),
                        result.get("target_query_seconds"), result.get("error"),
                    ),
                )
            finished_at = datetime.now(timezone.utc)
            cursor.execute(
                f"""
                UPDATE {SCHEMA}.dd_run_attempts
                   SET status = %s, preflight_id = %s, finished_at = %s, error = %s
                 WHERE run_id = %s
                """,
                (status, preflight_id, finished_at, error, run_id),
            )
            self._record_watermark_transition(cursor, run_id, finished_at)

    @staticmethod
    def _record_watermark_transition(cursor, run_id, recorded_at):
        cursor.execute(
            f"""
            SELECT runs.run_id, runs.check_id AS check_id,
                   runs.scheduled_for, runs.window_start, runs.window_end,
                   runs.attempt, runs.status, checks.checks
              FROM {SCHEMA}.dd_run_attempts runs
              JOIN {SCHEMA}.dd_check_definitions checks
                ON checks.check_id = runs.check_id
             WHERE runs.run_id = %s
            """,
            (run_id,),
        )
        definition = cursor.fetchone()
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"pipelinewise.data_diff.coverage:{definition['check_id']}",),
        )

        previous = DataDiffRepository._watermark_state_for_update(
            cursor, definition["check_id"]
        )
        existing_slot = DataDiffRepository._run_slot_state_for_update(
            cursor, definition["check_id"], definition["scheduled_for"]
        )
        has_later_slot = DataDiffRepository._has_later_run_slot(
            cursor, definition["check_id"], definition["scheduled_for"]
        )
        slot_state_changed = DataDiffRepository._upsert_run_slot_state(
            cursor, definition
        )

        data_checks_enabled = bool(set(definition["checks"]) - {"schema_compatibility"})
        if (
            slot_state_changed
            and previous
            and existing_slot is None
            and not has_later_slot
        ):
            coverage = advance_coverage(
                previous,
                definition,
                data_checks_enabled=data_checks_enabled,
            )
        elif not slot_state_changed and previous:
            coverage = {
                key: previous[key]
                for key in (
                    "coverage_start", "verified_through", "max_observed_end",
                    "coverage_status", "blocking_run_id", "reason",
                )
            }
        else:
            coverage = DataDiffRepository._recalculate_watermark(
                cursor,
                definition["check_id"],
                data_checks_enabled=data_checks_enabled,
            )

        event_type = coverage_event_type(previous, coverage)
        state_version = int(previous["state_version"]) + 1 if previous else 1
        DataDiffRepository._upsert_watermark_state(
            cursor,
            definition,
            coverage,
            event_type,
            state_version,
            recorded_at,
        )
        DataDiffRepository._insert_watermark_event(
            cursor,
            definition,
            previous,
            coverage,
            event_type,
            recorded_at,
        )

    @staticmethod
    def _watermark_state_for_update(cursor, check_id):
        cursor.execute(
            f"""
            SELECT *
              FROM {SCHEMA}.dd_watermark_state
             WHERE check_id = %s
             FOR UPDATE
            """,
            (check_id,),
        )
        return cursor.fetchone()

    @staticmethod
    def _run_slot_state_for_update(cursor, check_id, scheduled_for):
        cursor.execute(
            f"""
            SELECT run_id, attempt
              FROM {SCHEMA}.dd_run_slot_state
             WHERE check_id = %s AND scheduled_for = %s
             FOR UPDATE
            """,
            (check_id, scheduled_for),
        )
        return cursor.fetchone()

    @staticmethod
    def _has_later_run_slot(cursor, check_id, scheduled_for):
        cursor.execute(
            f"""
            SELECT EXISTS(
                SELECT 1
                  FROM {SCHEMA}.dd_run_slot_state
                 WHERE check_id = %s AND scheduled_for > %s
            ) AS has_later_slot
            """,
            (check_id, scheduled_for),
        )
        return cursor.fetchone()["has_later_slot"]

    @staticmethod
    def _upsert_run_slot_state(cursor, definition):
        cursor.execute(
            f"""
            INSERT INTO {SCHEMA}.dd_run_slot_state(
                check_id, scheduled_for, run_id, attempt,
                window_start, window_end, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (check_id, scheduled_for) DO UPDATE
               SET run_id = EXCLUDED.run_id,
                   attempt = EXCLUDED.attempt,
                   window_start = EXCLUDED.window_start,
                   window_end = EXCLUDED.window_end,
                   status = EXCLUDED.status
             WHERE EXCLUDED.attempt > {SCHEMA}.dd_run_slot_state.attempt
            RETURNING run_id
            """,
            (
                definition["check_id"], definition["scheduled_for"],
                definition["run_id"], definition["attempt"],
                definition["window_start"], definition["window_end"],
                definition["status"],
            ),
        )
        return cursor.fetchone() is not None

    @staticmethod
    def _recalculate_watermark(cursor, check_id, *, data_checks_enabled):
        cursor.execute(
            f"""
            SELECT run_id, scheduled_for, window_start, window_end, attempt, status
              FROM {SCHEMA}.dd_run_slot_state
             WHERE check_id = %s
             ORDER BY scheduled_for
            """,
            (check_id,),
        )
        return calculate_coverage(
            [dict(row) for row in cursor.fetchall()],
            data_checks_enabled=data_checks_enabled,
        )

    @staticmethod
    def _upsert_watermark_state(
        cursor,
        definition,
        coverage,
        event_type,
        state_version,
        recorded_at,
    ):
        cursor.execute(
            f"""
            INSERT INTO {SCHEMA}.dd_watermark_state(
                check_id, coverage_start, verified_through, max_observed_end,
                coverage_status, blocking_run_id, evaluated_run_id, event_type,
                state_version, updated_at, reason
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (check_id) DO UPDATE
               SET coverage_start = EXCLUDED.coverage_start,
                   verified_through = EXCLUDED.verified_through,
                   max_observed_end = EXCLUDED.max_observed_end,
                   coverage_status = EXCLUDED.coverage_status,
                   blocking_run_id = EXCLUDED.blocking_run_id,
                   evaluated_run_id = EXCLUDED.evaluated_run_id,
                   event_type = EXCLUDED.event_type,
                   state_version = EXCLUDED.state_version,
                   updated_at = EXCLUDED.updated_at,
                   reason = EXCLUDED.reason
            """,
            (
                definition["check_id"], coverage["coverage_start"],
                coverage["verified_through"], coverage["max_observed_end"],
                coverage["coverage_status"], coverage["blocking_run_id"],
                definition["run_id"], event_type, state_version, recorded_at,
                coverage["reason"],
            ),
        )

    @staticmethod
    def _insert_watermark_event(
        cursor,
        definition,
        previous,
        coverage,
        event_type,
        recorded_at,
    ):
        cursor.execute(
            f"""
            INSERT INTO {SCHEMA}.dd_watermark_events(
                watermark_event_id, check_id, evaluated_run_id, event_type,
                coverage_start, previous_verified_through, verified_through,
                max_observed_end, coverage_status, blocking_run_id,
                recorded_at, reason
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                uuid.uuid4(), definition["check_id"], definition["run_id"],
                event_type, coverage["coverage_start"],
                previous["verified_through"] if previous else None,
                coverage["verified_through"], coverage["max_observed_end"],
                coverage["coverage_status"], coverage["blocking_run_id"],
                recorded_at, coverage["reason"],
            ),
        )

    def __enter__(self):
        self.database.connect()
        return self

    def __exit__(self, *_args):
        self.database.close()
