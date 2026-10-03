import base64
import binascii
import datetime
import decimal
import glob
import json
import logging
import os
import re
import psycopg2
import psycopg2.extras

from argparse import Namespace
from psycopg2 import sql
from time import monotonic, sleep
from typing import Callable, Dict, Optional


from . import utils, split_gzip
from .partial_sync_boundary import PartialSyncBoundary
from .source_transformations import compile_source_select, quote_source_identifier, validate_bookmark_column
from ...utils import safe_column_name

LOGGER = logging.getLogger(__name__)
MIN_SUPPORTED_POSTGRES_VERSION = 140000
MIN_SAFE_POSTGRES_VERSIONS = {14: 140018, 15: 150013, 16: 160009, 17: 170005}
PGOUTPUT_PLUGIN = 'pgoutput'
WAL2JSON_PLUGIN = 'wal2json'
MAX_REPLICATION_SLOT_NAME_LENGTH = 63
MAX_POSTGRES_TAP_ID_LENGTH = 50
POSTGRES_TAP_ID_PATTERN = re.compile(r'^[a-z0-9_]+$')
PGOUTPUT_MIGRATION_STATE_KEY = '_pipelinewise_pgoutput_migration'
PGOUTPUT_MIGRATION_STATE_VERSION = 2
PUBLICATION_FENCE_COMMENT_PREFIX = 'pipelinewise-publication-fence-v1:'
REPLICA_REPLAY_TIMEOUT_SECONDS = 300
SLOT_RELEASE_TIMEOUT_SECONDS = 30


class UnsupportedPostgresVersionError(RuntimeError):
    """The source server is older than the supported PostgreSQL version floor."""


class FastSyncTapPostgres:
    """
    Common functions for fastsync from a Postgres database
    """

    def __init__(self, connection_config, tap_type_to_target_type, target_quote=None):
        self.connection_config = connection_config
        self.tap_type_to_target_type = tap_type_to_target_type
        self.target_quote = target_quote
        self.source_transformations = None
        self.target_iceberg_version = None
        self.hstore_as_json = False
        self.conn = None
        self.curr = None
        self.primary_host_conn = None

    @staticmethod
    def generate_replication_slot_name(dbname, tap_id=None, prefix='pipelinewise'):
        """Generate replication slot name with

        :param str dbname: Database name that will be part of the replication slot name
        :param str tap_id: Optional. If provided then it will be appended to the end of the slot name
        :param str prefix: Optional. Defaults to 'pipelinewise'
        :return: well formatted lowercased replication slot name
        :rtype: str
        """
        # Add tap_id to the end of the slot name if provided
        if tap_id:
            tap_id = f'_{tap_id}'
        # Convert None to empty string
        else:
            tap_id = ''

        slot_name = f'{prefix}_{dbname}{tap_id}'.lower()

        # Replace invalid characters to ensure replication slot name is in accordance with Postgres spec
        return re.sub('[^a-z0-9_]', '_', slot_name)[:MAX_REPLICATION_SLOT_NAME_LENGTH]

    @staticmethod
    def _replication_slot_name_is_truncated(dbname, tap_id, prefix='pipelinewise'):
        """Return whether PostgreSQL truncates this normalized historical name."""
        slot_name = re.sub('[^a-z0-9_]', '_', f'{prefix}_{dbname}_{tap_id}'.lower())
        return len(slot_name) > MAX_REPLICATION_SLOT_NAME_LENGTH

    @classmethod
    def _implicit_truncated_historical_slot_present(
        cls, dbname, tap_id, previous_tap_id, legacy, current, slots
    ):
        """Return whether an existing inferred old slot has ambiguous ownership."""
        return (
            previous_tap_id is None
            and current != legacy
            and current in slots
            and cls._replication_slot_name_is_truncated(dbname, tap_id)
        )

    @classmethod
    def _reject_implicit_truncated_historical_slot(
        cls, dbname, tap_id, previous_tap_id, legacy, current, slots
    ):
        """Require explicit ownership before claiming a non-injective old slot name."""
        if cls._implicit_truncated_historical_slot_present(
            dbname, tap_id, previous_tap_id, legacy, current, slots
        ):
            raise RuntimeError(
                f'Historical tap-specific PostgreSQL slot "{current}" has an implicitly '
                'truncated name that may belong to another tap. Verify ownership and set '
                'previous_tap_id explicitly before migrating, advancing, or removing it. '
                'No source changes were made.'
            )

    @classmethod
    def generate_canonical_replication_name(cls, tap_id: str) -> str:
        """Return the shared canonical name for one tap's slot and publication."""
        cls.validate_postgres_tap_id(tap_id)
        return f'ppw_slot_{tap_id}'

    @classmethod
    def _replication_slot_names(cls, dbname: str, tap_id: str, previous_tap_id: Optional[str] = None):
        """Return the pgoutput destination and wal2json candidates in migration order."""
        if not isinstance(tap_id, str) or not tap_id:
            raise RuntimeError('The pgoutput replication slot requires a non-empty tap ID.')
        # Removed legacy configs may contain IDs that predate canonical validation.
        # Keep the raw candidate so cleanup can report every ambiguous source object without mutating it.
        destination = f'ppw_slot_{tap_id}'
        legacy = cls.generate_replication_slot_name(dbname)
        if previous_tap_id is not None and (not isinstance(previous_tap_id, str) or not previous_tap_id):
            raise RuntimeError('previous_tap_id must be a non-empty historical tap ID.')
        current = cls.generate_replication_slot_name(dbname, previous_tap_id or tap_id)
        return destination, legacy, current

    @classmethod
    def validate_postgres_tap_id(cls, tap_id: str) -> None:
        """Require an injective tap ID that fits the canonical slot name."""
        if (
            not isinstance(tap_id, str)
            or not POSTGRES_TAP_ID_PATTERN.fullmatch(tap_id)
            or len(tap_id) > MAX_POSTGRES_TAP_ID_LENGTH
        ):
            raise RuntimeError(
                'PostgreSQL tap IDs must contain only lowercase ASCII letters, digits, and underscores, '
                f'and be at most {MAX_POSTGRES_TAP_ID_LENGTH} characters. '
                'No source or state changes were made.'
            )

    @classmethod
    def validate_replication_slot_identity(cls, dbname: str, tap_id: str, previous_tap_id: Optional[str] = None):
        """Validate and return the canonical and historical slot identities."""
        cls.validate_postgres_tap_id(tap_id)
        return cls._replication_slot_names(dbname, tap_id, previous_tap_id)

    @staticmethod
    def _fetch_replication_slots(cursor, slot_names):
        cursor.execute(
            'SELECT slot_name, database, plugin, active FROM pg_replication_slots '
            'WHERE slot_name IN (%s, %s, %s)',
            slot_names,
        )
        return {row[0]: row for row in cursor.fetchall()}

    @classmethod
    def _historical_migration_source(cls, slots, legacy, current, *, fresh_start=False):
        """Return the tap-owned historical slot without claiming a shared slot."""
        if current in slots and current != legacy:
            return current
        if legacy in slots and not fresh_start:
            raise RuntimeError(
                f'Historical database-wide PostgreSQL slot "{legacy}" may be shared by multiple taps '
                'and cannot be migrated automatically. A DBA must migrate it to the dedicated '
                f'tap-specific slot "{current}" before pgoutput migration. '
                'No source changes were made.'
            )
        return None

    @staticmethod
    def _validate_replication_slot(slot, database, expected_plugins, *, require_inactive):
        slot_name, slot_database, plugin, active = slot
        if slot_database != database or plugin not in expected_plugins or (require_inactive and active):
            expected = ' or '.join(sorted(expected_plugins))
            inactive = ' and be inactive' if require_inactive else ''
            raise RuntimeError(
                f'PostgreSQL slot "{slot_name}" must belong to database "{database}", '
                f'use {expected}{inactive}. No source changes were made.'
            )

    @staticmethod
    def _decode_managed_publication_comment(comment):
        """Return validated PipelineWise publication metadata, if present."""
        if not isinstance(comment, str) or not comment.startswith(
                PUBLICATION_FENCE_COMMENT_PREFIX):
            return None
        try:
            encoded = comment.removeprefix(PUBLICATION_FENCE_COMMENT_PREFIX)
            decoded = base64.b64decode(encoded.encode(), altchars=b'-_', validate=True)
            payload = json.loads(decoded.decode())
        except (binascii.Error, json.JSONDecodeError, TypeError, UnicodeError, ValueError):
            return None
        expected_keys = {'state', 'original_comment'}
        if isinstance(payload, dict) and 'managed_tables' in payload:
            expected_keys.add('managed_tables')
        if not (
            isinstance(payload, dict)
            and set(payload) == expected_keys
            and payload['state'] in {'pending', 'ready'}
            and (
                payload['original_comment'] is None
                or isinstance(payload['original_comment'], str)
            )
            and (
                'managed_tables' not in payload
                or (
                    isinstance(payload['managed_tables'], list)
                    and all(
                        isinstance(table, list)
                        and len(table) == 2
                        and all(isinstance(part, str) and part for part in table)
                        for table in payload['managed_tables']
                    )
                )
            )
        ):
            return None
        return payload

    @classmethod
    def _is_managed_publication_comment(cls, comment):
        """Return whether a publication comment proves PipelineWise ownership."""
        return cls._decode_managed_publication_comment(comment) is not None

    @classmethod
    def _accept_concurrent_pgoutput_slot(cls, cursor, slot_names, database, error):
        """Accept a concurrent creator only after validating the resulting destination."""
        if getattr(error, 'pgcode', None) != '42710':
            raise error
        cls._wait_for_inactive_slots(cursor, database, {slot_names[0]: PGOUTPUT_PLUGIN}, require_ready=True)

    @classmethod
    def _wait_for_inactive_slots(cls, cursor, database, expected_plugins, *, require_ready=False, allow_missing=False):
        """Wait for server-side slot cleanup without accepting unfinished creation."""
        deadline = monotonic() + SLOT_RELEASE_TIMEOUT_SECONDS
        while True:
            cursor.execute(
                'SELECT slot_name, database, plugin, active, confirmed_flush_lsn::text '
                'FROM pg_replication_slots WHERE slot_name = ANY(%s)',
                (list(expected_plugins),),
            )
            slots = {row[0]: row for row in cursor.fetchall()}
            missing = set(expected_plugins) - set(slots)
            if missing and not allow_missing:
                raise RuntimeError(f'Cannot find PostgreSQL replication slot(s): {sorted(missing)}.')
            for name, slot in slots.items():
                cls._validate_replication_slot(slot[:4], database, {expected_plugins[name]}, require_inactive=False)
            if all(not row[3] and (not require_ready or row[4] is not None) for row in slots.values()):
                return slots
            if monotonic() >= deadline:
                raise RuntimeError('Timed out waiting for PostgreSQL replication slots to be inactive and initialized.')
            sleep(0.1)

    @classmethod
    def validate_migration_state_marker(cls, connection_config: Dict, marker: Dict):
        """Validate a versioned migration marker against tap identity."""
        common_required = {'version', 'phase', 'source_slot', 'destination_slot', 'slot_lsn'}
        phase = marker.get('phase') if isinstance(marker, dict) else None
        bridge_pending = phase == 'bridge_pending'
        if (
            not isinstance(marker, dict)
            or not common_required.issubset(marker)
            or type(marker.get('version')) is not int
            or marker['version'] != PGOUTPUT_MIGRATION_STATE_VERSION
            or phase not in {
                'bridge_pending', 'bridge', 'pgoutput_overlap', 'overlap_complete'
            }
            or not isinstance(marker.get('source_slot'), str)
            or not isinstance(marker.get('destination_slot'), str)
            or type(marker.get('slot_lsn')) is not int
            or marker['slot_lsn'] < 0
            or type(marker.get('boundary_lsn')) is not int
            or marker['boundary_lsn'] < marker['slot_lsn']
            or (
                not bridge_pending
                and (
                    type(marker.get('bridge_lsn')) is not int
                    or marker['bridge_lsn'] <= marker['slot_lsn']
                )
            )
            or (
                phase == 'overlap_complete'
                and (
                    type(marker.get('crossover_lsn')) is not int
                    or marker['crossover_lsn'] < marker['slot_lsn']
                )
            )
        ):
            raise RuntimeError(
                f'Invalid {PGOUTPUT_MIGRATION_STATE_KEY} state marker. '
                'No source or state changes were made.'
            )

        database = connection_config['dbname']
        previous_tap_id = connection_config.get('previous_tap_id')
        destination, legacy, current = cls.validate_replication_slot_identity(
            database, connection_config['tap_id'], previous_tap_id
        )
        source = marker['source_slot']
        if marker['destination_slot'] != destination or source != current:
            raise RuntimeError(
                f'{PGOUTPUT_MIGRATION_STATE_KEY} does not match the configured PostgreSQL tap. '
                'No source or state changes were made.'
            )
        cls._reject_implicit_truncated_historical_slot(
            database, connection_config['tap_id'], previous_tap_id,
            legacy, current, {source: None},
        )
        return phase, destination, source

    @classmethod
    def migration_slots_coexist(cls, connection_config: Dict) -> bool:
        """Return whether automatic migration currently owns both dedicated slots."""
        database = connection_config['dbname']
        previous_tap_id = connection_config.get('previous_tap_id')
        destination, legacy, source = cls.validate_replication_slot_identity(
            database,
            connection_config['tap_id'],
            previous_tap_id,
        )
        if source == legacy:
            return False

        connection = cls.get_connection(connection_config, prioritize_primary=True)
        try:
            with connection.cursor() as cur:
                slots = cls._fetch_replication_slots(
                    cur, (destination, source, legacy)
                )
                if cls._implicit_truncated_historical_slot_present(
                    database, connection_config['tap_id'], previous_tap_id,
                    legacy, source, slots,
                ):
                    LOGGER.warning(
                        'Ignoring implicitly truncated historical PostgreSQL slot "%s" '
                        'when checking migration ownership',
                        source,
                    )
                    return False
                if destination not in slots or source not in slots:
                    return False
                cls._validate_replication_slot(
                    slots[destination], database, {PGOUTPUT_PLUGIN}, require_inactive=False
                )
                cls._validate_replication_slot(
                    slots[source], database, {WAL2JSON_PLUGIN}, require_inactive=False
                )
                return True
        finally:
            connection.close()

    @staticmethod
    def _lsn_to_int(lsn: str) -> int:
        try:
            high, low = lsn.split('/')
            return (int(high, 16) << 32) + int(low, 16)
        except (AttributeError, TypeError, ValueError) as exc:
            raise RuntimeError(f'Invalid PostgreSQL replication-slot LSN: {lsn!r}') from exc

    @staticmethod
    def _int_to_lsn(lsn: int) -> str:
        return f'{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}'

    @classmethod
    def _advance_replication_slots(
        cls,
        connection_config: Dict,
        expected_plugins: Dict[str, str],
        durable_lsn: int,
    ) -> None:
        """Idempotently advance inactive validated slots to a target-durable boundary."""
        if type(durable_lsn) is not int or durable_lsn < 0:
            raise RuntimeError(
                f'Invalid target-durable PostgreSQL LSN: {durable_lsn!r}. '
                'No source changes were made.'
            )
        database = connection_config['dbname']
        slot_names = list(expected_plugins)
        connection = cls.get_connection(connection_config, prioritize_primary=True)
        try:
            with connection.cursor() as cur:
                slots = cls._wait_for_inactive_slots(cur, database, expected_plugins, require_ready=True)
                for slot_name in slot_names:
                    confirmed_lsn = cls._lsn_to_int(slots[slot_name][4])
                    if confirmed_lsn >= durable_lsn:
                        continue
                    cur.execute(
                        'SELECT slot_name, end_lsn::text '
                        'FROM pg_replication_slot_advance(%s, %s::pg_lsn)',
                        (slot_name, cls._int_to_lsn(durable_lsn)),
                    )
                    result = cur.fetchone()
                    if (
                        not result
                        or result[0] != slot_name
                        or cls._lsn_to_int(result[1]) < durable_lsn
                    ):
                        raise RuntimeError(
                            f'PostgreSQL did not advance slot "{slot_name}" to the '
                            'target-durable boundary. State was retained.'
                        )
        finally:
            connection.close()

    @classmethod
    def advance_canonical_replication_slot(
        cls, connection_config: Dict, durable_lsn: int
    ) -> None:
        """Release canonical pgoutput WAL through the minimum target-durable bookmark."""
        destination, _, _ = cls.validate_replication_slot_identity(
            connection_config['dbname'], connection_config['tap_id'], connection_config.get('previous_tap_id')
        )
        cls._advance_replication_slots(
            connection_config, {destination: PGOUTPUT_PLUGIN}, durable_lsn
        )

    @classmethod
    def promote_migrated_replication_slot(cls, connection_config: Dict, marker: Dict) -> Dict:
        """Validate the untouched pgoutput slot and prepare crash-safe overlap replay."""
        phase, destination, source = cls.validate_migration_state_marker(
            connection_config, marker
        )
        if phase != 'bridge':
            raise RuntimeError(
                f'Cannot promote a PostgreSQL migration in phase "{phase}". '
                'No source or state changes were made.'
            )

        database = connection_config['dbname']
        connection = cls.get_connection(connection_config, prioritize_primary=True)
        try:
            with connection.cursor() as cur:
                slots = cls._wait_for_inactive_slots(
                    cur,
                    database,
                    {destination: PGOUTPUT_PLUGIN, source: WAL2JSON_PLUGIN},
                    require_ready=True,
                )
                destination_lsn = cls._lsn_to_int(slots[destination][4])
                if destination_lsn != marker['slot_lsn']:
                    raise RuntimeError(
                        f'Canonical pgoutput slot "{destination}" moved from its original '
                        'migration LSN. PostgreSQL cannot replay discarded WAL; run an '
                        'unfiltered whole-tap FastSync.'
                    )
        finally:
            connection.close()

        updated = dict(marker)
        updated['phase'] = 'pgoutput_overlap'
        updated.pop('crossover_lsn', None)
        return updated

    @classmethod
    def drop_promoted_wal2json_slot(cls, connection_config: Dict, marker: Dict) -> None:
        """Idempotently drop wal2json after the promoted state has been persisted."""
        phase, destination, source = cls.validate_migration_state_marker(
            connection_config, marker
        )
        if phase not in {'pgoutput_overlap', 'overlap_complete'}:
            raise RuntimeError(
                f'Cannot drop wal2json for a PostgreSQL migration in phase "{phase}". '
                'No source or state changes were made.'
            )

        database = connection_config['dbname']
        connection = cls.get_connection(connection_config, prioritize_primary=True)
        try:
            with connection.cursor() as cur:
                slots = cls._wait_for_inactive_slots(
                    cur,
                    database,
                    {destination: PGOUTPUT_PLUGIN, source: WAL2JSON_PLUGIN},
                    require_ready=True,
                    allow_missing=True,
                )
                if destination not in slots:
                    raise RuntimeError(
                        f'Cannot retire wal2json slot "{source}" without canonical pgoutput slot '
                        f'"{destination}". No source or state changes were made.'
                    )
                cls._validate_replication_slot(
                    slots[destination][:4], database, {PGOUTPUT_PLUGIN}, require_inactive=True
                )
                destination_lsn = cls._lsn_to_int(slots[destination][4])
                if destination_lsn < marker['slot_lsn'] or (
                    (phase == 'pgoutput_overlap' or source in slots)
                    and destination_lsn != marker['slot_lsn']
                ):
                    raise RuntimeError(
                        f'Canonical pgoutput slot "{destination}" no longer has the '
                        'promoted migration position. The wal2json slot was retained.'
                    )
                if source not in slots:
                    return
                cls._validate_replication_slot(
                    slots[source][:4], database, {WAL2JSON_PLUGIN}, require_inactive=True
                )
                LOGGER.info('Dropping promoted wal2json slot "%s"', source)
                cur.execute('SELECT pg_drop_replication_slot(%s)', (source,))
        finally:
            connection.close()

    @classmethod
    def retire_logical_slots(cls, connection_config: Dict, *, before_drop: Callable[[], None]) -> None:
        """Retire tap-owned slots after the managed publication becomes empty."""
        database = connection_config['dbname']
        previous_tap_id = connection_config.get('previous_tap_id')
        destination, legacy, current = cls.validate_replication_slot_identity(
            database,
            connection_config['tap_id'],
            previous_tap_id,
        )
        connection = cls.get_connection(connection_config, prioritize_primary=True)
        try:
            with connection.cursor() as cur:
                slots = cls._fetch_replication_slots(
                    cur, (destination, current, legacy)
                )
                preserve_current = cls._implicit_truncated_historical_slot_present(
                    database, connection_config['tap_id'], previous_tap_id,
                    legacy, current, slots,
                )
                if preserve_current:
                    LOGGER.warning(
                        'Preserving implicitly truncated historical PostgreSQL slot "%s" '
                        'during final LOG_BASED retirement',
                        current,
                    )
                owned_slots = {}
                if destination in slots:
                    owned_slots[destination] = PGOUTPUT_PLUGIN
                if current != legacy and current in slots and not preserve_current:
                    owned_slots[current] = WAL2JSON_PLUGIN

                for slot_name, plugin in owned_slots.items():
                    cls._validate_replication_slot(
                        slots[slot_name],
                        database,
                        {plugin},
                        require_inactive=True,
                    )

                if not owned_slots:
                    # There is no tap-owned source object to protect. Clear stale
                    # local history without inspecting or claiming a same-name
                    # publication that may belong to a DBA.
                    before_drop()
                    return

                cur.execute(
                    "SELECT pg_catalog.obj_description(oid, 'pg_publication') "
                    'FROM pg_catalog.pg_publication WHERE pubname = %s',
                    (destination,),
                )
                publication = cur.fetchone()
                if publication is not None:
                    metadata = cls._decode_managed_publication_comment(publication[0])
                    if (
                        metadata is None
                        or metadata.get('state') != 'ready'
                        or metadata.get('managed_tables') != []
                    ):
                        raise RuntimeError(
                            f'PostgreSQL publication "{destination}" must contain ready '
                            'PipelineWise managed metadata with no managed tables before '
                            'logical slots can be retired. No source or state changes were made.'
                        )

                # The local state must stop advertising reusable LOG_BASED bookmarks
                # before PostgreSQL forgets the corresponding WAL history.
                before_drop()

                for slot_name in dict.fromkeys((destination, current)):
                    if slot_name not in owned_slots:
                        continue
                    LOGGER.info(
                        'Dropping retired PostgreSQL logical replication slot "%s"',
                        slot_name,
                    )
                    cur.execute('SELECT pg_drop_replication_slot(%s)', (slot_name,))

                if legacy in slots and legacy not in owned_slots:
                    LOGGER.warning(
                        'Leaving potentially shared database-wide PostgreSQL slot "%s" unchanged',
                        legacy,
                    )
        finally:
            connection.close()

    @classmethod
    def drop_slot(
        cls,
        connection_config: Dict,
        *,
        allow_unsupported_version_for_config_removal: bool = False,
    ) -> None:
        """
        Dropping the logical replication slot from primary server

        Args:
            connection_config: Dictionary with db credentials
            allow_unsupported_version_for_config_removal: Permit cleanup of a
                removed tap whose source predates the supported version floor.
        """
        LOGGER.info('Attempting to drop slot ...')

        database = connection_config['dbname']
        tap_id = connection_config['tap_id']
        destination, legacy, current = cls._replication_slot_names(
            database, tap_id, connection_config.get('previous_tap_id')
        )
        has_canonical_tap_id = (
            isinstance(tap_id, str)
            and POSTGRES_TAP_ID_PATTERN.fullmatch(tap_id)
            and len(tap_id) <= MAX_POSTGRES_TAP_ID_LENGTH
        )
        if not has_canonical_tap_id:
            LOGGER.warning(
                'Skipping automatic PostgreSQL source cleanup for legacy tap ID %r. '
                'Leaving canonical slot/publication candidate "%s" unchanged because '
                'the tap ID does not satisfy the canonical naming rules.',
                tap_id,
                destination,
            )
            LOGGER.warning(
                'Leaving normalized historical wal2json slot candidates %s unchanged. '
                'Their non-injective names may belong to another tap; complete manual ownership review.',
                list(dict.fromkeys((current, legacy))),
            )
            return

        LOGGER.debug('Creating a connection to Primary server ..')
        connection = cls.get_connection(
            connection_config,
            prioritize_primary=True,
            allow_unsupported_version_for_config_removal=(
                allow_unsupported_version_for_config_removal
            ),
        )
        LOGGER.debug('Connection to Primary server created.')

        try:
            slot_names = (destination, current, legacy)
            publication_name = destination
            expected_plugins = {}
            for slot_name, plugin in (
                (destination, PGOUTPUT_PLUGIN),
                (current, WAL2JSON_PLUGIN),
            ):
                expected_plugins.setdefault(slot_name, set()).add(plugin)

            with connection.cursor() as cur:
                slots = cls._fetch_replication_slots(cur, slot_names)
                preserve_current = cls._implicit_truncated_historical_slot_present(
                    database, tap_id, connection_config.get('previous_tap_id'),
                    legacy, current, slots,
                )
                if preserve_current:
                    LOGGER.warning(
                        'Preserving implicitly truncated historical PostgreSQL slot "%s" '
                        'during deleted-tap cleanup',
                        current,
                    )
                publication = None
                managed_slots = {
                    name: slot
                    for name, slot in slots.items()
                    if name in {destination, current}
                    and not (name == current and preserve_current)
                    and not (name == legacy and slot[2] == WAL2JSON_PLUGIN)
                }
                if legacy in slots and legacy not in managed_slots:
                    LOGGER.warning(
                        'Leaving potentially shared database-wide PostgreSQL slot "%s" unchanged',
                        legacy,
                    )
                for slot in managed_slots.values():
                    cls._validate_replication_slot(
                        slot,
                        database,
                        expected_plugins[slot[0]],
                        require_inactive=True,
                    )
                if publication_name:
                    cur.execute(
                        'SELECT publication.pubname, owner.rolname, actor.rolsuper, current_user, '
                        "pg_catalog.obj_description(publication.oid, 'pg_publication') "
                        'FROM pg_catalog.pg_publication AS publication '
                        'JOIN pg_catalog.pg_roles AS owner ON owner.oid = publication.pubowner '
                        'JOIN pg_catalog.pg_roles AS actor ON actor.rolname = current_user '
                        'WHERE publication.pubname = %s',
                        (publication_name,),
                    )
                    publication = cur.fetchone()
                    if publication and publication[1] != publication[3] and not publication[2]:
                        raise RuntimeError(
                            f'PostgreSQL publication "{publication_name}" is owned by '
                            f'"{publication[1]}" and cannot be removed by "{publication[3]}". '
                            'No source changes were made.'
                        )
                    if publication and not cls._is_managed_publication_comment(publication[4]):
                        raise RuntimeError(
                            f'PostgreSQL publication "{publication_name}" does not contain valid '
                            'PipelineWise managed metadata. Preserve it and complete manual review '
                            'before removal. No source changes were made.'
                        )
                dropped = 0
                for slot_name in dict.fromkeys((destination, current)):
                    if slot_name not in managed_slots:
                        continue
                    LOGGER.info('Dropping the slot "%s"', slot_name)
                    cur.execute('SELECT pg_drop_replication_slot(%s)', (slot_name,))
                    dropped += 1
                if publication:
                    LOGGER.info('Dropping the publication "%s"', publication_name)
                    cur.execute(
                        sql.SQL('DROP PUBLICATION {}').format(
                            sql.Identifier(publication_name)
                        )
                    )
                LOGGER.info('Number of dropped slots: %s', dropped)

        finally:
            connection.close()

    @classmethod
    def reset_slot(cls, connection_config: Dict, *, before_reset: Callable[..., Optional[str]]) -> Dict:
        """Invalidate state, remove owned old slots, then create a fresh pgoutput slot."""
        LOGGER.info('Attempting to reset slot ...')

        connection = cls.get_connection(connection_config, prioritize_primary=True)
        try:
            with connection.cursor() as cur:
                slot_name, slot_exists, source_name = cls._preflight_slot_reset(cur, connection_config)
                # State must be durable before changing the slot boundary; a lost
                # response cannot prove whether the source mutation completed.
                backup_path = before_reset(fresh_start_marker={
                    'version': 1, 'wal2json_slot': source_name, 'destination_slot': slot_name,
                })
                phase = 'drop' if slot_exists else 'create'
                try:
                    if slot_exists:
                        LOGGER.info('Dropping the slot "%s"', slot_name)
                        cur.execute('SELECT pg_drop_replication_slot(%s)', (slot_name,))
                    if source_name:
                        phase = 'drop legacy'
                        LOGGER.info('Dropping the historical wal2json slot "%s"', source_name)
                        cur.execute('SELECT pg_drop_replication_slot(%s)', (source_name,))
                    phase = 'create'
                    LOGGER.info('Creating the slot "%s"', slot_name)
                    cur.execute(
                        'SELECT slot_name, lsn::text FROM pg_create_logical_replication_slot(%s, %s)',
                        (slot_name, PGOUTPUT_PLUGIN),
                    )
                    created_name, slot_lsn = cur.fetchone()
                    if created_name != slot_name:
                        raise RuntimeError(f'PostgreSQL returned unexpected slot {created_name!r} during reset.')
                    return {
                        'destination_slot': slot_name,
                        'slot_lsn': cls._lsn_to_int(slot_lsn),
                    }
                except psycopg2.Error as exc:
                    raise RuntimeError(
                        f'PostgreSQL slot reset failed during {phase} for "{slot_name}"; '
                        'the source-side outcome may be uncertain. Tap bookmarks remain invalidated. '
                        f'Pre-reset state backup: {backup_path or "no previous state file"}. '
                        'Keep scheduled replication stopped, resolve the source error, and rerun '
                        'the unfiltered fast_sync (adding --force only to bypass the size limit). '
                        'Do not restore old LOG_BASED bookmarks '
                        'after a completed or uncertain slot mutation.'
                    ) from exc
        finally:
            connection.close()

    @classmethod
    def _preflight_slot_reset(cls, cursor, connection_config):
        """Reject active or incompatible pgoutput slots before invalidating state."""
        database = connection_config['dbname']
        slot_name, legacy, current = cls.validate_replication_slot_identity(
            database, connection_config['tap_id'], connection_config.get('previous_tap_id')
        )
        slots = cls._fetch_replication_slots(cursor, (slot_name, legacy, current))
        preserve_current = cls._implicit_truncated_historical_slot_present(
            database, connection_config['tap_id'], connection_config.get('previous_tap_id'),
            legacy, current, slots,
        )
        if preserve_current:
            LOGGER.warning(
                'Preserving implicitly truncated historical PostgreSQL slot "%s" during slot reset',
                current,
            )
        if slot_name in slots:
            cls._validate_replication_slot(
                slots[slot_name], database, {PGOUTPUT_PLUGIN}, require_inactive=True
            )
        source_name = current if current != legacy and current in slots and not preserve_current else None
        if source_name:
            cls._validate_replication_slot(
                slots[source_name], database, {WAL2JSON_PLUGIN}, require_inactive=True
            )
        return slot_name, slot_name in slots, source_name

    @classmethod
    def get_connection(
        cls,
        connection_config: Dict,
        prioritize_primary: bool = False,
        *,
        allow_unsupported_version_for_config_removal: bool = False,
    ):
        """
        Class method to create a pg connection instance with autocommit enabled
        Connection is either to the primary or a replica if its credentials are given

        Args:
            prioritize_primary: boolean to control whether to connect to primary or replica
            connection_config: Dictionary containing the db connection details
            allow_unsupported_version_for_config_removal: Permit only removed-tap
                cleanup to connect below the supported version floor.
        Returns:
            pg Connection instance
        """
        connection_args = {
            key: connection_config[key] if prioritize_primary else connection_config.get(f'replica_{key}',
                                                                                       connection_config[key])
            for key in ('host', 'port', 'user', 'password')
        }
        connection_args.update({'dbname': connection_config['dbname'], 'connect_timeout': 30})
        if connection_config.get('sslmode'):
            connection_args['sslmode'] = connection_config['sslmode']
        elif connection_config.get('ssl') == 'true':
            connection_args['sslmode'] = 'require'
        conn = psycopg2.connect(**connection_args)

        if not allow_unsupported_version_for_config_removal and conn.server_version < MIN_SUPPORTED_POSTGRES_VERSION:
            server_version = conn.server_version
            try:
                conn.close()
            finally:
                raise UnsupportedPostgresVersionError(
                    'PostgreSQL 14 or later is required; '
                    f'connected server reports server_version_num {server_version}'
                )

        minimum_safe_version = MIN_SAFE_POSTGRES_VERSIONS.get(conn.server_version // 10000)
        if minimum_safe_version is not None and conn.server_version < minimum_safe_version:
            LOGGER.warning(
                'PostgreSQL server_version_num %s predates the logical-decoding catalog-cache fixes in '
                '14.18, 15.13, 16.9, and 17.5. wal2json and pgoutput may omit or misdecode changes; '
                'upgrade to a fixed minor release.',
                conn.server_version,
            )

        # Set connection to autocommit
        conn.autocommit = True

        LOGGER.info('Connection to PGSQL server established')

        return conn

    def open_connection(self):
        """
        Open connection
        """
        self.conn = self.get_connection(
            self.connection_config, prioritize_primary=False
        )
        self.curr = self.conn.cursor()

    def close_connection(self, silent=False):
        """
        Close source connections
        """
        connection = self.conn
        self.conn = None
        self.curr = None

        self._close_primary_host_connection(silent=silent)

        if connection is None:
            return

        try:
            connection.close()
        except Exception as exc:
            if not silent:
                LOGGER.exception(exc)
                LOGGER.info('Connection seems to be already closed.')

    def _close_primary_host_connection(self, silent=False):
        """Close and clear the dedicated primary-host connection."""
        connection = self.primary_host_conn
        self.primary_host_conn = None

        if connection is None:
            return

        try:
            connection.close()
        except Exception as exc:
            if not silent:
                LOGGER.exception(exc)
                LOGGER.info('Primary host connection seems to be already closed.')

    def query(self, query, params=None):
        """
        Run query
        """
        LOGGER.info('Running query: %s', query)
        with self.conn as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, params)

                if cur.rowcount > 0:
                    return cur.fetchall()

                return []

    def primary_host_query(self, query, params=None):
        """
        Run query on the primary host
        """
        LOGGER.info('Running query: %s', query)
        with self.primary_host_conn as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, params)

                if cur.rowcount > 0:
                    return cur.fetchall()

                return []

    def create_replication_slot(self, *, fresh_start=False):
        """Create a fresh pgoutput slot while retaining migration source history."""
        database = self.connection_config['dbname']
        destination, legacy, current = self.validate_replication_slot_identity(
            database, self.connection_config['tap_id'], self.connection_config.get('previous_tap_id')
        )
        slot_names = (destination, legacy, current)

        with self.primary_host_conn.cursor() as cur:
            slots = self._fetch_replication_slots(cur, slot_names)
            if destination in slots:
                self._validate_replication_slot(
                    slots[destination][:4], database, {PGOUTPUT_PLUGIN}, require_inactive=False
                )
                self._wait_for_inactive_slots(
                    cur, database, {destination: PGOUTPUT_PLUGIN}, require_ready=True
                )
                return

            implicit_truncated_source = self._implicit_truncated_historical_slot_present(
                database, self.connection_config['tap_id'], self.connection_config.get('previous_tap_id'),
                legacy, current, slots,
            )
            if implicit_truncated_source and not fresh_start:
                self._reject_implicit_truncated_historical_slot(
                    database, self.connection_config['tap_id'], self.connection_config.get('previous_tap_id'),
                    legacy, current, slots,
                )

            source_name = None if implicit_truncated_source else self._historical_migration_source(
                slots, legacy, current, fresh_start=fresh_start)
            if source_name is None:
                LOGGER.info('Creating pgoutput replication slot "%s"', destination)
                try:
                    cur.execute(
                        'SELECT * FROM pg_create_logical_replication_slot(%s, %s)',
                        (destination, PGOUTPUT_PLUGIN),
                    )
                except psycopg2.Error as exc:
                    self._accept_concurrent_pgoutput_slot(cur, slot_names, database, exc)
                return

            self._validate_replication_slot(
                slots[source_name], database, {WAL2JSON_PLUGIN}, require_inactive=True
            )
            LOGGER.info(
                'Creating pgoutput replication slot "%s" beside wal2json slot "%s"',
                destination,
                source_name,
            )
            try:
                cur.execute(
                    'SELECT * FROM pg_create_logical_replication_slot(%s, %s)',
                    (destination, PGOUTPUT_PLUGIN),
                )
            except psycopg2.Error as exc:
                self._accept_concurrent_pgoutput_slot(cur, slot_names, database, exc)

    @classmethod
    def ensure_replication_slot(cls, connection_config, *, fresh_start=False):
        """Finish canonical slot initialization before starting parallel snapshots."""
        source = cls(connection_config, None)
        source.primary_host_conn = cls.get_connection(connection_config, prioritize_primary=True)
        try:
            source.create_replication_slot(fresh_start=fresh_start)
        finally:
            source._close_primary_host_connection()

    @classmethod
    def capture_snapshot_boundary(cls, connection):
        """Commit a flushed WAL record whose exact end can be replayed by a standby."""
        with connection:
            with connection.cursor() as cur:
                cur.execute('SET LOCAL synchronous_commit = on')
                cur.execute("SELECT pg_logical_emit_message(true, 'pipelinewise_snapshot', '')::text")
                boundary = cls._lsn_to_int(cur.fetchone()[0])
        return boundary

    def fetch_current_log_pos(self):
        """
        Get the actual wal position in Postgres
        """
        # Create replication slot dedicated connection
        # Always use Primary server for creating replication_slot
        self.primary_host_conn = self.get_connection(
            self.connection_config, prioritize_primary=True
        )
        try:
            # Create replication slot
            self.create_replication_slot()
            primary_lsn = self.capture_snapshot_boundary(self.primary_host_conn)
        finally:
            self._close_primary_host_connection()

        # is replica_host set ?
        if self.connection_config.get('replica_host'):
            lsn = self._wait_for_replica_replay(primary_lsn)
        else:
            lsn = primary_lsn

        return {'lsn': lsn, 'version': 1}

    def _wait_for_replica_replay(self, primary_lsn):
        """Wait for a replica snapshot that includes pgoutput publication and slot setup."""
        deadline = monotonic() + REPLICA_REPLAY_TIMEOUT_SECONDS
        LOGGER.info('Waiting for PostgreSQL replica replay through %s', self._int_to_lsn(primary_lsn))
        while True:
            result = self.query('SELECT pg_is_in_recovery() AS in_recovery, pg_last_wal_replay_lsn() AS current_lsn')
            if result and result[0].get('in_recovery') is False:
                return primary_lsn
            replay_lsn = result[0].get('current_lsn') if result else None
            if replay_lsn is not None:
                replay_lsn = self._lsn_to_int(replay_lsn)
                if replay_lsn >= primary_lsn:
                    return replay_lsn
            if monotonic() >= deadline:
                raise RuntimeError(
                    'PostgreSQL replica did not replay the pgoutput publication and slot boundary '
                    f'{self._int_to_lsn(primary_lsn)} within {REPLICA_REPLAY_TIMEOUT_SECONDS} seconds. '
                    'No snapshot was exported. Resolve replica lag and retry the sync.'
                )
            sleep(1)

    def fetch_current_incremental_key_pos(self, table, replication_key):
        """
        Get the actual incremental key position in the table
        """
        validate_bookmark_column(table, replication_key, self.source_transformations)
        schema_name, table_name = table.split('.')
        result = self.query(
            f'SELECT MAX({replication_key}) AS key_value FROM {schema_name}."{table_name}"'
        )
        if not result:
            raise Exception(
                f'Cannot get replication key value for table: {table}'
            )

        postgres_key_value = result[0].get('key_value')

        if postgres_key_value is None:
            LOGGER.warning('No replication value found for table %s, returning empty bookmark', table)
            return {}

        key_value = postgres_key_value

        # Convert postgres data/datetime format to JSON friendly values
        if isinstance(postgres_key_value, datetime.datetime):
            key_value = postgres_key_value.isoformat()

        elif isinstance(postgres_key_value, datetime.date):
            key_value = postgres_key_value.isoformat() + 'T00:00:00'

        elif isinstance(postgres_key_value, decimal.Decimal):
            key_value = float(postgres_key_value)

        return {
            'replication_key': replication_key,
            'replication_key_value': key_value,
            'version': 1,
        }

    def get_primary_keys(self, table):
        """
        Get the primary key of a table
        """
        schema_name, table_name = table.split('.')

        sql = """
            SELECT attribute.attname
            FROM pg_catalog.pg_index AS index_def
            JOIN pg_catalog.pg_class AS table_class
              ON table_class.oid = index_def.indrelid
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = table_class.relnamespace
            CROSS JOIN LATERAL unnest(index_def.indkey)
              WITH ORDINALITY AS key_column(attnum, key_ordinality)
            JOIN pg_catalog.pg_attribute AS attribute
              ON attribute.attrelid = table_class.oid
             AND attribute.attnum = key_column.attnum
            WHERE namespace.nspname = %s
              AND table_class.relname = %s
              AND index_def.indisprimary
              AND key_column.key_ordinality <= index_def.indnkeyatts
            ORDER BY key_column.key_ordinality
        """
        pk_specs = self.query(sql, (schema_name, table_name))
        if len(pk_specs) > 0:
            return [safe_column_name(k[0], self.target_quote) for k in pk_specs]

        return None

    def get_table_columns(self, table_name, max_num=None, date_type='date', *, metadata_query=None):
        """
        Get PG table column details from information_schema
        """
        table_dict = utils.tablename_to_dict(table_name)

        if max_num:
            decimals = len(max_num.split('.')[1]) if '.' in max_num else 0

            decimal_format = f"""
              'CASE WHEN "' || column_name || '" IS NULL THEN NULL ELSE GREATEST(LEAST({max_num}, ROUND("' || column_name || '"::numeric , {decimals})), -{max_num}) END'
            """  # noqa: E501
            integer_format = """
              '"' || column_name || '"'
            """
        else:
            decimal_format = """
              '"' || column_name || '"'
            """
            integer_format = decimal_format

        schema_name = table_dict.get('schema_name')
        table_name = table_dict.get('table_name')
        hstore_projection = (
            "WHEN udt_name = 'hstore' THEN 'hstore_to_json(\"' || "
            "column_name || '\") AS \"' || column_name || '\"'"
            if self.hstore_as_json else ''
        )

        sql = f"""
                SELECT
                    column_name
                    ,CASE WHEN udt_name = 'hstore' THEN 'hstore' ELSE data_type END AS data_type
                    ,safe_sql_value
                    ,character_maximum_length
                FROM (SELECT
                column_name,
                data_type,
                udt_name,
                CASE
                    WHEN data_type = 'ARRAY' THEN 'array_to_json("' || column_name || '") AS ' || column_name
                    {hstore_projection}
                    WHEN data_type = 'date' THEN
                       'CASE WHEN "' ||column_name|| E'" < \\'0001-01-01\\' '
                            'OR "' ||column_name|| E'" > \\'9999-12-31\\' THEN \\'9999-12-31\\' '
                            'ELSE "' ||column_name|| '"::{date_type} END AS "' ||column_name|| '"'
                    WHEN udt_name = 'time' THEN 'replace("' || column_name || E'"::varchar,\\\'24:00:00\\\',\\\'00:00:00\\\') AS ' || column_name
                    WHEN udt_name = 'timetz' THEN 'replace(("' || column_name || E'" at time zone \'\'UTC\'\')::time::varchar,\\\'24:00:00\\\',\\\'00:00:00\\\') AS ' || column_name
                    WHEN udt_name in ('timestamp', 'timestamptz') THEN
                       'CASE WHEN "' ||column_name|| E'" < \\'0001-01-01 00:00:00.000\\' '
                            'OR "' ||column_name|| E'" > \\'9999-12-31 23:59:59.999\\' THEN \\'9999-12-31 23:59:59.999\\' '
                            'ELSE "' ||column_name|| '" END AS "' ||column_name|| '"'
                    WHEN data_type IN ('double precision', 'numeric', 'decimal', 'real') THEN {decimal_format} || ' AS ' || column_name
                    WHEN data_type IN ('smallint', 'integer', 'bigint', 'serial', 'bigserial') THEN {integer_format} || ' AS ' || column_name
                    ELSE '"'||column_name||'"'
                END AS safe_sql_value,
                character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = %s
                    AND table_name = %s
                ORDER BY ordinal_position
                ) AS x
            """  # noqa: E501

        query = self.query if metadata_query is None else metadata_query
        return query(sql, params=(schema_name, table_name))

    def map_table_columns(self, columns):
        """Map already-read metadata without connections or primary-key queries."""
        return [
            '{} {}'.format(
                safe_column_name(column[0], self.target_quote), self._mapped_column_type(column[1], column[3]),
            )
            for column in columns
        ]

    def map_column_types_to_target(self, table_name):
        """
        Map PG column types to equivalent types in target
        """
        postgres_columns = self.get_table_columns(table_name)
        return {
            'columns': self.map_table_columns(postgres_columns),
            'primary_key': self.get_primary_keys(table_name),
            'source_column_names': [column[0] for column in postgres_columns],
        }

    def copy_table(
        self,
        table_name,
        path,
        max_num=None,
        date_type='date',
        split_large_files=False,
        split_file_chunk_size_mb=1000,
        split_file_max_chunks=20,
        compress=True,
        boundary=None,
    ):
        """
        Export data from table to a zipped csv
        Args:
            table_name: Fully qualified table name to export
            path: Path where to create the zip file(s) with the exported data
            split_large_files: Split large files to multiple pieces and create multiple zip files
                               with -partXYZ postfix in the filename. (Default: False)
            split_file_chunk_size_mb: File chunk sizes if `split_large_files` enabled. (Default: 1000)
            split_file_max_chunks: Max number of chunks if `split_large_files` enabled. (Default: 20)
        """
        table_columns = self.get_table_columns(table_name, max_num, date_type)
        column_safe_sql_values = [c.get('safe_sql_value') for c in table_columns]

        # If self.get_table_columns returns zero row then table not exist
        if len(column_safe_sql_values) == 0:
            raise Exception(f'{table_name} table not found.')

        source_boundary = (
            boundary.source_sql(
                'postgres',
                [column[0] for column in table_columns],
            )
            if boundary is not None
            else None
        )

        source_table = table_name
        schema_name, table_name = table_name.split('.')

        metadata_columns = [
            "now() AT TIME ZONE 'UTC' AS _SDC_EXTRACTED_AT",
            "now() AT TIME ZONE 'UTC' AS _SDC_BATCHED_AT",
            'null _SDC_DELETED_AT'
        ]
        column_safe_sql_values += metadata_columns

        if source_boundary is not None:
            where_clause = self.curr.mogrify(
                source_boundary.statement,
                source_boundary.parameters,
            )
            if isinstance(where_clause, bytes):
                connection_encoding = self.curr.connection.encoding
                python_encoding = psycopg2.extensions.encodings.get(
                    connection_encoding, connection_encoding
                )
                where_clause = where_clause.decode(python_encoding)
        else:
            where_clause = ''

        select_sql = f'SELECT {",".join(column_safe_sql_values)} FROM {schema_name}."{table_name}"{where_clause}'
        transformed = self._compile_source_projection(source_table, table_columns, where_clause)
        if transformed is not None:
            select_sql = f'SELECT _ppw_export.*, {",".join(metadata_columns)} FROM ({transformed}) AS _ppw_export'
        sql = f"COPY ({select_sql}) TO STDOUT with CSV DELIMITER ','"

        LOGGER.info('Exporting data: %s', sql)

        gzip_splitter = split_gzip.open(
            path,
            mode='wb',
            chunk_size_mb=split_file_chunk_size_mb,
            max_chunks=split_file_max_chunks if split_large_files else 0,
            compress=compress,
        )

        with gzip_splitter as split_gzip_files:
            self.curr.copy_expert(sql, split_gzip_files, size=131072)

    def _mapped_column_type(self, data_type, character_maximum_length):
        """Share the existing target mapping between DDL and transformation validation."""
        column_type = (
            'VARIANT' if data_type == 'hstore' and self.hstore_as_json else self.tap_type_to_target_type(data_type)
        )
        if isinstance(column_type, list):
            column_type = column_type[1 if character_maximum_length > 1 else 0]
        return column_type

    def _compile_source_projection(self, table_name, table_columns, where_clause=''):
        """Share projection validation between recovery preflight and export."""
        if self.source_transformations is None:
            return None
        columns = []
        for column in table_columns:
            target_type = self._mapped_column_type(column['data_type'], column.get('character_maximum_length'))
            columns.append(dict(column, target_type=target_type))
        table_reference = '.'.join(
            quote_source_identifier(part, 'postgres') for part in table_name.split('.')
        )
        return compile_source_select(
            table_name, table_reference, where_clause, columns,
            self.source_transformations, 'postgres', self.target_iceberg_version,
        )

    def validate_source_transformations(self, table_name):
        """Reject invalid rules before binding a new Iceberg recovery attempt."""
        if self.source_transformations is not None:
            self._compile_source_projection(table_name, self.get_table_columns(table_name))

    def export_source_table_data(
            self, args: Namespace, tap_id: str,
            boundary: PartialSyncBoundary = None) -> list:
        """Exporting data from the source table"""
        filename = utils.gen_export_filename(tap_id=tap_id, table=args.table, sync_type='partialsync')
        filepath = os.path.join(args.temp_dir, filename)

        self.copy_table(
            args.table,
            filepath,
            split_large_files=args.target.get('split_large_files'),
            split_file_chunk_size_mb=args.target.get('split_file_chunk_size_mb'),
            split_file_max_chunks=args.target.get('split_file_max_chunks'),
            boundary=boundary
        )
        file_parts = glob.glob(f'{filepath}*')
        return file_parts
