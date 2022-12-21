"""
PipelineWise CLI - Pipelinewise class
"""
import logging
import os
import shutil
import signal
import sys
import json
import copy
import psutil
import pidfile
import multiprocessing

from datetime import datetime
from time import time
from typing import Dict, Optional, List, Any
from joblib import Parallel, delayed, parallel_backend
from tabulate import tabulate

from . import utils
from .constants import ConnectorType
from . import commands
from .commands import TapParams, TargetParams, TransformParams
from .config import Config
from .alert_sender import AlertSender
from .alert_handlers.base_alert_handler import BaseAlertHandler
from .errors import (
    InvalidTransformationException, DuplicateConfigException,
    InvalidConfigException, PartialSyncNotSupportedTypeException,
    PreRunChecksException
)

FASTSYNC_PAIRS = {
    ConnectorType.TAP_MYSQL: {
        ConnectorType.TARGET_SNOWFLAKE,
        ConnectorType.TARGET_REDSHIFT,
        ConnectorType.TARGET_POSTGRES,
        ConnectorType.TARGET_BIGQUERY,
    },
    ConnectorType.TAP_POSTGRES: {
        ConnectorType.TARGET_SNOWFLAKE,
        ConnectorType.TARGET_REDSHIFT,
        ConnectorType.TARGET_POSTGRES,
        ConnectorType.TARGET_BIGQUERY,
    },
    ConnectorType.TAP_MONGODB: {
        ConnectorType.TARGET_SNOWFLAKE,
        ConnectorType.TARGET_POSTGRES,
        ConnectorType.TARGET_BIGQUERY,
    },
}

PARTIAL_SYNC_PAIRS = {
    ConnectorType.TAP_MYSQL: {
        ConnectorType.TARGET_SNOWFLAKE
    },
    ConnectorType.TAP_POSTGRES: {
        ConnectorType.TARGET_SNOWFLAKE
    }

}


# pylint: disable=too-many-lines,too-many-instance-attributes,too-many-public-methods
class PipelineWise:
    """PipelineWise main Class"""

    INCREMENTAL = 'INCREMENTAL'
    LOG_BASED = 'LOG_BASED'
    FULL_TABLE = 'FULL_TABLE'
    STATUS_SUCCESS = 'SUCCESS'
    STATUS_FAILED = 'FAILED'
    TRANSFORM_FIELD_CONNECTOR_NAME = 'transform-field'

    def __init__(self, args, config_dir, venv_dir, profiling_dir=None):

        self.profiling_mode = args.profiler
        self.profiling_dir = profiling_dir
        self.drop_pg_slot = False
        self.args = args
        self.logger = logging.getLogger(__name__)
        self.config_dir = config_dir
        self.venv_dir = venv_dir
        self.extra_log = args.extra_log
        self.pipelinewise_bin = os.path.join(
            self.venv_dir, 'cli', 'bin', 'pipelinewise'
        )
        self.config_path = os.path.join(self.config_dir, 'config.json')
        self.load_config()
        self.alert_sender = AlertSender(self.config.get('alert_handlers'))

        if args.tap != '*':
            self.tap = self.get_tap(args.target, args.tap)
            self.tap_bin = self.get_connector_bin(self.tap['type'])
            self.tap_python_bin = self.get_connector_python_bin(self.tap['type'])

        if args.target != '*':
            self.target = self.get_target(args.target)
            self.target_bin = self.get_connector_bin(self.target['type'])
            self.target_python_bin = self.get_connector_python_bin(self.target['type'])

        self.transform_field_bin = self.get_connector_bin(
            self.TRANSFORM_FIELD_CONNECTOR_NAME
        )
        self.transform_field_python_bin = self.get_connector_python_bin(
            self.TRANSFORM_FIELD_CONNECTOR_NAME
        )
        self.tap_run_log_file = None

        # Catch SIGINT and SIGTERM to exit gracefully
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig, self.stop_tap)

    def send_alert(
        self, message: str, level: str = BaseAlertHandler.ERROR, exc: Exception = None
    ) -> dict:
        """
        Send alert messages to every alert handler if sender is not disabled for the tap

        Args:
            message: the alert message
            level: alert level
            exc: optional exception that triggered the alert

        Returns:
            Dictionary with number of successfully sent alerts
        """
        stats = {'sent': 0}

        send_alert = self.tap.get('send_alert', True)
        if send_alert:
            tap_slack_channel = self.tap.get('slack_alert_channel')
            stats = self.alert_sender.send_to_all_handlers(
                message=message, level=level, exc=exc, tap_slack_channel=tap_slack_channel
            )

        return stats

    def create_consumable_target_config(self, target_config, tap_inheritable_config):
        """
        Create consumable target config by appending "inheritable" config to the common target config
        """
        dict_a, dict_b = {}, {}
        try:
            dict_a = utils.load_json(target_config)
            dict_b = utils.load_json(tap_inheritable_config)

            # Copy everything from dictB into dictA - Not a real merge
            dict_a.update(dict_b)

            # Save the new dict as JSON into a temp file
            tempfile_path = utils.create_temp_file(
                dir=self.get_temp_dir(), prefix='target_config_', suffix='.json'
            )[1]
            utils.save_json(dict_a, tempfile_path)

            return tempfile_path
        except Exception as exc:
            raise Exception(
                f'Cannot merge JSON files {dict_a} {dict_b} - {exc}'
            ) from exc

    # pylint: disable=too-many-statements,too-many-branches,too-many-nested-blocks,too-many-locals,too-many-arguments
    def create_filtered_tap_properties(
        self,
        target_type: ConnectorType,
        tap_type: ConnectorType,
        tap_properties: str,
        tap_state: str,
        filters: Dict[str, Any],
        create_fallback=False,
    ):
        """
        Create a filtered version of tap properties file based on specific filter conditions.

        Return values:
            1) A temporary JSON file where only those tables are selected to
                sync which meet the filter criteria
            2) List of tap_stream_ids where filter criteria matched
            3) OPTIONAL when create_fallback is True:
                Temporary JSON file with table that don't meet the
                filter criteria
            4) OPTIONAL when create_fallback is True:
                List of tap_stream_ids where filter criteria don't match
        """
        # Get filter conditions with default values from input dictionary
        # Nothing selected by default
        f_selected: bool = filters.get('selected', False)
        f_tap_target_pairs: Dict = filters.get('tap_target_pairs', {})
        f_replication_method = filters.get('replication_method', None)
        f_initial_sync_required: bool = filters.get('initial_sync_required', False)

        # Lists of tables that meet and don't meet the filter criteria
        filtered_tap_stream_ids = []
        fallback_filtered_stream_ids = []

        self.logger.debug('Filtering properties JSON by conditions: %s', filters)
        try:
            # Load JSON files
            properties = utils.load_json(tap_properties)
            state = utils.load_json(tap_state)

            # Create a dictionary for tables that don't meet filter criteria
            fallback_properties = copy.deepcopy(properties) if create_fallback else {}

            # Foreach stream (table) in the original properties
            for stream_idx, stream in enumerate(
                properties.get('streams', tap_properties)
            ):
                initial_sync_required = False

                # Collect required properties from the properties file
                tap_stream_id = stream.get('tap_stream_id')
                table_name = stream.get('table_name')
                metadata = stream.get('metadata', [])

                # Collect further properties from the properties file under the metadata key
                table_meta = {}
                meta_idx = 0
                for meta_idx, meta in enumerate(metadata):
                    if isinstance(meta, dict) and len(meta.get('breadcrumb', [])) == 0:
                        table_meta = meta.get('metadata')
                        break

                # Can we make sure that the stream has the right metadata?
                # To be safe, check if no right metadata has been found, then throw an exception.
                if not table_meta:
                    self.logger.error(
                        'Stream %s has no metadata with no breadcrumbs: %s.',
                        tap_stream_id,
                        metadata,
                    )
                    raise Exception(f'Missing metadata in stream {tap_stream_id}')

                selected = table_meta.get('selected', False)
                replication_method = table_meta.get('replication-method', None)

                # Detect if initial sync is required. Look into the state file, get the bookmark
                # for the current stream (table) and if valid bookmark doesn't exist then
                # initial sync is required
                bookmarks = (
                    state.get('bookmarks', {}) if isinstance(state, dict) else {}
                )

                new_stream = False

                # if stream not in bookmarks, then it's a new table
                if tap_stream_id not in bookmarks:
                    new_stream = True
                    initial_sync_required = True
                else:
                    stream_bookmark = bookmarks[tap_stream_id]

                    if self._is_initial_sync_required(
                        replication_method, stream_bookmark
                    ):
                        initial_sync_required = True

                # Compare actual values to the filter conditions.
                # Set the "selected" key to True if actual values meet the filter criteria
                # Set the "selected" key to False if the actual values don't meet the filter criteria
                # pylint: disable=too-many-boolean-expressions
                if (
                    (f_selected is None or selected == f_selected)
                    and (
                        f_tap_target_pairs is None
                        or target_type in f_tap_target_pairs.get(tap_type, set())
                    )
                    and (
                        f_replication_method is None
                        or replication_method in f_replication_method
                    )
                    and (
                        f_initial_sync_required is None
                        or initial_sync_required == f_initial_sync_required
                    )
                ):
                    self.logger.debug(
                        """Filter condition(s) matched:
                        Table              : %s
                        Tap Stream ID      : %s
                        Selected           : %s
                        Replication Method : %s
                        Init Sync Required : %s
                    """,
                        table_name,
                        tap_stream_id,
                        selected,
                        replication_method,
                        initial_sync_required,
                    )

                    # Filter condition matched: mark table as selected to sync
                    properties['streams'][stream_idx]['metadata'][meta_idx]['metadata'][
                        'selected'
                    ] = True
                    filtered_tap_stream_ids.append(tap_stream_id)

                    # Filter condition matched:
                    # if the stream is a new table and is a singer stream, then mark it as selected to sync in the
                    # the fallback properties as well if the table is selected in the original properties.
                    # Otherwise, mark it as not selected
                    if create_fallback:
                        if new_stream and replication_method in [
                            self.INCREMENTAL,
                            self.LOG_BASED,
                        ]:
                            fallback_properties['streams'][stream_idx]['metadata'][
                                meta_idx
                            ]['metadata']['selected'] = True
                            if selected:
                                fallback_filtered_stream_ids.append(tap_stream_id)
                        else:
                            fallback_properties['streams'][stream_idx]['metadata'][
                                meta_idx
                            ]['metadata']['selected'] = False
                else:
                    # Filter condition didn't match: mark table as not selected to sync
                    properties['streams'][stream_idx]['metadata'][meta_idx]['metadata'][
                        'selected'
                    ] = False

                    # Filter condition didn't match: mark table as selected to sync in the fallback properties
                    # Fallback only if the table is selected in the original properties
                    if create_fallback and selected is True:
                        fallback_properties['streams'][stream_idx]['metadata'][
                            meta_idx
                        ]['metadata']['selected'] = True
                        fallback_filtered_stream_ids.append(tap_stream_id)

            # Save the generated properties file(s) and return
            # Fallback required: Save filtered and fallback properties JSON
            if create_fallback:
                # Save to files: filtered and fallback properties
                temp_properties_path = utils.create_temp_file(
                    dir=self.get_temp_dir(), prefix='properties_', suffix='.json'
                )[1]
                utils.save_json(properties, temp_properties_path)

                temp_fallback_properties_path = utils.create_temp_file(
                    dir=self.get_temp_dir(), prefix='properties_', suffix='.json'
                )[1]
                utils.save_json(fallback_properties, temp_fallback_properties_path)

                return (
                    temp_properties_path,
                    filtered_tap_stream_ids,
                    temp_fallback_properties_path,
                    fallback_filtered_stream_ids,
                )

            # Fallback not required: Save only the filtered properties JSON
            temp_properties_path = utils.create_temp_file(
                dir=self.get_temp_dir(), prefix='properties_', suffix='.json'
            )[1]
            utils.save_json(properties, temp_properties_path)

            return temp_properties_path, filtered_tap_stream_ids

        except Exception as exc:
            raise Exception(f'Cannot create JSON file - {exc}') from exc

    def load_config(self):
        """
        Load configuration
        """
        self.logger.debug('Loading config at %s', self.config_path)
        config = utils.load_json(self.config_path)

        if config:
            self.config = config
        else:
            self.config = {}

    def get_temp_dir(self):
        """
        Returns the tap specific temp directory
        """
        return os.path.join(self.config_dir, 'tmp')

    def get_tap_dir(self, target_id, tap_id):
        """
        Get absolute path of a tap directory
        """
        return os.path.join(self.config_dir, target_id, tap_id)

    def get_tap_log_dir(self, target_id, tap_id):
        """
        Get absolute path of a tap log directory
        """
        return os.path.join(self.get_tap_dir(target_id, tap_id), 'log')

    def get_target_dir(self, target_id):
        """
        Get absolute path of a target directory
        """
        return os.path.join(self.config_dir, target_id)

    def get_connector_bin(self, connector_type):
        """
        Get absolute path of a connector executable
        """
        return os.path.join(self.venv_dir, connector_type, 'bin', connector_type)

    def get_connector_python_bin(self, connector_type):
        """
        Get absolute path of a connector python command
        """
        return os.path.join(self.venv_dir, connector_type, 'bin', 'python')

    def get_targets(self):
        """
        Get every target
        """
        self.logger.debug('Getting targets from %s', self.config_path)
        self.load_config()
        try:
            targets = self.config.get('targets', [])
        except Exception as exc:
            raise Exception('Targets not defined') from exc

        return targets

    def get_target(self, target_id: str) -> Dict:
        """
        Get target by id
        """
        self.logger.debug('Getting %s target', target_id)
        targets = self.get_targets()

        target = next((item for item in targets if item['id'] == target_id), None)

        if not target:
            raise Exception(f'Cannot find {target_id} target')

        target_dir = self.get_target_dir(target_id)
        if os.path.isdir(target_dir):
            target['files'] = Config.get_connector_files(target_dir)
        else:
            raise Exception(f'Cannot find target at {target_dir}')

        return target

    def get_taps(self, target_id):
        """
        Get every tap from a specific target
        """
        self.logger.debug('Getting taps from %s target', target_id)
        target = self.get_target(target_id)

        try:
            taps = target['taps']

            # Add tap status
            for tap_idx, tap in enumerate(taps):
                taps[tap_idx]['status'] = self.detect_tap_status(target_id, tap['id'])

        except Exception as exc:
            raise Exception(f'No taps defined for {target_id} target') from exc

        return taps

    def get_tap(self, target_id: str, tap_id: str) -> Dict:
        """
        Get tap by id from a specific target
        """
        self.logger.debug('Getting %s tap from target %s', tap_id, target_id)
        taps = self.get_taps(target_id)

        tap = next((item for item in taps if item['id'] == tap_id), None)

        if not tap:
            raise Exception(f'Cannot find {tap_id} tap in {target_id} target')

        tap_dir = self.get_tap_dir(target_id, tap_id)
        if os.path.isdir(tap_dir):
            tap['files'] = Config.get_connector_files(tap_dir)
        else:
            raise Exception(f'Cannot find tap at {tap_dir}')

        # Add target and status details
        tap['target'] = self.get_target(target_id)
        tap['status'] = self.detect_tap_status(target_id, tap_id)

        return tap

    # TODO: This method is too complex! make its complexity less than 15!
    # pylint: disable=too-many-branches,too-many-statements,too-many-nested-blocks,too-many-locals
    def merge_schemas(self, old_schema, new_schema):  # noqa: C901
        """
        Merge two schemas
        """
        schema_with_diff = new_schema

        if not old_schema:
            schema_with_diff = new_schema
        else:
            new_streams = new_schema['streams']
            old_streams = old_schema['streams']
            for new_stream_idx, new_stream in enumerate(new_streams):
                new_tap_stream_id = new_stream['tap_stream_id']

                old_stream = next(
                    (
                        item
                        for item in old_streams
                        if item['tap_stream_id'] == new_tap_stream_id
                    ),
                    None,
                )

                # Is this a new stream?
                if not old_stream:
                    new_schema['streams'][new_stream_idx]['is-new'] = True

                # Copy stream selection from the old properties
                else:
                    # Find table specific metadata entries in the old and new streams
                    new_stream_table_mdata_idx = 0
                    old_stream_table_mdata_idx = 0
                    try:
                        new_stream_table_mdata_idx = [
                            i
                            for i, md in enumerate(new_stream['metadata'])
                            if md['breadcrumb'] == []
                        ][0]
                        old_stream_table_mdata_idx = [
                            i
                            for i, md in enumerate(old_stream['metadata'])
                            if md['breadcrumb'] == []
                        ][0]
                    except Exception:
                        pass

                    # Copy is-new flag from the old stream
                    try:
                        new_schema['streams'][new_stream_idx]['is-new'] = old_stream[
                            'is-new'
                        ]
                    except Exception:
                        pass

                    # Copy selected from the old stream
                    try:
                        new_schema['streams'][new_stream_idx]['metadata'][
                            new_stream_table_mdata_idx
                        ]['metadata']['selected'] = old_stream['metadata'][
                            old_stream_table_mdata_idx
                        ][
                            'metadata'
                        ][
                            'selected'
                        ]
                    except Exception:
                        pass

                    # Copy replication method from the old stream
                    try:
                        new_schema['streams'][new_stream_idx]['metadata'][
                            new_stream_table_mdata_idx
                        ]['metadata']['replication-method'] = old_stream['metadata'][
                            old_stream_table_mdata_idx
                        ][
                            'metadata'
                        ][
                            'replication-method'
                        ]
                    except Exception:
                        pass

                    # Copy replication key from the old stream
                    try:
                        new_schema['streams'][new_stream_idx]['metadata'][
                            new_stream_table_mdata_idx
                        ]['metadata']['replication-key'] = old_stream['metadata'][
                            old_stream_table_mdata_idx
                        ][
                            'metadata'
                        ][
                            'replication-key'
                        ]
                    except Exception:
                        pass

                    # Is this new or modified field?
                    new_fields = new_schema['streams'][new_stream_idx]['schema'][
                        'properties'
                    ]
                    old_fields = old_stream['schema']['properties']
                    for new_field_key in new_fields:
                        new_field = new_fields[new_field_key]
                        new_field_mdata_idx = -1

                        # Find new field metadata index
                        for i, mdata in enumerate(
                            new_schema['streams'][new_stream_idx]['metadata']
                        ):
                            if (
                                len(mdata['breadcrumb']) == 2
                                and mdata['breadcrumb'][0] == 'properties'
                                and mdata['breadcrumb'][1] == new_field_key
                            ):
                                new_field_mdata_idx = i

                        # Field exists
                        if new_field_key in old_fields.keys():
                            old_field = old_fields[new_field_key]
                            old_field_mdata_idx = -1

                            # Find old field metadata index
                            for i, mdata in enumerate(old_stream['metadata']):
                                if (
                                    len(mdata['breadcrumb']) == 2
                                    and mdata['breadcrumb'][0] == 'properties'
                                    and mdata['breadcrumb'][1] == new_field_key
                                ):
                                    old_field_mdata_idx = i

                            new_mdata = new_schema['streams'][new_stream_idx][
                                'metadata'
                            ][new_field_mdata_idx]['metadata']
                            old_mdata = old_stream['metadata'][old_field_mdata_idx][
                                'metadata'
                            ]

                            # Copy is-new flag from the old properties
                            try:
                                new_mdata['is-new'] = old_mdata['is-new']
                            except Exception:
                                pass

                            # Copy is-modified flag from the old properties
                            try:
                                new_mdata['is-modified'] = old_mdata['is-modified']
                            except Exception:
                                pass

                            # Copy field selection from the old properties
                            try:
                                new_mdata['selected'] = old_mdata['selected']
                            except Exception:
                                pass

                            # Field exists and type is the same - Do nothing more in the schema
                            if new_field == old_field:
                                self.logger.debug(
                                    'Field exists in %s stream with the same type: %s: %s',
                                    new_tap_stream_id,
                                    new_field_key,
                                    new_field,
                                )

                            # Field exists but types are different - Mark the field as modified in the metadata
                            else:
                                self.logger.debug(
                                    'Field exists in %s stream but types are different: %s: %s}',
                                    new_tap_stream_id,
                                    new_field_key,
                                    new_field,
                                )
                                try:
                                    new_schema['streams'][new_stream_idx]['metadata'][
                                        new_field_mdata_idx
                                    ]['metadata']['is-modified'] = True
                                    new_schema['streams'][new_stream_idx]['metadata'][
                                        new_field_mdata_idx
                                    ]['metadata']['is-new'] = False
                                except Exception:
                                    pass

                        # New field - Mark the field as new in the metadata
                        else:
                            self.logger.debug(
                                'New field in stream %s: %s: %s',
                                new_tap_stream_id,
                                new_field_key,
                                new_field,
                            )
                            try:
                                new_schema['streams'][new_stream_idx]['metadata'][
                                    new_field_mdata_idx
                                ]['metadata']['is-new'] = True
                            except Exception:
                                pass

            schema_with_diff = new_schema

        return schema_with_diff

    def make_default_selection(self, schema, selection_file):
        """
        Select the streams to sync in schema from a selection JSON file
        """
        if os.path.isfile(selection_file):
            self.logger.debug('Loading pre defined selection from %s', selection_file)
            tap_selection = utils.load_json(selection_file)
            selection = tap_selection['selection']

            streams = schema['streams']
            for stream_idx, stream in enumerate(streams):
                tap_stream_id = stream.get('tap_stream_id')
                tap_stream_sel = None
                for sel in selection:
                    if (
                        'tap_stream_id' in sel
                        and tap_stream_id.lower() == sel['tap_stream_id'].lower()
                    ):
                        tap_stream_sel = sel

                # Find table specific metadata entries in the old and new streams
                try:
                    stream_table_mdata_idx = [
                        i
                        for i, md in enumerate(stream['metadata'])
                        if md['breadcrumb'] == []
                    ][0]
                except Exception as exc:
                    raise Exception(
                        f'Metadata of stream {tap_stream_id} doesn\'t have an empty breadcrumb'
                    ) from exc

                if tap_stream_sel:
                    self.logger.debug(
                        'Mark %s tap_stream_id as selected with properties %s',
                        tap_stream_id,
                        tap_stream_sel,
                    )
                    schema['streams'][stream_idx]['metadata'][stream_table_mdata_idx][
                        'metadata'
                    ]['selected'] = True
                    if 'replication_method' in tap_stream_sel:
                        schema['streams'][stream_idx]['metadata'][
                            stream_table_mdata_idx
                        ]['metadata']['replication-method'] = tap_stream_sel[
                            'replication_method'
                        ]
                    if 'replication_key' in tap_stream_sel:
                        schema['streams'][stream_idx]['metadata'][
                            stream_table_mdata_idx
                        ]['metadata']['replication-key'] = tap_stream_sel[
                            'replication_key'
                        ]
                else:
                    self.logger.debug(
                        'Mark %s tap_stream_id as not selected', tap_stream_id
                    )
                    schema['streams'][stream_idx]['metadata'][stream_table_mdata_idx][
                        'metadata'
                    ]['selected'] = False

        return schema

    def init(self):
        """
        Initialise and create a sample project. The project will contain sample YAML configuration for every
        supported tap and target connects.
        """
        self.logger.info('Initialising new project %s...', self.args.name)
        project_dir = os.path.join(os.getcwd(), self.args.name)

        # Create project dir if not exists
        if os.path.exists(project_dir):
            self.logger.error(
                'Directory exists and cannot create new project: %s', self.args.name
            )
            sys.exit(1)
        else:
            os.mkdir(project_dir)

        for yaml in sorted(utils.get_sample_file_paths()):
            yaml_basename = os.path.basename(yaml)
            dst = os.path.join(project_dir, yaml_basename)

            self.logger.info('Creating %s...', yaml_basename)
            shutil.copyfile(yaml, dst)

    def test_tap_connection(self):
        """
        Test the tap connection. It will connect to the data source that is defined in the tap and will return
        success if it’s available.
        """
        tap_id = self.tap['id']
        tap_type = self.tap['type']
        target_id = self.target['id']
        target_type = self.target['type']

        self.logger.info(
            'Testing %s (%s) tap connection in %s (%s) target',
            tap_id,
            tap_type,
            target_id,
            target_type,
        )

        # Generate and run the command to run the tap directly
        # We will use the discover option to test connection
        tap_config = self.tap['files']['config']
        command = f'{self.tap_bin} --config {tap_config} --discover'

        if self.profiling_mode:
            dump_file = os.path.join(self.profiling_dir, f'tap_{tap_id}.pstat')
            command = f'{self.tap_python_bin} -m cProfile -o {dump_file} {command}'

        result = commands.run_command(command)

        # Get output and errors from tap
        # pylint: disable=unused-variable
        returncode, new_schema, tap_output = result

        if returncode != 0:
            self.logger.error(
                'Testing tap connection (%s - %s) FAILED', target_id, tap_id
            )
            sys.exit(1)

        # If the connection success then the response needs to be a valid JSON string
        if not utils.is_json(new_schema):
            self.logger.error(
                'Schema discovered by %s (%s) is not a valid JSON.', tap_id, tap_type
            )
            sys.exit(1)
        else:
            self.logger.info(
                'Testing tap connection (%s - %s) PASSED', target_id, tap_id
            )

    # pylint: disable=too-many-locals,inconsistent-return-statements
    def discover_tap(self, tap=None, target=None):
        """
        Run a specific tap in discovery mode. Discovery mode is connecting to the data source
        and collecting information that is required for running the tap.
        """
        if tap is None:
            tap = self.tap
        if target is None:
            target = self.target

        # Define tap props
        tap_id = tap.get('id')
        tap_type = tap.get('type')
        tap_config_file = tap.get('files', {}).get('config')
        tap_properties_file = tap.get('files', {}).get('properties')
        tap_selection_file = tap.get('files', {}).get('selection')
        tap_bin = self.get_connector_bin(tap_type)
        tap_python_bin = self.get_connector_python_bin(tap_type)

        # Define target props
        target_id = target.get('id')
        target_type = target.get('type')

        self.logger.info(
            'Discovering %s (%s) tap in %s (%s) target...',
            tap_id,
            tap_type,
            target_id,
            target_type,
        )

        # Generate and run the command to run the tap directly
        command = f'{tap_bin} --config {tap_config_file} --discover'

        if self.profiling_mode:
            dump_file = os.path.join(self.profiling_dir, f'tap_{tap_id}.pstat')
            command = f'{tap_python_bin} -m cProfile -o {dump_file} {command}'

        self.logger.debug('Discovery command: %s', command)

        result = commands.run_command(command)

        # Get output and errors from tap
        # pylint: disable=unused-variable
        returncode, new_schema, output = result

        if returncode != 0:
            return f'{target_id} - {tap_id}: {output}'

        # Convert JSON string to object
        try:
            new_schema = json.loads(new_schema)
        except Exception as exc:
            self.logger.exception(exc)
            return f'Schema discovered by {tap_id} ({tap_type}) is not a valid JSON.'

        # Merge the old and new schemas and diff changes
        old_schema = utils.load_json(tap_properties_file)
        if old_schema:
            schema_with_diff = self.merge_schemas(old_schema, new_schema)
        else:
            schema_with_diff = new_schema

        # Make selection from selection.json if exists
        try:
            schema_with_diff = self.make_default_selection(
                schema_with_diff, tap_selection_file
            )
            schema_with_diff = utils.delete_keys_from_dict(
                self.make_default_selection(schema_with_diff, tap_selection_file),
                # Removing multipleOf json schema validations from properties.json,
                # that's causing run time issues
                ['multipleOf'],
            )

        except Exception as exc:
            return f'Cannot load selection JSON at {tap_selection_file}. {str(exc)}'

        # Post import checks
        post_import_errors = self._run_post_import_tap_checks(
            tap, schema_with_diff, target_id
        )
        if len(post_import_errors) > 0:
            return (
                f'Post import tap checks failed in tap {tap_id}: {post_import_errors}'
            )

        # Save the new catalog into the tap
        try:
            self.logger.info(
                'Writing new properties file with changes into %s', tap_properties_file
            )
            utils.save_json(schema_with_diff, tap_properties_file)
        except Exception as exc:
            return f'Cannot save file. {str(exc)}'

    def detect_tap_status(self, target_id, tap_id):
        """
        Detect status of a tap
        """
        self.logger.debug('Detecting %s tap status in %s target', tap_id, target_id)
        tap_dir = self.get_tap_dir(target_id, tap_id)
        log_dir = self.get_tap_log_dir(target_id, tap_id)
        connector_files = Config.get_connector_files(tap_dir)
        status = {
            'currentStatus': 'unknown',
            'lastStatus': 'unknown',
            'lastTimestamp': None,
        }

        # Tap exists but configuration not completed
        if not os.path.isfile(connector_files['config']):
            status['currentStatus'] = 'not-configured'

        # Tap exists and has log in running status
        elif (
            os.path.isdir(log_dir)
            and len(utils.search_files(log_dir, patterns=['*.log.running'])) > 0
        ):
            status['currentStatus'] = 'running'

        # Configured and not running
        else:
            status['currentStatus'] = 'ready'

        # Get last run instance
        if os.path.isdir(log_dir):
            log_files = utils.search_files(
                log_dir, patterns=['*.log.success', '*.log.failed'], sort=True
            )
            if len(log_files) > 0:
                last_log_file = log_files[0]
                log_attr = utils.extract_log_attributes(last_log_file)
                status['lastStatus'] = log_attr['status']
                status['lastTimestamp'] = log_attr['timestamp']

        return status

    def status(self):
        """
        Prints a status summary table of every imported pipeline with their tap and target.
        """
        targets = self.get_targets()

        tab_headers = [
            'Tap ID',
            'Tap Type',
            'Target ID',
            'Target Type',
            'Enabled',
            'Status',
            'Last Sync',
            'Last Sync Result',
        ]
        tab_body = []
        pipelines = 0
        for target in targets:
            taps = self.get_taps(target['id'])

            for tap in taps:
                tab_body.append(
                    [
                        tap.get('id', '<Unknown>'),
                        tap.get('type', '<Unknown>'),
                        target.get('id', '<Unknown>'),
                        target.get('type', '<Unknown>'),
                        tap.get('enabled', '<Unknown>'),
                        tap.get('status', {}).get('currentStatus', '<Unknown>'),
                        tap.get('status', {}).get('lastTimestamp', '<Unknown>'),
                        tap.get('status', {}).get('lastStatus', '<Unknown>'),
                    ]
                )
                pipelines += 1

        print(tabulate(tab_body, headers=tab_headers, tablefmt='simple'))
        print(f'{pipelines} pipeline(s)')

    def run_tap_singer(
        self,
        tap: TapParams,
        target: TargetParams,
        transform: TransformParams,
        stream_buffer_size: int = 0,
    ) -> str:
        """
        Generate and run piped shell command to sync tables using singer taps and targets
        """
        # Build the piped executable command
        command = commands.build_singer_command(
            tap=tap,
            target=target,
            transform=transform,
            stream_buffer_size=stream_buffer_size,
            stream_buffer_log_file=self.tap_run_log_file,
            profiling_mode=self.profiling_mode,
            profiling_dir=self.profiling_dir,
        )

        # Do not run if another instance is already running
        log_dir = os.path.dirname(self.tap_run_log_file)
        if (
            os.path.isdir(log_dir)
            and len(utils.search_files(log_dir, patterns=['*.log.running'])) > 0
        ):
            self.logger.info(
                'Failed to run. Another instance of the same tap is already running. '
                'Log file detected in running status at %s',
                log_dir,
            )
            sys.exit(1)

        start = None
        state = None

        def update_state_file(line: str) -> str:
            # Update state variable with latest state
            if utils.is_state_message(line):
                # if it has been more than 2 seconds since we last updated the state file
                # update it again with newly received state
                nonlocal start, state

                if start is None or time() - start >= 2:
                    with open(tap.state, 'w', encoding='utf-8') as state_file:
                        state_file.write(line)

                    # Update start time to be the current time.
                    start = time()

                # Keep track of state message so that we do one last file update at the end of the run_tap_singer
                # function. This is to avoid the edge case where the last state message and the one before it are
                # less than 2 sec apart.
                state = line

            return line

        # Singer tap is running in subprocess.
        # Collect the formatted logs and log it in the main PipelineWise process as well.
        # Logs are already formatted at this stage so not using logging functions to avoid double formatting.
        def update_state_file_with_extra_log(line: str) -> str:
            sys.stdout.write(line)
            return update_state_file(line)

        # Run command with update_state_file as a callback to call for every stdout line
        if self.extra_log:
            commands.run_command(
                command, self.tap_run_log_file, update_state_file_with_extra_log
            )
        else:
            commands.run_command(command, self.tap_run_log_file, update_state_file)

        # update the state file one last time to make sure it always has the last state message.
        if state is not None:
            with open(tap.state, 'w', encoding='utf-8') as statefile:
                statefile.write(state)

    def run_tap_partialsync(self, tap: TapParams, target: TargetParams, transform: TransformParams):
        """Running the tap for partial sync table"""

        # Build the partial sync executable command
        command = commands.build_partialsync_command(
            tap=tap,
            target=target,
            transform=transform,
            venv_dir=self.venv_dir,
            temp_dir=self.get_temp_dir(),
            table=self.args.table,
            column=self.args.column,
            start_value=self.args.start_value,
            end_value=self.args.end_value,
            drop_target_table=self.args.drop_target_table
        )

        # Do not run if another instance is already running
        self._do_not_run_if_another_instance_is_running(sync_method='partialsync')

        def add_partialsync_output_to_main_logger(line: str) -> str:
            sys.stdout.write(line)
            return line

        if self.extra_log:
            # Run command and copy partialsync output to main logger
            commands.run_command(
                command, self.tap_run_log_file, add_partialsync_output_to_main_logger
            )
        else:
            # Run command
            commands.run_command(command, self.tap_run_log_file)

    def run_tap_fastsync(
        self, tap: TapParams, target: TargetParams, transform: TransformParams
    ):
        """
        Generating and running shell command to sync tables using the native fastsync components
        """
        # Build the fastsync executable command
        command = commands.build_fastsync_command(
            tap=tap,
            target=target,
            transform=transform,
            venv_dir=self.venv_dir,
            temp_dir=self.get_temp_dir(),
            tables=self.args.tables,
            profiling_mode=self.profiling_mode,
            profiling_dir=self.profiling_dir,
            drop_pg_slot=self.drop_pg_slot,
        )

        # Do not run if another instance is already running
        self._do_not_run_if_another_instance_is_running(sync_method='fastsync')

        # Fastsync is running in subprocess.
        # Collect the formatted logs and log it in the main PipelineWise process as well
        # Logs are already formatted at this stage so not using logging functions to avoid double formatting.
        def add_fastsync_output_to_main_logger(line: str) -> str:
            sys.stdout.write(line)
            return line

        if self.extra_log:
            # Run command and copy fastsync output to main logger
            commands.run_command(
                command, self.tap_run_log_file, add_fastsync_output_to_main_logger
            )
        else:
            # Run command
            commands.run_command(command, self.tap_run_log_file)

    # pylint: disable=too-many-statements,too-many-locals
    def run_tap(self):
        """
        Generating command(s) to run tap to sync data from source to target

        The generated commands can use one or multiple commands of:
            1. Fastsync:
                    Native and optimised component to sync table from a
                    specific type of tap into a specific type of target.
                    This command will be used automatically when FULL_TABLE
                    replication method selected or when initial sync is required.

            2. Singer Taps and Targets:
                    Dynamic components following the singer specification to
                    sync tables from multiple sources to multiple targets.
                    This command will be used automatically when INCREMENTAL
                    and LOG_BASED replication method selected. FULL_TABLE
                    replication are not using the singer components because
                    they are too slow to sync large tables.
        """
        tap_id = self.tap['id']
        tap_type = self.tap['type']
        target_id = self.target['id']
        target_type = self.target['type']
        stream_buffer_size = self.tap.get(
            'stream_buffer_size', commands.DEFAULT_STREAM_BUFFER_SIZE
        )

        self.logger.info('Running %s tap in %s target', tap_id, target_id)

        # Run only if tap enabled
        if not self.tap.get('enabled', False):
            self.logger.info('Tap %s is not enabled.', self.tap['name'])
            sys.exit(1)

        # Generate and run the command to run the tap directly
        tap_config = self.tap['files']['config']
        tap_inheritable_config = self.tap['files']['inheritable_config']
        tap_properties = self.tap['files']['properties']
        tap_state = self.tap['files']['state']
        tap_transformation = self.tap['files']['transformation']
        target_config = self.target['files']['config']

        # Some target attributes can be passed and override by tap (aka. inheritable config)
        # We merge the two configs and use that with the target
        cons_target_config = self.create_consumable_target_config(
            target_config, tap_inheritable_config
        )

        # Output will be redirected into target and tap specific log directory
        log_dir = self.get_tap_log_dir(target_id, tap_id)
        current_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

        # Create fastsync and singer specific filtered tap properties that contains only
        # the the tables that needs to be synced by the specific command
        (
            tap_properties_fastsync,
            fastsync_stream_ids,
            tap_properties_singer,
            singer_stream_ids,
        ) = self.create_filtered_tap_properties(
            ConnectorType(target_type),
            ConnectorType(tap_type),
            tap_properties,
            tap_state,
            {
                'selected': True,
                'tap_target_pairs': FASTSYNC_PAIRS,
                'initial_sync_required': True,
            },
            create_fallback=True,
        )

        utils.create_backup_of_the_file(tap_state)
        start_time = datetime.now()
        try:
            target_params = TargetParams(
                target_id=target_id,
                type=target_type,
                bin=self.target_bin,
                python_bin=self.target_python_bin,
                config=cons_target_config,
            )

            transform_params = TransformParams(
                bin=self.transform_field_bin,
                python_bin=self.transform_field_python_bin,
                config=tap_transformation,
                tap_id=tap_id,
                target_id=target_id,
            )

            # Run fastsync for FULL_TABLE replication method
            if len(fastsync_stream_ids) > 0:
                self.logger.info(
                    'Table(s) selected to sync by fastsync/partialsync: %s', fastsync_stream_ids
                )
                self.sync_tables()

            else:
                self.logger.info(
                    'No table available that needs to be sync by fastsync'
                )

            # Run singer tap for INCREMENTAL and LOG_BASED replication methods
            if len(singer_stream_ids) > 0:
                with pidfile.PIDFile(self.tap['files']['pidfile']):
                    self.logger.info(
                        'Table(s) selected to sync by singer: %s', singer_stream_ids
                    )
                    self.tap_run_log_file = os.path.join(
                        log_dir, f'{target_id}-{tap_id}-{current_time}.singer.log'
                    )
                    tap_params = TapParams(
                        tap_id=tap_id,
                        type=tap_type,
                        bin=self.tap_bin,
                        python_bin=self.tap_python_bin,
                        config=tap_config,
                        properties=tap_properties_singer,
                        state=tap_state,
                    )

                    self.run_tap_singer(
                        tap=tap_params,
                        target=target_params,
                        transform=transform_params,
                        stream_buffer_size=stream_buffer_size,
                    )
            else:
                self.logger.info(
                    'No table available that needs to be sync by singer'
                )

        except pidfile.AlreadyRunningError:
            self.logger.error('Another instance of the tap is already running.')
            sys.exit(1)
        # Delete temp files if there is any
        except commands.RunCommandException as exc:
            self.logger.exception(exc)
            self._print_tap_run_summary(self.STATUS_FAILED, start_time, datetime.now())
            self.send_alert(message=f'{tap_id} tap failed', exc=exc)
            sys.exit(1)
        except Exception as exc:
            self._print_tap_run_summary(self.STATUS_FAILED, start_time, datetime.now())
            self.send_alert(message=f'{tap_id} tap failed', exc=exc)
            raise exc
        finally:
            utils.silentremove(cons_target_config)
            utils.silentremove(tap_properties_fastsync)
            utils.silentremove(tap_properties_singer)
        self._print_tap_run_summary(self.STATUS_SUCCESS, start_time, datetime.now())

    # pylint: disable=unused-argument
    def stop_tap(self, sig=None, frame=None):
        """
        Stop running tap

        The command finds the tap specific pidfile that was created by run_tap command and sends
        a SIGTERM to the process.
        """
        self.logger.info('Trying to stop tap gracefully...')
        pidfile_path = self.tap['files']['pidfile']
        try:
            with open(pidfile_path, encoding='utf-8') as pidf:
                pid = int(pidf.read())
                pgid = os.getpgid(pid)
                parent = psutil.Process(pid)

                # Terminate all the processes in the current process' process group.
                for child in parent.children(recursive=True):
                    if os.getpgid(child.pid) == pgid:
                        self.logger.info('Sending SIGTERM to child pid %s...', child.pid)
                        child.terminate()
                        try:
                            child.wait(timeout=5)
                        except psutil.TimeoutExpired:
                            child.kill()

        except ProcessLookupError:
            self.logger.error(
                'Pid %s not found. Is the tap running on this machine? '
                'Stopping taps remotely is not supported.',
                pid,
            )
            sys.exit(1)

        except FileNotFoundError:
            self.logger.error(
                'No pidfile found at %s. Tap does not seem to be running.', pidfile_path
            )
            sys.exit(1)

        # Remove pidfile.
        os.remove(pidfile_path)

        # Rename log files from running to terminated status
        if self.tap_run_log_file:
            tap_run_log_file_running = f'{self.tap_run_log_file}.running'
            tap_run_log_file_terminated = f'{self.tap_run_log_file}.terminated'

            if os.path.isfile(tap_run_log_file_running):
                os.rename(tap_run_log_file_running, tap_run_log_file_terminated)

        sys.exit(1)

    # pylint: disable=too-many-locals
    def sync_tables(self):
        """
        syncing tables by using fast sync
        """
        with pidfile.PIDFile(self.tap['files']['pidfile']):
            try:
                selected_tables = self._get_sync_tables_setting_from_selection_file(self.args.tables)
                processes_list = []
                if selected_tables['partial_sync']:
                    partial_sync_process = multiprocessing.Process(
                        target=self.sync_tables_partial_sync, args=(selected_tables['partial_sync'],))
                    partial_sync_process.start()
                    processes_list.append(partial_sync_process)

                if selected_tables['full_sync']:
                    fast_sync_process = multiprocessing.Process(
                        target=self.sync_tables_fast_sync, args=(selected_tables['full_sync'],))
                    fast_sync_process.start()
                    processes_list.append(fast_sync_process)

                for process in processes_list:
                    process.join()
            except pidfile.AlreadyRunningError:
                self.logger.error('Another instance of the tap is already running.')
                sys.exit(1)

    def sync_tables_fast_sync(self, selected_tables):
        """
        Sync every or a list of selected tables from a specific tap.
        It performs an initial sync and resets the table bookmarks to their new location.

        The function is using the fastsync components hence it's only
        available for taps and targets where the native and optimised
        fastsync component is implemented.
        """
        self.args.tables = ','.join(f'"{x}"' for x in selected_tables)
        tap_id = self.tap['id']
        tap_type = self.tap['type']
        target_id = self.target['id']
        target_type = self.target['type']
        fastsync_bin = utils.get_fastsync_bin(self.venv_dir, tap_type, target_type)

        self.logger.info(
            'Syncing tables from %s (%s) to %s (%s)...',
            tap_id,
            tap_type,
            target_id,
            target_type,
        )

        cons_target_config = None
        try:
            self._check_if_tap_is_enabled()

            self._check_if_complete_tap_configuration(fastsync_bin, tap_type, target_type)

            self._cleanup_tap_state_file()

            # Generate and run the command to run the tap directly
            tap_config = self.tap['files']['config']
            tap_inheritable_config = self.tap['files']['inheritable_config']
            tap_properties = self.tap['files']['properties']
            tap_state = self.tap['files']['state']
            tap_transformation = self.tap['files']['transformation']
            target_config = self.target['files']['config']

            # Set drop_pg_slot to True if we want to sync the whole tap
            # This flag will be used by FastSync PG to (PG/SF/Redshift)
            self.drop_pg_slot = bool(not self.args.tables)

            # Some target attributes can be passed and override by tap (aka. inheritable config)
            # We merge the two configs and use that with the target
            cons_target_config = self.create_consumable_target_config(
                target_config, tap_inheritable_config
            )

            # Output will be redirected into target and tap specific log directory
            log_dir = self.get_tap_log_dir(target_id, tap_id)
            current_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

            # sync_tables command always using fastsync
            self.tap_run_log_file = os.path.join(
                log_dir, f'{target_id}-{tap_id}-{current_time}.fastsync.log'
            )

            # Create parameters as NamedTuples
            tap_params = TapParams(
                tap_id=tap_id,
                type=tap_type,
                bin=self.tap_bin,
                python_bin=self.tap_python_bin,
                config=tap_config,
                properties=tap_properties,
                state=tap_state,
            )

            target_params = TargetParams(
                target_id=target_id,
                type=target_type,
                bin=self.target_bin,
                python_bin=self.target_python_bin,
                config=cons_target_config,
            )

            transform_params = TransformParams(
                bin=self.transform_field_bin,
                config=tap_transformation,
                python_bin=self.transform_field_python_bin,
                tap_id=tap_id,
                target_id=target_id,
            )

            self.run_tap_fastsync(
                tap=tap_params, target=target_params, transform=transform_params
            )

        except commands.RunCommandException as exc:
            self.logger.exception(exc)
            self.send_alert(message=f'Failed to sync tables in {tap_id} tap', exc=exc)
            sys.exit(1)
        except PreRunChecksException as exc:
            raise exc
        except Exception as exc:
            self.send_alert(message=f'Failed to sync tables in {tap_id} tap', exc=exc)
            raise exc
        finally:
            if cons_target_config:
                utils.silentremove(cons_target_config)

    def validate(self):
        """
        Validates a project directory with YAML tap and target files.
        """
        yaml_dir = self.args.dir
        self.logger.info('Searching YAML config files in %s', yaml_dir)
        tap_yamls, target_yamls = utils.get_tap_target_names(yaml_dir)

        self.logger.info('Detected taps: %s', tap_yamls)
        self.logger.info('Detected targets: %s', target_yamls)

        target_schema = utils.load_schema('target')
        tap_schema = utils.load_schema('tap')

        vault_secret = self.args.secret

        # dictionary of targets ID and type
        targets = {}

        # Validate target json schemas and that no duplicate IDs exist
        for yaml_file in target_yamls:
            self.logger.info('Started validating target file: %s', yaml_file)

            # pylint: disable=E1136  # False positive when loading vault encrypted YAML
            target_yml = utils.load_yaml(os.path.join(yaml_dir, yaml_file), vault_secret)
            utils.validate(target_yml, target_schema)

            if target_yml['id'] in targets:
                raise DuplicateConfigException(f'Duplicate target found "{target_yml["id"]}"')

            targets[target_yml['id']] = target_yml['type']

            self.logger.info('Finished validating target file: %s', yaml_file)

        tap_ids = set()

        # Validate tap json schemas, check that every tap has valid 'target' and that no duplicate IDs exist
        for yaml_file in tap_yamls:
            self.logger.info('Started validating %s ...', yaml_file)

            # pylint: disable=E1136  # False positive when loading vault encrypted YAML
            tap_yml = utils.load_yaml(os.path.join(yaml_dir, yaml_file), vault_secret)
            utils.validate(tap_yml, tap_schema)

            if tap_yml['id'] in tap_ids:
                raise DuplicateConfigException(f'Duplicate tap found "{tap_yml["id"]}"')

            if tap_yml['target'] not in targets:
                raise InvalidConfigException(
                    f"Can't find the target with the ID '{tap_yml['target']}' referenced in '{yaml_file}'."
                    f'Available target IDs: {list(targets.keys())}',
                    )

            tap_ids.add(tap_yml['id'])

            # If there is a fastsync component for this tap-target combo and transformations on json properties are
            # configured then fail the validation.
            # The reason being that at the time of writing this, transformations in Fastsync are done on the
            # target side using mostly SQL UPDATE, and transformations on properties in json fields are not
            # implemented due to the need of converting XPATH syntax to SQL which has been deemed as not worth it
            if self.__does_fastsync_component_exist(targets[tap_yml['target']], tap_yml['type']):
                self.logger.debug('FastSync component found for tap %s', tap_yml['id'])

                # Load the transformations
                transformations = Config.generate_transformations(tap_yml)

                # check if transformations are using "field_paths" or "field_path" config, fail if so
                for transformation in transformations:
                    if transformation.get('field_paths') is not None:
                        raise InvalidTransformationException(
                            'This tap-target combo has FastSync component and is configuring a transformation on json '
                            'properties which are not supported by FastSync!\n'
                            f'Please omit "field_paths" from the transformation config of tap "{tap_yml["id"]}"'
                        )

                    if transformation['when'] is not None:
                        for condition in transformation['when']:
                            if condition.get('field_path') is not None:
                                raise InvalidTransformationException(
                                    'This tap-target combo has FastSync component and is configuring a transformation '
                                    'conditions on json properties which are not supported by FastSync!\n'
                                    f'Please omit "field_path" from the transformation config of tap "{tap_yml["id"]}"'
                                )

            self.logger.info('Finished validating %s', yaml_file)

        self.logger.info('Validation successful')

    def import_project(self):
        """
        Take a list of YAML files from a directory and use it as the source to build
        singer compatible json files and organise them into pipeline directory structure
        """
        # Read the YAML config files and transform/save into singer compatible
        # JSON files in a common directory structure
        config = Config.from_yamls(self.config_dir, self.args.dir, self.args.secret)
        selected_taps_id = self.args.taps.split(',')
        config.save(selected_taps_id)

        # Activating tap stream selections
        #
        # Run every tap in discovery mode to generate the singer specific
        # properties.json files for the taps. The properties file than
        # updated to replicate only the tables that is defined in the YAML
        # files and to use the required replication methods
        #
        # The tap Discovery mode needs to connect to each source databases and
        # doing that sequentially is slow. For a better performance we do it
        # in parallel.
        self.logger.info('ACTIVATING TAP STREAM SELECTIONS...')
        total_targets = 0
        total_taps = 0
        discover_excs = []
        found_selected_taps = set()

        # Import every tap from every target
        start_time = datetime.now()
        for target in config.targets.values():
            total_targets += 1
            selected_taps = []

            if selected_taps_id == ['*']:
                total_taps += len(target.get('taps'))
                selected_taps = target.get('taps')
            else:
                for tap in target.get('taps'):
                    if tap['id'] in selected_taps_id:
                        selected_taps.append(tap)
                        found_selected_taps.add(tap['id'])

            with parallel_backend('threading', n_jobs=-1):
                # Discover taps in parallel and return the list of exception of the failed ones
                discover_excs.extend(
                    list(
                        filter(
                            None,
                            Parallel(verbose=100)(
                                delayed(self.discover_tap)(tap=tap, target=target)
                                for tap in selected_taps
                            ),
                        )
                    )
                )

        # Log summary

        if selected_taps_id != ['*']:
            total_taps = len(selected_taps_id)
            not_found_taps = set(selected_taps_id) - found_selected_taps
            for tap in not_found_taps:
                discover_excs.append(f'tap "{tap}" not found!')

        end_time = datetime.now()
        # pylint: disable=logging-too-many-args
        self.logger.info(
            """
            -------------------------------------------------------
            IMPORTING YAML CONFIGS FINISHED
            -------------------------------------------------------
                Total targets to import        : %s
                Total taps to import           : %s
                Taps imported successfully     : %s
                Taps failed to import          : %s
                Runtime                        : %s
            -------------------------------------------------------
            """,
            total_targets,
            total_taps,
            total_taps - len(discover_excs),
            str(discover_excs),
            end_time - start_time,
        )
        if len(discover_excs) > 0:
            sys.exit(1)

    def encrypt_string(self):
        """
        Encrypt the supplied string using the provided vault secret
        """
        b_ciphertext = utils.vault_encrypt(self.args.string, self.args.secret)
        yaml_text = utils.vault_format_ciphertext_yaml(b_ciphertext)

        print(yaml_text)
        print('Encryption successful')

    def partial_sync_table(self):
        """
        This method calls partial sync if partial_sync_table command is chosen
        """
        with pidfile.PIDFile(self.tap['files']['pidfile']):
            try:
                self.sync_tables_partial_sync()
            except pidfile.AlreadyRunningError as exc:
                self.logger.error('Another instance of the tap is already running.')
                raise SystemExit(1) from exc

    def sync_tables_partial_sync(self, defined_tables=None):
        """
        Partial Sync Tables
        """

        cons_target_config = None

        # Continue only if tap and target is supported by partial sync
        try:
            self._check_supporting_tap_and_target_for_partial_sync()

            tap_id = self.tap['id']
            tap_type = self.tap['type']
            target_id = self.target['id']
            target_type = self.target['type']
            sync_bin = utils.get_partialsync_bin(self.venv_dir, tap_type, target_type)

            self.logger.info(
                'Partial syncing table from %s (%s) to %s (%s)...',
                tap_id,
                tap_type,
                target_id,
                target_type,
            )

            self._check_if_tap_is_enabled()

            self._check_if_complete_tap_configuration(sync_bin, tap_type, target_type)

            if self.args.table != '*':
                self._validate_selected_table_and_column()
                self._check_if_state_exists()
                self.args.drop_target_table = None
            else:
                table_names = []
                table_columns = []
                table_values = []
                table_drop_targets = []
                for table, sync_settings in defined_tables.items():
                    table_names.append(table)
                    table_columns.append(sync_settings['column'])
                    table_values.append(sync_settings['value'])
                    table_drop_targets.append(sync_settings.get('drop_target_table'))

                self.args.table = ','.join(table_names)
                self.args.column = ','.join(table_columns)
                self.args.start_value = ','.join(table_values)
                self.args.drop_target_table = ','.join(map(str, table_drop_targets))

            # Generate and run the command to run the tap directly
            tap_config = self.tap['files']['config']
            tap_inheritable_config = self.tap['files']['inheritable_config']
            tap_properties = self.tap['files']['properties']
            tap_state = self.tap['files']['state']
            tap_transformation = self.tap['files']['transformation']
            target_config = self.target['files']['config']

            self.drop_pg_slot = False

            cons_target_config = self.create_consumable_target_config(
                target_config, tap_inheritable_config
            )

            # Output will be redirected into target and tap specific log directory
            log_dir = self.get_tap_log_dir(target_id, tap_id)
            current_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

            self.tap_run_log_file = os.path.join(
                log_dir, f'{target_id}-{tap_id}-{current_time}.partialsync.log'
            )

            # Create parameters as NamedTuples
            tap_params = TapParams(
                tap_id=tap_id,
                type=tap_type,
                bin=self.tap_bin,
                python_bin=self.tap_python_bin,
                config=tap_config,
                properties=tap_properties,
                state=tap_state,
            )

            target_params = TargetParams(
                target_id=target_id,
                type=target_type,
                bin=self.target_bin,
                python_bin=self.target_python_bin,
                config=cons_target_config,
            )

            transform_params = TransformParams(
                bin=self.transform_field_bin,
                config=tap_transformation,
                python_bin=self.transform_field_python_bin,
                tap_id=tap_id,
                target_id=target_id,
            )

            self.run_tap_partialsync(tap=tap_params, target=target_params, transform=transform_params)

        # Delete temp file if there is any
        except commands.RunCommandException as exc:
            self.logger.exception(exc)
            self.send_alert(message=f'Failed to sync tables in {tap_id} tap', exc=exc)
            raise SystemExit(1) from exc
        except PartialSyncNotSupportedTypeException as exc:
            self.logger.error(exc)
            raise SystemExit(1) from exc
        except PreRunChecksException as exp:
            raise SystemExit(1) from exp
        except Exception as exc:
            self.send_alert(message=f'Failed to sync tables in {tap_id} tap', exc=exc)
            raise exc
        finally:
            if cons_target_config:
                utils.silentremove(cons_target_config)

    def _check_supporting_tap_and_target_for_partial_sync(self):
        tap_type = self.tap['type']
        tap_id = self.tap['id']
        target_type = self.target['type']
        target_id = self.target['id']

        if ConnectorType(target_type) not in PARTIAL_SYNC_PAIRS.get(ConnectorType(tap_type), {}):
            raise PartialSyncNotSupportedTypeException(
                f'Error! {tap_id}({tap_type})-{target_id}({target_type}) pair is not supported for the partial sync!'
            )

    def _check_if_complete_tap_configuration(self, fastsync_bin, tap_type, target_type):
        # Tap exists but configuration not completed
        if not os.path.isfile(fastsync_bin):
            self.logger.error(
                'Table sync function is not implemented from %s datasources to %s type of targets',
                tap_type,
                target_type
            )
            raise SystemExit(1)

    def _check_if_tap_is_enabled(self):
        # Run only if tap enabled
        if not self.tap.get('enabled', False):
            self.logger.info('Tap %s is not enabled.', self.tap['name'])
            raise PreRunChecksException()

    def _check_if_state_exists(self):
        state_file = self.tap['files']['state']
        if os.path.exists(state_file):
            return
        self.logger.error('Could not find state file in "%s"!', state_file)
        raise PreRunChecksException()

    def _validate_selected_table_and_column(self):
        properties = utils.load_json(self.tap['files']['properties'])

        # because self.args.table is in this format <database>.<table_name>
        table_name = self.args.table.split('.')[-1]

        streams = properties['streams']

        table_in_properties = next(
            (item for item in streams if item['table_name'] == table_name), None
        )
        if table_in_properties is None:
            self.logger.error('Not found table "%s" in properties!', self.args.table)
            raise PreRunChecksException()

        self.__check_if_table_is_selected(table_in_properties)

        try:
            column_type = table_in_properties['schema']['properties'][self.args.column]['type']
            if 'boolean' in column_type:
                self.logger.error('column "%s" has invalid type for partial sync!', self.args.column)
                raise PreRunChecksException('Invalid type for partial sync!')
        except KeyError as exp:
            self.logger.error('Not found column "%s" in properties!', self.args.column)
            raise PreRunChecksException() from exp

    def _is_initial_sync_required(
        self, replication_method: str, stream_bookmark: Dict
    ) -> bool:
        """
            Detects if a stream needs initial sync or not.
            Initial sync is required for INCREMENTAL and LOG_BASED tables
            where the state file has no valid bookmark.

            Valid bookmark keys:
              'replication_key_value' key created for INCREMENTAL tables
              'log_pos' key created by MySQL LOG_BASED tables
              'lsn' key created by PostgreSQL LOG_BASED tables
              'modified_since' key created by CSV S3 INCREMENTAL tables
              'token' key created by MongoDB LOG_BASED tables

            FULL_TABLE replication method is taken as initial sync required
        :param replication_method: stream replication method
        :param stream_bookmark: stream state bookmark
        :return: Boolean, True if needs initial sync, False otherwise
        """
        return (
            replication_method == self.FULL_TABLE
            or (
                replication_method == self.INCREMENTAL
                and 'replication_key_value' not in stream_bookmark
                and 'modified_since' not in stream_bookmark
            )
            or (
                replication_method == self.LOG_BASED
                and 'lsn' not in stream_bookmark
                and 'log_file' not in stream_bookmark
                and 'log_pos' not in stream_bookmark
                and 'gtid' not in stream_bookmark
                and 'token' not in stream_bookmark
            )
        )

    def _print_tap_run_summary(self, status, start_time, end_time):
        summary = f"""
-------------------------------------------------------
TAP RUN SUMMARY
-------------------------------------------------------
    Status  : {status}
    Runtime : {end_time - start_time}
-------------------------------------------------------
"""

        # Print summary to stdout
        self.logger.info(summary)

        # Add summary to tap run log file
        if self.tap_run_log_file:
            tap_run_log_file_success = f'{self.tap_run_log_file}.success'
            tap_run_log_file_failed = f'{self.tap_run_log_file}.failed'

            # Find which log file we need to write the summary
            log_file_to_write_summary = None
            if os.path.isfile(tap_run_log_file_success):
                log_file_to_write_summary = tap_run_log_file_success
            elif os.path.isfile(tap_run_log_file_failed):
                log_file_to_write_summary = tap_run_log_file_failed

            # Append the summary to the right log file
            if log_file_to_write_summary:
                with open(log_file_to_write_summary, 'a', encoding='utf-8') as logfile:
                    logfile.write(summary)

    # pylint: disable=unused-variable
    def _run_post_import_tap_checks(
        self, tap: Dict, catalog: Dict, target_id: str
    ) -> List:
        """
        Run post import checks on a tap.

        :param tap: dictionary containing all taps details
        :param catalog: tap properties object
        :param target_id: ID of the target used by the tap
        :return: List of errors. If no error returns an empty list
        """
        errors = []

        error = self.__validate_transformations(
            tap.get('files', {}).get('transformation'), catalog, tap['id'], target_id
        )

        if error:
            errors.append(error)

        # Foreach stream (table) in the original properties
        for stream_idx, stream in enumerate(catalog.get('streams', catalog)):
            # Collect required properties from the properties file
            tap_stream_id = stream.get('tap_stream_id')
            metadata = stream.get('metadata', [])

            # Collect further properties from the tap and target properties
            table_meta = {}
            for meta_idx, meta in enumerate(metadata):
                if isinstance(meta, dict) and len(meta.get('breadcrumb', [])) == 0:
                    table_meta = meta.get('metadata')
                    break

            selected = table_meta.get('selected', False)
            replication_method = table_meta.get('replication-method')
            table_key_properties = table_meta.get('table-key-properties', [])
            primary_key_required = tap.get('primary_key_required', True)

            # Check if primary key is set for INCREMENTAL and LOG_BASED replications
            if (
                selected
                and replication_method in [self.INCREMENTAL, self.LOG_BASED]
                and len(table_key_properties) == 0
                and primary_key_required
            ):
                errors.append(
                    f'No primary key set for {tap_stream_id} stream ({replication_method})'
                )
                break

        return errors

    def _cleanup_tap_state_file(self) -> None:
        tables = self.args.tables
        state_file = self.tap['files']['state']
        if tables:
            self._clean_tables_from_bookmarks_in_state_file(state_file, tables)

    @staticmethod
    def _clean_tables_from_bookmarks_in_state_file(state_file_to_clean: str, tables: str) -> None:
        try:
            with open(state_file_to_clean, 'r+', encoding='UTF-8') as state_file:
                state_data = json.load(state_file)
                bookmarks = state_data.get('bookmarks')
                list_of_tables = tables.split(',')
                if bookmarks:
                    for table_name in list_of_tables:
                        bookmarks.pop(table_name.replace('"', ''), None)

                state_file.seek(0)
                json.dump(state_data, state_file)
                state_file.truncate()

        except FileNotFoundError:
            pass
        except json.JSONDecodeError:
            pass

    @staticmethod
    def _get_fixed_name_of_table(stream_id):
        return stream_id.replace('-', '.', 1)

    def _get_sync_tables_setting_from_selection_file(self, tables):
        selection = utils.load_json(self.tap['files']['selection'])
        selection = selection.get('selection')
        all_tables = {'full_sync': [], 'partial_sync': {}}
        if selection:
            for table in selection:
                table_name = self._get_fixed_name_of_table(table['tap_stream_id'])
                if tables is None or table_name in tables:
                    if table.get('sync_start_from'):
                        all_tables['partial_sync'][table_name] = table['sync_start_from']
                    else:
                        all_tables['full_sync'].append(table_name)
            return all_tables

    def _do_not_run_if_another_instance_is_running(self, sync_method):
        log_dir = os.path.dirname(self.tap_run_log_file)
        log_patterns = [f'*.{sync_method}.log.running', '*.singer.log.running']
        if (
                os.path.isdir(log_dir)
                and len(utils.search_files(log_dir, patterns=log_patterns)) > 0
        ):
            self.logger.info(
                'Failed to run. Another instance of the same tap is already running. '
                'Log file detected in running status at %s',
                log_dir,
            )
            sys.exit(1)

    def __check_if_table_is_selected(self, table_in_properties):
        table_metadata = table_in_properties.get('metadata', [])
        for metadata in table_metadata:
            metadata_properties = metadata.get('metadata', {})
            selected = metadata_properties.get('selected')
            if selected is True:   # pylint: disable=no-else-return
                return
            elif selected is False:
                break

        self.logger.error('table "%s" is not selected in properties!', self.args.table)
        raise PreRunChecksException()

    def __validate_transformations(
        self, transformation_file: str, catalog: Dict, tap_id: str, target_id: str
    ) -> Optional[str]:
        """
        Run validation of transformation config
        Args:
            transformation_file: path to transformation config
            catalog: Catalog object
            tap_id: The ID of the tap to which the transformations belong
            target_id: the ID of the target used by the tap

        Returns: error as string
        """
        if transformation_file:

            # create a temp file with the content being the given catalog object
            # we need this file to execute the validation cli command
            temp_catalog_file = utils.create_temp_file(
                dir=self.get_temp_dir(), prefix='properties_', suffix='.json'
            )[1]

            utils.save_json(catalog, temp_catalog_file)

            command = f"""
                {self.transform_field_bin} --validate --config {transformation_file} --catalog {temp_catalog_file}
                """

            if self.profiling_mode:
                dump_file = os.path.join(
                    self.profiling_dir, f'transformation_{tap_id}_{target_id}.pstat'
                )
                command = f'{self.transform_field_python_bin} -m cProfile -o {dump_file} {command}'

            self.logger.debug('Transformation validation command: %s', command)

            result = commands.run_command(command)

            # Get output and errors from command
            returncode, _, stderr = result

            if returncode != 0:
                return stderr

    @classmethod
    def __does_fastsync_component_exist(cls, target_type: str, tap_type: str) -> bool:
        """
        Checks if the given tap-target combo have FastSync
        Args:
            target_type: type of the target
            tap_type: type of tap

        Returns:
            Boolean, True if FastSync exists, False otherwise.
        """
        return ConnectorType(target_type) in FASTSYNC_PAIRS.get(ConnectorType(tap_type), {})
