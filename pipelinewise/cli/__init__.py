"""
PipelineWise CLI
"""
import argparse
import errno
import os
import sys
import copy
import logging

from cProfile import Profile
from datetime import datetime
from typing import Optional, Tuple
from importlib.metadata import version

from pipelinewise.cli.utils import generate_random_string
from pipelinewise.cli.pipelinewise import PipelineWise
from pipelinewise.logger import Logger
from pipelinewise.cli.errors import CommandSpecificArgumentsException

__version__ = version('pipelinewise')
USER_HOME = os.path.expanduser('~')
DEFAULT_CONFIG_DIR = os.path.join(USER_HOME, '.pipelinewise')
CONFIG_DIR = os.environ.get('PIPELINEWISE_CONFIG_DIRECTORY', DEFAULT_CONFIG_DIR)
PROFILING_DIR = os.path.join(CONFIG_DIR, 'profiling')
PIPELINEWISE_DEFAULT_HOME = os.path.join(USER_HOME, 'pipelinewise')
PIPELINEWISE_HOME = os.path.abspath(
    os.environ.setdefault('PIPELINEWISE_HOME', PIPELINEWISE_DEFAULT_HOME)
)
VENV_DIR = os.path.join(PIPELINEWISE_HOME, '.virtualenvs')
COMMANDS = [
    'init',
    'run_tap',
    'stop_tap',
    'discover_tap',
    'status',
    'test_tap_connection',
    'fast_sync',
    'sync_tables',  # backward-compatible alias for fast_sync
    'import_config',
    'import',  # Backward-compatible alias; use 'import_config' instead
    'validate',
    'encrypt_string',
    'partial_sync_table',
    'copy_native_to_iceberg',
    'reset_state',
    'list_data_diff_checks',
    'run_data_diff_checks',
    'rerun_data_diff_check',
]


def __init_logger(log_file=None, debug=False):
    """
    Initialise logger and update its handlers and level accordingly
    """
    # get logger for pipelinewise
    logger = Logger(debug).get_logger('pipelinewise')

    # copy log configuration: level and formatter
    level = logger.level
    formatter = copy.deepcopy(logger.handlers[0].formatter)

    # Create log file handler if required
    if log_file and log_file != '*':
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

    return logger


def __init_profiler(
    profiler_arg: bool, logger: logging.Logger
) -> Tuple[Optional[Profile], Optional[str]]:
    """
    Initialise profiling environment by creating a cprofile.Profiler instance, a folder where pstats can be dumped
    Args:
        profiler_arg: the value of profiler argument passed when running the command
        logger: a logger instance

    Returns:
        If profiling enabled, a tuple of profiler instance and profiling directory where the stats files
        would be dumped, otherwise, a tuple of nulls
    """
    if profiler_arg:
        logger.info('Profiling mode enabled')

        logger.debug('Creating & enabling profiler ...')

        profiler = Profile()
        profiler.enable()

        logger.debug('Profiler created.')

        profiling_dir = os.path.join(
            PROFILING_DIR,
            f'{datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")}_{generate_random_string(10)}',
        )

        try:
            os.makedirs(profiling_dir)
            logger.debug('Profiling directory "%s" created', profiling_dir)

        except OSError as ex:
            if ex.errno != errno.EEXIST:
                raise

            logger.debug('Profiling directory "%s" already exists', profiling_dir)

        return profiler, profiling_dir

    logger.info('Profiling mode not enabled')

    return None, None


def __disable_profiler(
    profiler: Optional[Profile],
    profiling_dir: Optional[str],
    pstat_filename: Optional[str],
    logger: logging.Logger,
):
    """
    Disable given profiler and dump pipelinewise stats into a pStat file
    Args:
        profiler: optional instance of cprofile.Profiler to disable
        profiling_dir: profiling dir where pstat file will be created
        pstat_filename: custom pstats file name, the extension .pstat will be appended to the name
        logger: Logger instance to do some info and debug logging
    """
    if profiler is not None:
        logger.debug('disabling profiler and dumping stats...')

        profiler.disable()

        if not pstat_filename.endswith('.pstat'):
            pstat_filename = f'{pstat_filename}.pstat'

        dump_file = os.path.join(profiling_dir, pstat_filename)

        logger.debug('Attempting to dump profiling stats in file "%s" ...', dump_file)
        profiler.dump_stats(dump_file)
        logger.debug('Profiling stats dump successful')

        logger.info('Profiling stats files are in folder "%s"', profiling_dir)

        profiler.clear()


def _validate_command_specific_arguments(args):
    # Command specific argument validations
    if args.command == 'init' and args.name == '*':
        raise CommandSpecificArgumentsException('You must specify a project name using the argument --name')

    if args.command in [
        'discover_tap', 'test_tap_connection', 'run_tap', 'stop_tap',
        'fast_sync', 'sync_tables', 'reset_state',
    ]:
        if args.tap == '*':
            raise CommandSpecificArgumentsException('You must specify a source name using the argument --tap')
        if args.target == '*':
            raise CommandSpecificArgumentsException('You must specify a destination name using the argument --target')

    if args.command in ['import_config', 'import_project']:
        if args.dir == '*':
            raise CommandSpecificArgumentsException(
                'You must specify a directory path with config YAML files using the argument --dir'
            )

    if args.command == 'validate' and args.dir == '*':
        raise CommandSpecificArgumentsException(
            'You must specify a directory path with config YAML files using the argument --dir'
        )

    if args.command == 'encrypt_string':
        if not args.secret:
            raise CommandSpecificArgumentsException(
                'You must specify a path to a file with vault secret using the argument --secret'
            )
        if not args.string:
            raise CommandSpecificArgumentsException('You must specify a string to encrypt using the argument --string')

    if args.command == 'partial_sync_table':
        _validate_partial_sync_arguments(args)

    if args.command == 'copy_native_to_iceberg':
        _validate_iceberg_copy_arguments(args)

    _validate_data_diff_arguments(args)


def _validate_iceberg_copy_arguments(args):
    """Require one explicit Snowflake destination, table, and Iceberg version."""
    if (
        not isinstance(args.target, str)
        or not args.target.strip()
        or args.target == '*'
    ):
        raise CommandSpecificArgumentsException(
            'You must specify a destination name using the argument --target'
        )
    if (
        not isinstance(args.table, str)
        or not args.table.strip()
        or args.table == '*'
    ):
        raise CommandSpecificArgumentsException(
            'You must specify a fully qualified Snowflake table using the argument --table'
        )
    if (
        not isinstance(args.iceberg_version, int)
        or isinstance(args.iceberg_version, bool)
        or args.iceberg_version != 3
    ):
        raise CommandSpecificArgumentsException(
            'You must explicitly specify Iceberg version 3 using --iceberg-version 3'
        )


def _validate_data_diff_arguments(args):
    """Validate filters and remediation evidence."""
    if args.command in [
        'list_data_diff_checks',
        'run_data_diff_checks',
    ]:
        if args.tap != '*' and args.target == '*':
            raise CommandSpecificArgumentsException(
                'You must specify --target when filtering data-diff checks by --tap'
            )
        if args.command == 'run_data_diff_checks':
            if not getattr(args, 'all', False) and args.target == '*' and args.tap == '*':
                raise CommandSpecificArgumentsException(
                    'You must specify --target and --tap, or use --all to run all definitions'
                )

    if args.command == 'rerun_data_diff_check':
        if not args.run_id:
            raise CommandSpecificArgumentsException(
                'You must specify the failed data-diff run using --run-id'
            )
        if not args.remediation_ref:
            raise CommandSpecificArgumentsException(
                'You must specify the repair or incident reference using '
                '--remediation-ref'
            )


def _validate_partial_sync_arguments(args):
    """Validating specific arguments for partial sync"""
    if args.tap == '*':
        raise CommandSpecificArgumentsException('You must specify a source name using the argument --tap')

    if args.target == '*':
        raise CommandSpecificArgumentsException('You must specify a destination name using the argument --target')

    if args.table == '*':
        raise CommandSpecificArgumentsException('You must specify a source table by using the argument --table')

    if args.column == '*':
        raise CommandSpecificArgumentsException('You must specify a column by using the argument --column')

    if args.start_value == '*':
        raise CommandSpecificArgumentsException(
            'You must specify a start value by using the argument --start_value')


# pylint: disable=too-many-branches,too-many-statements
def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='PipelineWise {} - Command Line Interface'.format(__version__),
        add_help=True,
    )
    parser.add_argument('command', type=str, choices=COMMANDS)
    parser.add_argument('--target', type=str, default='*', help='Name of the target')
    parser.add_argument('--tap', type=str, default='*', help='Name of the tap')
    parser.add_argument('--taps', type=str, default='*', help='Comma separated list of tap IDs to import')
    parser.add_argument('--tables', type=str, help='List of tables to sync')
    parser.add_argument(
        '--dir', type=str, default='*', help='Path to directory with config'
    )
    parser.add_argument('--name', type=str, default='*', help='Name of the project')
    parser.add_argument('--secret', type=str, help='Path to vault password file')
    parser.add_argument('--string', type=str)
    parser.add_argument(
        '--version',
        action='version',
        help='Displays the installed versions',
        version='PipelineWise {} - Command Line Interface'.format(__version__),
    )
    parser.add_argument('--log', type=str, default='*', help='File to log into')
    parser.add_argument(
        '--extra_log',
        default=False,
        required=False,
        help='Copy singer and fastsync logging into PipelineWise logger',
        action='store_true',
    )
    parser.add_argument(
        '--debug',
        default=False,
        required=False,
        help='Forces the debug mode with logging on stdout and log level debug',
        action='store_true',
    )
    parser.add_argument(
        '--profiler',
        '-p',
        default=False,
        required=False,
        help='Enables code profiling mode using Python builtin profiler cProfile. '
        'The stats will be dumped into a folder in .pipelinewise/profiling',
        action='store_true',
    )
    parser.add_argument(
        '--table',
        type=str,
        default='*',
        help='Table name for commands that operate on one table',
    )
    parser.add_argument(
        '--eventual',
        choices=['native', 'iceberg'],
        default='native',
        help=(
            'Final format after native-to-Iceberg conversion; iceberg requires '
            'a controlled reader-and-writer outage'
        ),
    )
    parser.add_argument(
        '--iceberg-version',
        type=int,
        default=None,
        help='Managed Iceberg version for native-to-Iceberg conversion',
    )
    parser.add_argument('--column', type=str, default='*', help='Name of the column to use as sync key in partial sync')
    parser.add_argument('--start_value', type=str, default='*', help='Start value of the column to partial sync')
    parser.add_argument('--end_value', type=str, default=None, help='End value of the column to partial sync')
    parser.add_argument('--force', default=False, required=False,
                        help='Force fast_sync or a completed data-diff slot',
                        action='store_true'
                        )
    parser.add_argument('--replication_method_only', default='*', type=str,
                        help='Sync only tables which their replication method is as entered value')
    parser.add_argument('--check', type=str, default=None,
                        help='Data-diff check name, logical key, or version ID')
    parser.add_argument('--output-format', choices=['table', 'json'], default='table',
                        help='Output format for list commands')
    parser.add_argument('--include-versioned', default=False, action='store_true',
                        help='Include superseded definition revisions')
    parser.add_argument('--run-id', default=None,
                        help='Failed data-diff run ID to reproduce for remediation')
    parser.add_argument('--remediation-ref', default=None,
                        help='Ticket, incident, or change reference for a remediation rerun')
    parser.add_argument('--all', default=False, required=False,
                        help='Run data-diff checks for all definitions across all taps and targets',
                        action='store_true')
    args = parser.parse_args()

    # import_config and import commands are synonyms
    #
    # import_config : canonical CLI command for importing a project
    # import        : deprecated alias retained for backward compatibility
    # Both command names map to import_project because 'import' is a Python keyword.
    if args.command in ['import_config', 'import']:
        args.command = 'import_project'

    # fast_sync and sync_tables are synonyms
    # sync_tables is kept for backward compatibility; use 'fast_sync' instead
    if args.command in ['fast_sync', 'sync_tables']:
        args.command = 'fast_sync'
    try:
        _validate_command_specific_arguments(args)
    except CommandSpecificArgumentsException as exp:
        print(str(exp))
        sys.exit(1)

    logger = __init_logger(args.log, args.debug)

    profiler, profiling_dir = __init_profiler(args.profiler, logger)

    ppw_instance = PipelineWise(args, CONFIG_DIR, VENV_DIR, profiling_dir)

    try:
        getattr(ppw_instance, args.command)()
    finally:
        __disable_profiler(
            profiler, profiling_dir, f'pipelinewise_{args.command}', logger
        )


if __name__ == '__main__':
    main()
