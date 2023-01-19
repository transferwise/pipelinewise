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
from pkg_resources import get_distribution

from pipelinewise.cli.utils import generate_random_string
from pipelinewise.cli.pipelinewise import PipelineWise
from pipelinewise.logger import Logger
from pipelinewise.cli.errors import CommandSpecificArgumentsException

__version__ = get_distribution('pipelinewise').version
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
    'sync_tables',
    'import',
    'import_config',  # This is for backward compatibility; use 'import' instead
    'validate',
    'encrypt_string',
    'partial_sync_table',
    'db_upgrade',
    'scheduler'
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


# pylint: disable=too-many-branches
def _validate_command_specific_arguments(args):
    # Command specific argument validations
    if args.command == 'init' and args.name == '*':
        raise CommandSpecificArgumentsException('You must specify a project name using the argument --name')

    if args.command in ['discover_tap', 'test_tap_connection', 'run_tap', 'stop_tap', 'sync_tables']:
        if args.tap == '*':
            raise CommandSpecificArgumentsException('You must specify a source name using the argument --tap')
        if args.target == '*':
            raise CommandSpecificArgumentsException('You must specify a destination name using the argument --target')

    if args.command == 'import_config':
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
    parser.add_argument('--target', type=str, default='*', help='"Name of the target')
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
    parser.add_argument('--table', type=str, default='*', help='Name of the table to partial sync')
    parser.add_argument('--column', type=str, default='*', help='Name of the column to use as sync key in partial sync')
    parser.add_argument('--start_value', type=str, default='*', help='start value of the column to partial sync')
    parser.add_argument('--end_value', type=str, default=None, help='end value of the column to partial sync')

    args = parser.parse_args()

    # import and import_config commands are synonyms
    #
    # import        : short CLI command name to import project
    # import_config : this is for backward compatibility; use 'import' instead from CLI
    # Every command argument is mapped to a python function with the same name, but 'import' is a
    # python keyword and can't be used as function name
    if args.command == 'import' or args.command == 'import_config':
        args.command = 'import_project'
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
