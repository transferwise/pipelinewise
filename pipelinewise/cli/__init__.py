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

from .utils import generate_random_string
from .pipelinewise import PipelineWise
from ..logger import Logger

__version__ = get_distribution('pipelinewise').version
USER_HOME = os.path.expanduser('~')
CONFIG_DIR = os.path.join(USER_HOME, '.pipelinewise')
PROFILING_DIR = os.path.join(CONFIG_DIR, 'profiling')
PIPELINEWISE_DEFAULT_HOME = os.path.join(USER_HOME, 'pipelinewise')
PIPELINEWISE_HOME = os.path.abspath(os.environ.setdefault('PIPELINEWISE_HOME', PIPELINEWISE_DEFAULT_HOME))
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


def __init_profiler(profiler_arg: bool, logger: logging.Logger) -> Tuple[Optional[Profile], Optional[str]]:
    """
    Initialise profiling environment by creating a cprofile.Profiler instance, a folder where pstats can be dumped
    Args:
        profiler_arg:
        logger:

    Returns:

    """
    if profiler_arg:
        logger.info('Profiling mode enabled')

        logger.debug('Creating & enabling profiler ...')

        profiler = Profile()
        profiler.enable()

        logger.debug('Profiler created.')

        profiling_dir = os.path.join(PROFILING_DIR,
                                     f'{datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")}_{generate_random_string(10)}'
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


def __disable_profiler(profiler: Optional[Profile],
                       profiling_dir: Optional[str],
                       pstat_filename: Optional[str],
                       logger: logging.Logger):
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
    parser.add_argument('--tables', type=str, help='List of tables to sync')
    parser.add_argument('--dir', type=str, default='*', help='Path to directory with config')
    parser.add_argument('--name', type=str, default='*', help='Name of the project')
    parser.add_argument('--secret', type=str, help='Path to vault password file')
    parser.add_argument('--string', type=str)
    parser.add_argument('--version',
                        action='version',
                        help='Displays the installed versions',
                        version='PipelineWise {} - Command Line Interface'.format(__version__))
    parser.add_argument('--log', type=str, default='*', help='File to log into')
    parser.add_argument('--extra_log',
                        default=False,
                        required=False,
                        help='Copy singer and fastsync logging into PipelineWise logger',
                        action='store_true')
    parser.add_argument('--debug',
                        default=False,
                        required=False,
                        help='Forces the debug mode with logging on stdout and log level debug',
                        action='store_true')
    parser.add_argument('--profiler', '-p',
                        default=False,
                        required=False,
                        help='Enables code profiling mode using Python builtin profiler cProfile. '
                             'The stats will be dumped into a folder in .pipelinewise/profiling',
                        action='store_true'
                        )

    args = parser.parse_args()

    # Command specific argument validations
    if args.command == 'init' and args.name == '*':
        print('You must specify a project name using the argument --name')
        sys.exit(1)

    if args.command in ['discover_tap', 'test_tap_connection', 'run_tap', 'stop_tap']:
        if args.tap == '*':
            print('You must specify a source name using the argument --tap')
            sys.exit(1)
        if args.target == '*':
            print('You must specify a destination name using the argument --target')
            sys.exit(1)

    if args.command == 'sync_tables':
        if args.tap == '*':
            print('You must specify a source name using the argument --tap')
            sys.exit(1)
        if args.target == '*':
            print('You must specify a destination name using the argument --target')
            sys.exit(1)

    # import and import_config commands are synonyms
    #
    # import        : short CLI command name to import project
    # import_config : this is for backward compatibility; use 'import' instead from CLI
    if args.command == 'import' or args.command == 'import_config':
        if args.dir == '*':
            print('You must specify a directory path with config YAML files using the argument --dir')
            sys.exit(1)

        # Every command argument is mapped to a python function with the same name, but 'import' is a
        # python keyword and can't be used as function name
        args.command = 'import_project'

    if args.command == 'validate' and args.dir == '*':
        print('You must specify a directory path with config YAML files using the argument --dir')
        sys.exit(1)

    if args.command == 'encrypt_string':
        if not args.secret:
            print('You must specify a path to a file with vault secret using the argument --secret')
            sys.exit(1)
        if not args.string:
            print('You must specify a string to encrypt using the argument --string')
            sys.exit(1)

    logger = __init_logger(args.log, args.debug)

    profiler, profiling_dir = __init_profiler(args.profiler, logger)

    ppw_instance = PipelineWise(args, CONFIG_DIR, VENV_DIR, profiling_dir)

    try:
        getattr(ppw_instance, args.command)()
    finally:
        __disable_profiler(profiler, profiling_dir, f'pipelinewise_{args.command}', logger)


if __name__ == '__main__':
    main()
