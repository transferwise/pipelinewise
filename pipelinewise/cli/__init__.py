"""
PipelineWise CLI
"""
import argparse
import os
import sys

from pkg_resources import get_distribution

from .pipelinewise import PipelineWise

__version__ = get_distribution('pipelinewise').version
USER_HOME = os.path.expanduser('~')
CONFIG_DIR = os.path.join(USER_HOME, '.pipelinewise')
PIPELINEWISE_DEFAULT_HOME = os.path.join(USER_HOME, 'pipelinewise')
PIPELINEWISE_HOME = os.path.abspath(os.environ.setdefault('PIPELINEWISE_HOME', PIPELINEWISE_DEFAULT_HOME))
VENV_DIR = os.path.join(PIPELINEWISE_HOME, '.virtualenvs')
COMMANDS = [
    'init',
    'run_tap',
    'discover_tap',
    'status',
    'test_tap_connection',
    'sync_tables',
    'import',
    'import_config',  # This is for backward compatibility; use 'import' instead
    'validate',
    'encrypt_string',
]


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
    parser.add_argument(
        '--version',
        action='version',
        help='Displays the installed versions',
        version='PipelineWise {} - Command Line Interface'.format(__version__),
    )
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

    args = parser.parse_args()

    # Command specific argument validations
    if args.command == 'init':
        if args.name == '*':
            print('You must specify a project name using the argument --name')
            sys.exit(1)

    if args.command == 'discover_tap' or args.command == 'test_tap_connection' or args.command == 'run_tap':
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

    if args.command == 'validate':
        if args.dir == '*':
            print('You must specify a directory path with config YAML files using the argument --dir')
            sys.exit(1)

    if args.command == 'encrypt_string':
        if not args.secret:
            print('You must specify a path to a file with vault secret using the argument --secret')
            sys.exit(1)
        if not args.string:
            print('You must specify a string to encrypt using the argument --string')
            sys.exit(1)

    pipelinewise = PipelineWise(args, CONFIG_DIR, VENV_DIR)
    getattr(pipelinewise, args.command)()


if __name__ == '__main__':
    main()
