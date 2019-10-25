#!/usr/bin/env python3
import os
import sys
from pathlib import Path

from pkg_resources import get_distribution
import argparse

from .pipelinewise import PipelineWise

__version__ = get_distribution('pipelinewise').version
user_home = os.path.expanduser('~')
config_dir = os.path.join(user_home, '.pipelinewise')
pipelinewise_default_home = os.path.join(user_home, 'pipelinewise')
pipelinewise_home = os.path.abspath(os.environ.setdefault("PIPELINEWISE_HOME", pipelinewise_default_home))
venv_dir = os.path.join(pipelinewise_home, '.virtualenvs')

commands = [
  'init',
  'run_tap',
  'discover_tap',
  'status',
  'test_tap_connection',
  'sync_tables',
  'import',
  'import_config',  # This is for backward compatibility; use 'import' instead
  'encrypt_string'
]

target_help = """Name of the target"""
tap_help = """Name of the tap"""
tables_help = """List of tables to sync"""
dir_help = """Path to directory with config"""
name_help = """Name of the project"""
secret_help = """Path to vault password file"""
version_help = """Displays the installed versions"""
log_help = """File to log into"""
extra_log_help = """Copy singer and fastsync logging into PipelineWise logger"""
debug_help = """Forces the debug mode with logging on stdout and log level debug."""

def main():
    '''Main entry point'''
    parser = argparse.ArgumentParser(description='PipelineWise {} - Command Line Interface'.format(__version__), add_help=True)
    parser.add_argument('command', type=str, choices=commands)
    parser.add_argument('--target', type=str, default='*', help=target_help)
    parser.add_argument('--tap', type=str, default='*', help=tap_help)
    parser.add_argument('--tables', type=str, help=tables_help)
    parser.add_argument('--dir', type=str, default='*', help=dir_help)
    parser.add_argument('--name', type=str, default='*', help=name_help)
    parser.add_argument('--secret', type=str, help=secret_help)
    parser.add_argument('--string', type=str)
    parser.add_argument('--version', action="version", help=version_help, version='PipelineWise {} - Command Line Interface'.format(__version__))
    parser.add_argument('--log', type=str, default='*', help=log_help)
    parser.add_argument('--extra_log', default=False, required=False, help=extra_log_help, action="store_true")
    parser.add_argument('--debug', default=False, required=False, help=debug_help, action="store_true")

    args = parser.parse_args()

    # Command specific argument validations
    if args.command == 'init':
        if args.name == '*':
            print("You must specify a project name using the argument --name")
            sys.exit(1)

    if args.command == 'discover_tap' or args.command == 'test_tap_connection' or args.command == 'run_tap':
        if args.tap == '*':
            print("You must specify a source name using the argument --tap")
            sys.exit(1)
        if args.target == '*':
            print("You must specify a destination name using the argument --target")
            sys.exit(1)

    if args.command == 'sync_tables':
        if args.tap == '*':
            print("You must specify a source name using the argument --tap")
            sys.exit(1)
        if args.target == '*':
            print("You must specify a destination name using the argument --target")
            sys.exit(1)

    # import and import_config commands are synonyms
    #
    # import        : short CLI command name to import project
    # import_config : this is for backward compatibility; use 'import' instead from CLI
    if args.command == 'import' or args.command == 'import_config':
        if args.dir == '*':
            print("You must specify a directory path with config YAML files using the argumant --dir")
            sys.exit(1)

        # Every command argument is mapped to a python function with the same name, but 'import' is a
        # python keyword and can't be used as function name
        args.command = 'import_project'

    if args.command == 'encrypt_string':
        if not args.secret:
            print("You must specify a path to a file with vault secret using the argument --secret")
            sys.exit(1)
        if not args.string:
            print("You must specify a string to encrypt using the argument --string")
            sys.exit(1)

    pipelinewise = PipelineWise(args, config_dir, venv_dir)
    getattr(pipelinewise, args.command)()

if __name__ == '__main__':
    main()
