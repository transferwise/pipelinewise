#!/usr/bin/env python3
import os
import sys
from pathlib import Path

from pkg_resources import get_distribution
from cli.pipelinewise import PipelineWise
import argparse

__version__ = get_distribution('cli').version
user_home = os.path.expanduser('~')
config_dir = os.path.join(user_home, '.pipelinewise')
pipelinewise_default_home = os.path.join(user_home, 'pipelinewise')
pipelinewise_home = os.path.abspath(os.environ.setdefault("PIPELINEWISE_HOME", pipelinewise_default_home))
venv_dir = os.path.join(pipelinewise_home, '.virtualenvs')

commands = [
  'run_tap',
  'discover_tap',
  'status',
  'test_tap_connection',
  'clear_crontab',
  'init_crontab',
  'sync_tables',
  'import_config'
]

target_help = """Name of the target"""
tap_help = """Name of the tap"""
tables_help = """List of tables to sync"""
dir_help = """Path to directory with config"""
secret_help = """Path to vault password file"""
version_help = """Displays the installed versions"""
log_help = """File to log into"""
debug_help = """Forces the debug mode with logging on stdout and log level debug."""

def main():
    '''Main entry point'''
    parser = argparse.ArgumentParser(description='PipelineWise {} - Command Line Interface'.format(__version__), add_help=True)
    parser.add_argument('command', type=str, choices=commands)
    parser.add_argument('--target', type=str, default='*', help=target_help)
    parser.add_argument('--tap', type=str, default='*', help=tap_help)
    parser.add_argument('--tables', type=str, help=tables_help)
    parser.add_argument('--dir', type=str, default='*', help=dir_help)
    parser.add_argument('--secret', type=str, default='*', help=secret_help)
    parser.add_argument('--version', action="version", help=version_help, version='PipelineWise {} - Command Line Interface'.format(__version__))
    parser.add_argument('--log', type=str, default='*', help=log_help)
    parser.add_argument('--debug', default=False, required=False, help=debug_help, action="store_true")

    args = parser.parse_args()

    # Command specific argument validations
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

    if args.command == 'import_config':
        if args.dir == '*':
            print("You must specify a directory path with config YAML files using the argumant --dir")
            sys.exit(1)
        if args.secret == '*':
            print("You must specify a path to the vault secret file using the argument --secret")
            sys.exit(1)

    pipelinewise = PipelineWise(args, config_dir, venv_dir)
    getattr(pipelinewise, args.command)()

if __name__ == '__main__':
    main()
