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
  'show_status',
  'test_tap_connection'
]

command_help = """Available commands, """ + ','.join(commands)
target_help = """Name of the target"""
tap_help = """Name of the tap"""
version_help = """Displays the installed versions"""
debug_help = """Forces the debug mode with logging on stdout and log level debug."""

def main():
    '''Main entry point'''
    parser = argparse.ArgumentParser(description='TransferData {} - Command Line Interface'.format(__version__), add_help=True)
    parser.add_argument('command', type=str, help=command_help)
    parser.add_argument('--target', type=str, default='*', help=target_help)
    parser.add_argument('--tap', type=str, default='*', help=tap_help)
    parser.add_argument('--version', action="version", help=version_help, version='TransferData {} - Command Line Interface'.format(__version__))
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

    pipelinewise = PipelineWise(args, config_dir, venv_dir)
    getattr(pipelinewise, args.command)()

if __name__ == '__main__':
    main()
